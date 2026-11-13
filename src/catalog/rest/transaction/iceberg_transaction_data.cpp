#include "catalog/rest/transaction/iceberg_transaction_data.hpp"

#include "catalog/rest/transaction/iceberg_transaction.hpp"

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"

#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "catalog/rest/iceberg_table_set.hpp"
#include "catalog/rest/api/table_update.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/metadata_io/manifest_list/iceberg_manifest_list_reader.hpp"
#include "execution/operator/merge_into/iceberg_merge_delete_collector.hpp"

namespace duckdb {

static bool HasTableUpdate(const vector<unique_ptr<IcebergTableUpdate>> &updates, IcebergTableUpdateType type) {
	for (const auto &update : updates) {
		if (update->type == type) {
			return true;
		}
	}
	return false;
}

static void VerifyNoSnapshotExpiration(const vector<unique_ptr<IcebergTableUpdate>> &updates, const char *operation) {
	if (HasTableUpdate(updates, IcebergTableUpdateType::REMOVE_SNAPSHOTS)) {
		throw TransactionException("%s cannot be combined with snapshot expiration in the same transaction", operation);
	}
}

static void LoadMissingManifestCounts(ClientContext &context, const IcebergTableMetadata &metadata,
                                      const IcebergSnapshotScanInfo &snapshot_info,
                                      IcebergManifestListEntry &manifest_list_entry) {
	auto &counts = manifest_list_entry.GetFile().counts;
	if (counts && counts->Complete()) {
		return;
	}
	vector<IcebergManifestListEntry> manifest_files;
	manifest_files.push_back(manifest_list_entry);
	manifest_files[0].ResetManifestEntries();

	IcebergOptions options;
	auto &fs = FileSystem::GetFileSystem(context);
	auto scan = AvroScan::ScanManifest(snapshot_info, manifest_files, options, fs, "", metadata, context);
	auto reader = make_uniq<manifest_file::ManifestReader>(*scan);
	while (!reader->Finished()) {
		reader->Read();
	}
	scan->Complete();

	manifest_list_entry = std::move(manifest_files[0]);
	manifest_list_entry.GetManifest().SetCountsFromEntries(manifest_list_entry.GetManifestEntries());
}

static optional<int64_t> LoadExistingManifestList(ClientContext &context, const IcebergTableMetadata &metadata,
                                                  const string &branch,
                                                  vector<IcebergManifestListEntry> &existing_manifest_list) {
	existing_manifest_list.clear();

	//! The write builds on the target branch's head (main's current snapshot for main or a branch to be created)
	auto current_snapshot = metadata.GetBranchHead(branch).base_snapshot;
	if (!current_snapshot) {
		return {};
	}
	optional<int64_t> base_snapshot_id = current_snapshot->snapshot_id;

	IcebergSnapshotScanInfo snapshot_info;
	snapshot_info.snapshot = current_snapshot;
	snapshot_info.schema_id = metadata.GetCurrentSchemaId();

	IcebergManifestList::LoadManifestFiles(snapshot_info, metadata, context, existing_manifest_list);
	for (auto &manifest_list_entry : existing_manifest_list) {
		LoadMissingManifestCounts(context, metadata, snapshot_info, manifest_list_entry);
	}

	if (metadata.iceberg_version < 3) {
		return base_snapshot_id;
	}

	//! Validate committed identities without assigning provisional IDs to upgraded manifests.
	for (auto &manifest_list_entry : existing_manifest_list) {
		auto &manifest_file = manifest_list_entry.GetManifest();
		if (manifest_file.content != IcebergManifestContentType::DATA) {
			continue;
		}
		if (!manifest_file.first_row_id && current_snapshot->first_row_id) {
			throw InvalidConfigurationException(
			    "Table is corrupted, snapshot has 'first-row-id' but not all 'manifest_file' "
			    "entries have a 'first_row_id'");
		}
	}
	return base_snapshot_id;
}

IcebergTransactionData::IcebergTransactionData(ClientContext &context, IcebergTransaction &transaction,
                                               const IcebergTable &table_info)
    : context(context), transaction(transaction), table_info(table_info) {
	initial_table_uuid = table_info.GetMetadata().table_uuid;
	initial_format_version = table_info.GetMetadata().iceberg_version;
	initial_schema_id = table_info.GetMetadata().GetCurrentSchemaId();
	initial_snapshot_id = table_info.GetMetadata().current_snapshot_id;
	initial_default_spec_id = table_info.GetMetadata().default_spec_id;
	if (table_info.GetMetadata().HasSortOrder()) {
		initial_default_sort_order_id = table_info.GetMetadata().default_sort_order_id;
	}
}

int64_t IcebergTransactionData::GetCommitRetryCount() const {
	static constexpr const int64_t DEFAULT_RETRY_COUNT = 4;
	auto it = table_info.GetMetadata().table_properties.find("commit.retry.num-retries");
	if (it == table_info.GetMetadata().table_properties.end()) {
		return DEFAULT_RETRY_COUNT;
	}
	int64_t result;
	try {
		size_t processed = 0;
		result = std::stoll(it->second, &processed);
		if (processed != it->second.size()) {
			throw InvalidInputException(
			    "Invalid value '%s' for table property 'commit.retry.num-retries': expected an integer", it->second);
		}
	} catch (std::exception &) {
		throw InvalidInputException(
		    "Invalid value '%s' for table property 'commit.retry.num-retries': expected an integer", it->second);
	}
	if (result < 0) {
		throw InvalidInputException(
		    "Invalid value '%s' for table property 'commit.retry.num-retries': expected a non-negative integer",
		    it->second);
	}
	return result;
}

bool IcebergTransactionData::ContainsDelete() const {
	for (auto &update : updates) {
		if (update->type != IcebergTableUpdateType::ADD_SNAPSHOT) {
			continue;
		}
		if (update->Cast<IcebergAddSnapshot>().GetOperation() == IcebergSnapshotOperationType::DELETE) {
			return true;
		}
	}
	return false;
}

void IcebergTransactionData::VerifySnapshotExpirationAllowed() const {
	//! These updates are not all materialized in the metadata used by the expiration planner.
	if (HasTableUpdate(updates, IcebergTableUpdateType::ADD_SNAPSHOT)) {
		throw TransactionException("Snapshot expiration cannot follow data changes in the same transaction");
	}
	//! SET_SNAPSHOT_REF is staged by rollback and by every ref create/move (create_branch, create_tag,
	//! fast_forward, set_current_snapshot).
	if (HasTableUpdate(updates, IcebergTableUpdateType::SET_SNAPSHOT_REF)) {
		throw TransactionException("Snapshot expiration cannot be combined with snapshot rollback or other ref "
		                           "changes in the same transaction");
	}
	//! Ref drops and table replacement remove refs the expiration planner would still see.
	if (HasTableUpdate(updates, IcebergTableUpdateType::REMOVE_SNAPSHOT_REF)) {
		throw TransactionException("Snapshot expiration cannot follow ref changes in the same transaction");
	}
	if (HasTableUpdate(updates, IcebergTableUpdateType::SET_PROPERTIES) ||
	    HasTableUpdate(updates, IcebergTableUpdateType::REMOVE_PROPERTIES)) {
		throw TransactionException("Snapshot expiration cannot follow table property changes in the same transaction");
	}
}

bool IcebergTransactionData::IsFileInvalidated(const IcebergFileIdentity &file) const {
	lock_guard<mutex> guard(lock);
	return manifest_deletes.IsInvalidated(file);
}

bool IcebergTransactionData::HasInvalidatedFiles() const {
	lock_guard<mutex> guard(lock);
	return !manifest_deletes.IsEmpty();
}

shared_ptr<IcebergMergeDeleteCollector> IcebergTransactionData::GetMergeDeleteCollector(idx_t action_count) {
	lock_guard<mutex> guard(lock);
	auto query_id = transaction.active_query.load();
	if (!merge_delete_query_id || *merge_delete_query_id != query_id) {
		VerifyMergeDeletesComplete();
		merge_delete_collector = make_shared_ptr<IcebergMergeDeleteCollector>(action_count);
		merge_delete_query_id = query_id;
	} else if (merge_delete_collector->ActionCount() != action_count) {
		throw InternalException("MERGE delete actions disagree on their participant count");
	}
	return merge_delete_collector;
}

void IcebergTransactionData::VerifyMergeDeletesComplete() const {
	if (merge_delete_collector && !merge_delete_collector->IsComplete()) {
		throw InternalException("MERGE delete actions have not all completed");
	}
}

bool IcebergTransactionData::SupportsAppendRetry() const {
	if (!requirements.empty()) {
		return false;
	}
	//! A MERGE that only inserts commits a plain append, but its WHEN NOT MATCHED rows were chosen against the
	//! snapshot it read. Re-applying them on top of a concurrent commit could insert rows that now match.
	if (contains_merge) {
		return false;
	}
	if (updates.empty()) {
		return false;
	}
	for (auto &update : updates) {
		if (!update->IsRetryable()) {
			return false;
		}
	}
	return true;
}

bool IcebergTransactionData::RetryStateMatches(const IcebergTable &table) const {
	if (table.GetMetadata().table_uuid != initial_table_uuid) {
		return false;
	}
	if (table.GetMetadata().GetCurrentSchemaId() != initial_schema_id) {
		return false;
	}
	if (table.GetMetadata().default_spec_id != initial_default_spec_id) {
		return false;
	}
	if (table.GetMetadata().HasSortOrder() != initial_default_sort_order_id.IsValid()) {
		return false;
	}
	if (table.GetMetadata().HasSortOrder() &&
	    table.GetMetadata().default_sort_order_id.GetIndex() != initial_default_sort_order_id.GetIndex()) {
		return false;
	}
	return true;
}

IcebergSnapshotScanInfo GetWapReadSnapshot(ClientContext &context, const IcebergTableMetadata &metadata,
                                           optional_ptr<const IcebergTransactionData> transaction_data) {
	auto wap_branch = GetIcebergWapBranch(context);
	bool pending_creation = false;
	if (transaction_data) {
		transaction_data->VerifyTargetBranch(wap_branch);
		if (wap_branch != ICEBERG_MAIN_BRANCH && transaction_data->has_assert_create) {
			ThrowWapReadOfCreatedTable(transaction_data->table_info.name, wap_branch);
		}
		pending_creation = transaction_data->CreatesBranch(wap_branch);
	}
	return metadata.GetBranchReadSnapshot(wap_branch, pending_creation);
}

IcebergSnapshotScanInfo GetLookupSnapshot(const IcebergTableMetadata &metadata, const IcebergSnapshotLookup &lookup,
                                          optional_ptr<const IcebergTransactionData> transaction_data) {
	if (transaction_data && lookup.IsFromRef()) {
		auto &ref_name = lookup.GetSnapshotRef().name;
		if (!metadata.HasRef(ref_name) && transaction_data->CreatesBranch(ref_name)) {
			return metadata.GetBranchReadSnapshot(ref_name, true);
		}
	}
	return metadata.GetSnapshot(lookup);
}

void ThrowWapReadOfCreatedTable(const string &table_name, const string &wap_branch) {
	//! A new table has no branches until its first commit, which must go to main
	throw InvalidInputException("Table \"%s\" is created in this transaction, so it has no branch '%s' to read; read "
	                            "it with AT (VERSION => 'main') or RESET %s",
	                            table_name, wap_branch, WAP_BRANCH_CONFIG_VARIABLE);
}

const string &IcebergTransactionData::GetTargetBranch() const {
	return target_branch;
}

void IcebergTransactionData::VerifyTargetBranch(const string &session_branch) const {
	if (!target_branch_bound || session_branch == target_branch) {
		return;
	}
	throw InvalidInputException("Table \"%s\" was changed on branch '%s' in this transaction, so it can not be used "
	                            "with '%s' = '%s' before the transaction ends",
	                            table_info.name, target_branch, WAP_BRANCH_CONFIG_VARIABLE, session_branch);
}

void IcebergTransactionData::BindTargetBranch(lock_guard<mutex> &guard) {
	(void)guard;
	auto session_branch = GetIcebergWapBranch(context);
	if (replaces_table && session_branch != ICEBERG_MAIN_BRANCH) {
		//! A replacement detaches main (TableReplaceContents); its contents can only be written to main. Refused
		//! by the statement, before it loads a manifest list, rather than only at commit (CreateTableRequirements).
		throw InvalidInputException("Cannot replace table \"%s\" while writing to Iceberg branch '%s'", table_info.name,
		                            session_branch);
	}
	if (target_branch_bound) {
		VerifyTargetBranch(session_branch);
		return;
	}
	if (session_branch != ICEBERG_MAIN_BRANCH) {
		if (has_assert_create) {
			throw NotImplementedException("Cannot write table \"%s\" to branch '%s': it is created in this "
			                              "transaction, and a new table's first commit must go to main",
			                              table_info.name, session_branch);
		}
		//! Throws now, rather than at commit, if the ref is a tag
		auto head = table_info.GetMetadata().GetBranchHead(session_branch);
		target_branch_existed = head.ref_snapshot != nullptr;
		may_create_branch = GetIcebergWapBranchCreateMissing(context);
		if (!target_branch_existed && !may_create_branch) {
			throw InvalidInputException("Iceberg branch '%s' does not exist in table \"%s\", and '%s' is false",
			                            session_branch, table_info.name, WAP_BRANCH_CREATE_MISSING_CONFIG_VARIABLE);
		}
		if (!target_branch_existed && !head.base_snapshot) {
			ThrowParentlessCommit(session_branch, table_info.GetMetadata().HasBranchOtherThanMain());
		}
	} else if (!has_assert_create) {
		auto &metadata = table_info.GetMetadata();
		if (!metadata.GetBranchHead(session_branch).base_snapshot && metadata.HasBranchOtherThanMain()) {
			ThrowParentlessCommit(session_branch, true);
		}
	}
	target_branch = std::move(session_branch);
	target_branch_bound = true;
}

void IcebergTransactionData::VerifyReadBranch(lock_guard<mutex> &guard,
                                              optional_ptr<const IcebergSnapshotScanInfo> read_snapshot) const {
	(void)guard;
	if (!read_snapshot || (read_snapshot->is_transaction_head && read_snapshot->head_branch == target_branch)) {
		return;
	}
	throw InvalidInputException("Cannot change table \"%s\" on branch '%s': the statement read %s; re-run it",
	                            table_info.name, target_branch,
	                            read_snapshot->is_transaction_head ? "branch '" + read_snapshot->head_branch + "'"
	                                                               : string("a snapshot that is not a branch head"));
}

bool IcebergTransactionData::IsVisibleIn(const IcebergSnapshotScanInfo &snapshot) const {
	//! Before the first data-changing statement the target is "main", and nothing is staged for a branch
	return snapshot.is_transaction_head && snapshot.head_branch == target_branch;
}

bool IcebergTransactionData::CreatesBranch(const string &branch) const {
	return target_branch_bound && !target_branch_existed && target_branch == branch && !alters.empty();
}

void IcebergTransactionData::VerifyCommitBranch(const IcebergTableMetadata &metadata,
                                                const IcebergBranchHead &branch_head) const {
	if (alters.empty()) {
		return;
	}
	if (branch_head.IsMain()) {
		//! Checked against the refreshed metadata of every attempt: another engine may have committed to a branch
		//! while main still has no snapshot
		if (!branch_head.base_snapshot && metadata.HasBranchOtherThanMain()) {
			ThrowParentlessCommit(branch_head.branch, true);
		}
		return;
	}
	if (branch_head.ref_snapshot) {
		//! The branch exists now. If this transaction meant to create it (!target_branch_existed) and another writer
		//! created it first, the retry appends onto that writer's branch head. Java clients (Spark) do the same,
		//! client-side: every attempt refreshes and takes SnapshotUtil.latestSnapshot(base, targetBranch) as the
		//! parent (apache-iceberg-1.10.1, core/src/main/java/org/apache/iceberg/SnapshotProducer.java:253-256). A
		//! DELETE is only re-applied if that head descends from the snapshot it read (VerifyDeleteRetryability)
		return;
	}
	//! The refusals below are raised while staging a commit attempt, before anything of it is sent
	if (target_branch_existed) {
		//! Recreating it from main would rebase this transaction's changes onto main's lineage
		ThrowCommitRefusal(ExceptionType::TRANSACTION,
		                   StringUtil::Format("Iceberg branch '%s' of table \"%s\" was removed concurrently; the "
		                                      "changes made on it in this transaction are not committed",
		                                      branch_head.branch, table_info.name));
	}
	if (!may_create_branch) {
		ThrowCommitRefusal(ExceptionType::INVALID_INPUT,
		                   StringUtil::Format("Iceberg branch '%s' does not exist in table \"%s\", and '%s' is false",
		                                      branch_head.branch, table_info.name,
		                                      WAP_BRANCH_CREATE_MISSING_CONFIG_VARIABLE));
	}
	if (!branch_head.base_snapshot) {
		//! Checked against the refreshed metadata of every attempt: main may have lost its snapshot since bind
		ThrowParentlessCommit(branch_head.branch, metadata.HasBranchOtherThanMain());
	}
}

void IcebergTransactionData::ThrowParentlessCommit(const string &branch, bool table_has_other_branches) const {
	//! The commit's first snapshot would have no parent. Iceberg catalogs (Java's TableMetadata.Builder.addSnapshot)
	//! skip the sequence-number check for such a snapshot and adopt its sequence number as last-sequence-number
	//! (server-side, apache-iceberg-1.10.1, core/src/main/java/org/apache/iceberg/TableMetadata.java:1250-1259), so
	//! one staged on stale metadata would not be above the current last-sequence-number (it could equal or lower
	//! it), and later deletes would not apply to newer data files. That needs
	//! another ref that can receive concurrent commits; without one (an empty table, or one whose old snapshots are
	//! unreferenced after a replace) main's first commit is safe. A branch is never created without a parent.
	//! Raised while staging a statement or a commit attempt, before anything of the attempt is sent
	if (replaces_table && table_has_other_branches) {
		//! A replacement detaches main (TableReplaceContents), so its first snapshot has no parent
		ThrowCommitRefusal(
		    ExceptionType::INVALID_INPUT,
		    StringUtil::Format("Cannot write the replacement contents of table \"%s\": the table has branches other "
		                       "than main, so the replacement's first snapshot would have no parent, and catalogs do "
		                       "not check the sequence number of a snapshot without a parent. Drop the other branches "
		                       "in an earlier transaction, or replace the table without writing data",
		                       table_info.name));
	}
	if (table_has_other_branches) {
		ThrowCommitRefusal(
		    ExceptionType::INVALID_INPUT,
		    StringUtil::Format(
		        "Cannot commit to Iceberg branch '%s' of table \"%s\": main has no snapshot but the table has other "
		        "branches, so the new snapshot would have no parent, and catalogs do not check the sequence number of "
		        "a snapshot without a parent. Drop the other branches, or set main with another engine first",
		        branch, table_info.name));
	}
	ThrowCommitRefusal(ExceptionType::INVALID_INPUT,
	                   StringUtil::Format("Cannot create Iceberg branch '%s' of table \"%s\": main has no snapshot "
	                                      "yet, so the branch would start without a parent. Insert into main first "
	                                      "(RESET %s), then write to the branch",
	                                      branch, table_info.name, WAP_BRANCH_CONFIG_VARIABLE));
}

void IcebergTransactionData::CacheExistingManifestList(lock_guard<mutex> &guard, const IcebergTableMetadata &metadata) {
	if (!alters.empty()) {
		return;
	}
	base_snapshot_id = LoadExistingManifestList(context, metadata, target_branch, existing_manifest_list);
	scan_sequence_number = IcebergSnapshot::NextSequenceNumber(metadata.last_sequence_number);
}

void IcebergTransactionData::AddSnapshot(IcebergSnapshotOperationType operation,
                                         vector<IcebergManifestEntry> &&data_files,
                                         IcebergManifestDeletes &&altered_manifests, bool merge_action) {
	//! Serialize snapshot operations so transaction scans retain their insertion order.
	lock_guard<mutex> guard(lock);
	//! Before binding the branch or caching the manifest list, so a refused statement changes no state
	VerifyCanAddSnapshot();

	auto &table_metadata = table_info.GetMetadata();
	BindTargetBranch(guard);
	CacheExistingManifestList(guard, table_metadata);

	IcebergManifestContentType manifest_content_type;
	switch (operation) {
	case IcebergSnapshotOperationType::APPEND:
	case IcebergSnapshotOperationType::REPLACE:
	case IcebergSnapshotOperationType::OVERWRITE:
		//! This helper currently writes DATA manifest entries; REPLACE itself is not limited to data files.
		manifest_content_type = IcebergManifestContentType::DATA;
		break;
	default:
		throw NotImplementedException("Snapshot operation type %d does not write data manifests",
		                              static_cast<uint8_t>(operation));
	};

	auto manifest_metadata = IcebergManifestMetadata::FromTableMetadata(table_metadata, manifest_content_type);
	auto add_snapshot = make_uniq<IcebergAddSnapshot>(table_info, operation);
	//! A REPLACE may only remove fully deleted input files, without adding any data files.
	if (!data_files.empty()) {
		add_snapshot->AddPendingManifest(IcebergPendingManifest(manifest_metadata, std::move(data_files)));
	}
	// make sure we are still inserting into the current schema
	if (table_metadata.current_snapshot_id) {
		TableAddAssertCurrentSchemaId();
	}
	AddSnapshotUpdate(std::move(add_snapshot), std::move(altered_manifests), merge_action);
}

void IcebergTransactionData::AddPendingDeleteManifests(IcebergAddSnapshot &add_snapshot,
                                                       partitioned_manifest_entry_map_t &&delete_files) {
	auto &table_metadata = table_info.GetMetadata();
	//! One manifest per partition spec: a manifest declares a single spec, and the entries it holds carry
	//! partition values in that spec.
	for (auto &entry : delete_files) {
		auto manifest_metadata =
		    IcebergManifestMetadata::FromTableMetadata(table_metadata, IcebergManifestContentType::DELETE, entry.first);
		add_snapshot.AddPendingManifest(IcebergPendingManifest(manifest_metadata, std::move(entry.second)));
	}
}

void IcebergTransactionData::VerifyCanAddSnapshot() const {
	VerifyNoSnapshotExpiration(updates, "Data changes");
	if (rolls_back) {
		throw InvalidInputException("Cannot change table \"%s\" after rolling it back in the same transaction",
		                            table_info.name);
	}
	if (has_maintenance_rewrite) {
		throw InvalidInputException("Cannot change table \"%s\" after rewriting its data files in the same transaction",
		                            table_info.name);
	}
	if (!modified_refs.empty()) {
		throw InvalidInputException("Cannot change table \"%s\" after changing its refs in the same transaction",
		                            table_info.name);
	}
}

void IcebergTransactionData::AddSnapshotUpdate(unique_ptr<IcebergAddSnapshot> add_snapshot,
                                               IcebergManifestDeletes &&altered_manifests, bool merge_action) {
	VerifyCanAddSnapshot();
	//! A rewrite (iceberg_rewrite_data_files) is planned and validated against main's current snapshot
	if (add_snapshot->GetOperation() == IcebergSnapshotOperationType::REPLACE && target_branch != ICEBERG_MAIN_BRANCH) {
		throw NotImplementedException("Cannot rewrite data files of table \"%s\" while '%s' is set to '%s'; reset it "
		                              "to rewrite main",
		                              table_info.name, WAP_BRANCH_CONFIG_VARIABLE, target_branch);
	}
	contains_merge |= merge_action;
	// All action sinks share this execution's query ID; a later statement gets a different ID.
	auto query_id = transaction.active_query.load();
	if (merge_action && merge_query_id && *merge_query_id == query_id) {
		// Keeping one alter gives every action the same transaction-local sequence number.
		auto &snapshot = alters[merge_alter_index].get();
		snapshot.Merge(*add_snapshot);
		// Invalidation versions must identify the shared snapshot that removes these files.
		auto versioned_deletes = manifest_deletes.AtVersion(merge_alter_index);
		if (versioned_deletes.Merge(std::move(altered_manifests))) {
			snapshot.SetManifestDeletes(std::move(versioned_deletes));
		}
		return;
	}

	auto versioned_deletes = manifest_deletes.AtVersion(alters.size());
	if (versioned_deletes.Merge(std::move(altered_manifests))) {
		add_snapshot->SetManifestDeletes(std::move(versioned_deletes));
	}
	if (merge_action) {
		// The first action that writes files establishes the snapshot, regardless of finalization order.
		merge_query_id = query_id;
		merge_alter_index = alters.size();
	}
	alters.push_back(*add_snapshot);
	updates.push_back(std::move(add_snapshot));
}

void IcebergTransactionData::AddDeleteSnapshot(partitioned_manifest_entry_map_t &&delete_files,
                                               IcebergManifestDeletes &&altered_manifests, bool merge_action,
                                               optional_ptr<const IcebergSnapshotScanInfo> read_snapshot) {
	//! Serialize snapshot operations so transaction scans retain their insertion order.
	lock_guard<mutex> guard(lock);
	//! Before binding the branch or caching the manifest list, so a refused statement changes no state
	VerifyCanAddSnapshot();

	auto &table_metadata = table_info.GetMetadata();
	BindTargetBranch(guard);
	VerifyReadBranch(guard, read_snapshot);
	CacheExistingManifestList(guard, table_metadata);

	auto add_snapshot = make_uniq<IcebergAddSnapshot>(table_info, IcebergSnapshotOperationType::DELETE);
	AddPendingDeleteManifests(*add_snapshot, std::move(delete_files));
	// make sure we are still inserting into the current schema
	if (table_metadata.current_snapshot_id) {
		TableAddAssertCurrentSchemaId();
	}
	AddSnapshotUpdate(std::move(add_snapshot), std::move(altered_manifests), merge_action);
}

void IcebergTransactionData::AddUpdateSnapshot(partitioned_manifest_entry_map_t &&delete_files,
                                               vector<IcebergManifestEntry> &&data_files,
                                               IcebergManifestDeletes &&altered_manifests, bool merge_action,
                                               optional_ptr<const IcebergSnapshotScanInfo> read_snapshot) {
	//! Serialize snapshot operations so transaction scans retain their insertion order.
	lock_guard<mutex> guard(lock);
	//! Before binding the branch or caching the manifest list, so a refused statement changes no state
	VerifyCanAddSnapshot();

	auto &table_metadata = table_info.GetMetadata();
	BindTargetBranch(guard);
	VerifyReadBranch(guard, read_snapshot);

	CacheExistingManifestList(guard, table_metadata);

	auto data_manifest_metadata =
	    IcebergManifestMetadata::FromTableMetadata(table_metadata, IcebergManifestContentType::DATA);

	auto add_snapshot = make_uniq<IcebergAddSnapshot>(table_info);
	AddPendingDeleteManifests(*add_snapshot, std::move(delete_files));
	add_snapshot->AddPendingManifest(IcebergPendingManifest(data_manifest_metadata, std::move(data_files)));
	AddSnapshotUpdate(std::move(add_snapshot), std::move(altered_manifests), merge_action);
}

void IcebergTransactionData::TableAddSchema(int32_t schema_id) {
	auto &schema = table_info.GetMetadata().GetSchemaFromId(schema_id);
	auto add_schema_update = make_uniq<AddSchemaUpdate>(schema.Copy(), table_info.GetMetadata().last_column_id);
	updates.push_back(std::move(add_schema_update));
	assert_schema_id = true;
	TableSetCurrentSchema(schema_id);
}

void IcebergTransactionData::TableSetCurrentSchema(int32_t schema_id) {
	// Later specs and sort orders are validated against the schema current at this point in the transaction.
	updates.push_back(make_uniq<SetCurrentSchema>(schema_id));
}

void IcebergTransactionData::TableAssignUUID() {
	updates.push_back(make_uniq<AssignUUIDUpdate>(table_info.GetMetadata().table_uuid));
}

void IcebergTransactionData::TableAddAssertCreate() {
	has_assert_create = true;
	requirements.push_back(make_uniq<AssertCreateRequirement>());
	auto alter_update = transaction.GetAlterUpdate();
	D_ASSERT(alter_update);
	transaction.VerifyAlterUpdateAtomicity(*alter_update);
}

void IcebergTransactionData::TableAddAssertUUID() {
	requirements.push_back(make_uniq<AssertTableUUIDRequirement>(table_info.GetMetadata().table_uuid));
}

void IcebergTransactionData::TableAddAssertCurrentSchemaId() {
	assert_schema_id = true;
}

void IcebergTransactionData::TableAddAssertLastAssignedFieldId() {
	D_ASSERT(table_info.GetMetadata().HasLastColumnId());
	requirements.push_back(make_uniq<AssertLastAssignedFieldIdRequirement>(
	    static_cast<int32_t>(table_info.GetMetadata().GetLastColumnId())));
}

void IcebergTransactionData::TableAddAssertLastAssignedPartitionId() {
	int32_t last_assigned_partition_id = 999;
	if (table_info.GetMetadata().HasLastPartitionId()) {
		last_assigned_partition_id = table_info.GetMetadata().GetLastPartitionFieldId();
	}
	requirements.push_back(make_uniq<AssertLastAssignedPartitionIdRequirement>(last_assigned_partition_id));
}

void IcebergTransactionData::TableAddAssertDefaultSpecId() {
	requirements.push_back(make_uniq<AssertDefaultSpecIdRequirement>(table_info.GetMetadata().default_spec_id));
}

void IcebergTransactionData::TableAddUpradeFormatVersion() {
	updates.push_back(make_uniq<UpgradeFormatVersion>(table_info.GetMetadata().iceberg_version));
}

void IcebergTransactionData::TableAddPartitionSpec() {
	updates.push_back(make_uniq<AddPartitionSpec>(table_info.GetMetadata().GetLatestPartitionSpec()));
}

void IcebergTransactionData::TableAddSortOrder() {
	updates.push_back(make_uniq<AddSortOrder>(table_info.GetMetadata().GetLatestSortOrder()));
}

void IcebergTransactionData::TableSetDefaultSortOrder() {
	D_ASSERT(table_info.GetMetadata().HasSortOrder());
	updates.push_back(make_uniq<SetDefaultSortOrder>(table_info.GetMetadata().GetLatestSortOrder().sort_order_id));
}

void IcebergTransactionData::TableSetDefaultSpec() {
	updates.push_back(make_uniq<SetDefaultSpec>(table_info.GetMetadata().default_spec_id));
}

void IcebergTransactionData::TableReplaceContents(IcebergTableMetadata &metadata) {
	lock_guard<mutex> guard(lock);
	//! Expiration is planned from the refs this replacement removes (see VerifySnapshotExpirationAllowed).
	VerifyNoSnapshotExpiration(updates, "Table replacement");
	//! Earlier snapshot updates would be replayed at commit, restoring the contents being replaced. Earlier ref
	//! changes were validated against the refs being replaced, and the replacement's new snapshot could not be
	//! added after them (VerifyCanAddSnapshot).
	if (!alters.empty() || rolls_back || !modified_refs.empty()) {
		throw InvalidInputException("Cannot replace table \"%s\": it was already changed in this transaction",
		                            table_info.name);
	}
	//! Replacement detaches main; a session writing to another branch must not replace main underneath it.
	auto session_branch = GetIcebergWapBranch(context);
	if (session_branch != ICEBERG_MAIN_BRANCH) {
		throw NotImplementedException("Cannot replace table \"%s\" while '%s' is set to '%s'", table_info.name,
		                              WAP_BRANCH_CONFIG_VARIABLE, session_branch);
	}
	replaces_table = true;
	assert_schema_id = true;
	metadata.current_snapshot_id.reset();
	if (metadata.refs) {
		metadata.refs->additional_properties.erase("main");
	}
	existing_manifest_list.clear();
	base_snapshot_id.reset();
	updates.push_back(RemoveSnapshotRef::DetachMain());
}

void IcebergTransactionData::TableSetProperties(const case_insensitive_map_t<string> &properties) {
	VerifyNoSnapshotExpiration(updates, "Table property changes");
	if (properties.count("encryption.key-id")) {
		throw NotImplementedException("Iceberg table encryption is not supported (encryption.key-id is set)");
	}
	updates.push_back(make_uniq<SetProperties>(properties));
}

void IcebergTransactionData::TableRemoveProperties(const vector<string> &properties) {
	VerifyNoSnapshotExpiration(updates, "Table property changes");
	updates.push_back(make_uniq<RemoveProperties>(properties));
}

void IcebergTransactionData::TableRemoveSnapshots(const vector<int64_t> &snapshot_ids) {
	for (auto &update : updates) {
		if (update->type == IcebergTableUpdateType::REMOVE_SNAPSHOTS) {
			update->Cast<RemoveSnapshots>().AddSnapshotIds(snapshot_ids);
			return;
		}
	}
	updates.push_back(make_uniq<RemoveSnapshots>(snapshot_ids));
	//! The standard requirement detects changes to main, but cannot detect a
	//! concurrently created ref or retention-only policy changes.
	const auto &metadata = table_info.GetMetadata();
	requirements.push_back(make_uniq<AssertRefSnapshotId>(metadata.current_snapshot_id));
}

void IcebergTransactionData::TableSetLocation() {
	updates.push_back(make_uniq<SetLocation>(table_info.GetMetadata().location));
}

static bool IsAncestorOfCurrentSnapshot(const IcebergTableMetadata &metadata, int64_t target_snapshot_id,
                                        int64_t current_snapshot_id) {
	if (target_snapshot_id == current_snapshot_id) {
		return true;
	}
	//! Parent ids come from catalog metadata, which may be malformed: a cycle must not loop forever.
	//! A valid lineage visits each snapshot at most once, so more steps than snapshots means a cycle.
	const idx_t max_steps = metadata.snapshots.size();
	idx_t steps = 0;
	optional_ptr<const IcebergSnapshot> cursor = metadata.FindSnapshotByIdInternal(current_snapshot_id);
	while (cursor) {
		if (!cursor->parent_snapshot_id) {
			return false;
		}
		auto parent_id = *cursor->parent_snapshot_id;
		if (parent_id == target_snapshot_id) {
			return true;
		}
		if (++steps > max_steps) {
			throw InvalidConfigurationException(
			    "Iceberg snapshot lineage of snapshot %lld contains a cycle in the table metadata",
			    current_snapshot_id);
		}
		cursor = metadata.FindSnapshotByIdInternal(parent_id);
	}
	return false;
}

//! ----------- Ref management -----------

namespace {

struct ResolvedRef {
	IcebergRefType type;
	int64_t snapshot_id;
};

const char *RefTypeName(IcebergRefType type) {
	return type == IcebergRefType::TAG ? "tag" : "branch";
}

//! Resolve a ref by its exact (case-sensitive) name, as the catalog will when checking requirements.
optional<ResolvedRef> FindRef(const IcebergTableMetadata &metadata, const string &ref_name) {
	if (metadata.refs) {
		auto &refs = metadata.refs->additional_properties;
		auto entry = refs.find(ref_name);
		if (entry != refs.end()) {
			//! The same case-insensitive type match as reads (IcebergTableMetadata::GetSnapshotByRef), so
			//! every ref that can be read can also be managed.
			auto type = StringUtil::Lower(entry->second.type);
			ResolvedRef result;
			if (type == "branch") {
				result.type = IcebergRefType::BRANCH;
			} else if (type == "tag") {
				result.type = IcebergRefType::TAG;
			} else {
				//! The type comes from metadata, so keep the message bounded
				static constexpr idx_t MAX_TYPE_LENGTH = 64;
				auto &raw_type = entry->second.type;
				auto shown_type =
				    raw_type.size() > MAX_TYPE_LENGTH ? raw_type.substr(0, MAX_TYPE_LENGTH) + "..." : raw_type;
				throw InvalidConfigurationException(
				    "Iceberg ref '%s' has unknown type '%s', expected 'branch' or 'tag'", ref_name, shown_type);
			}
			result.snapshot_id = entry->second.snapshot_id;
			return result;
		}
	}
	if (ref_name == "main" && metadata.current_snapshot_id) {
		//! Older metadata has no refs, so 'main' is the current snapshot
		return ResolvedRef {IcebergRefType::BRANCH, *metadata.current_snapshot_id};
	}
	return {};
}

//! numeric_hint: a ref argument is always a ref name, so a name that parses as a snapshot id but is
//! not a ref gets this hint appended instead of a bare "does not exist".
ResolvedRef GetExistingRef(const IcebergTableMetadata &metadata, const string &ref_name, const char *description,
                           const char *numeric_hint = nullptr) {
	auto ref = FindRef(metadata, ref_name);
	if (!ref) {
		int64_t parsed;
		if (numeric_hint && TryCast::Operation<string_t, int64_t>(string_t(ref_name), parsed, true)) {
			throw InvalidInputException("%s '%s' does not exist (a ref argument is always a ref name; %s)", description,
			                            ref_name, numeric_hint);
		}
		throw InvalidInputException("%s '%s' does not exist", description, ref_name);
	}
	return *ref;
}

} // namespace

void IcebergTransactionData::TableRollbackToSnapshot(int64_t snapshot_id) {
	VerifyNoSnapshotExpiration(updates, "Snapshot rollback");
	//! The rollback moves main; refuse while the session targets another branch rather than surprise a WAP user.
	//! (Spark's rollback_to_snapshot moves main regardless of spark.wap.branch, client-side: apache-iceberg-1.10.0,
	//! spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/RollbackToSnapshotProcedure.java:88)
	auto session_branch = GetIcebergWapBranch(context);
	if (session_branch != ICEBERG_MAIN_BRANCH) {
		throw NotImplementedException("Cannot roll back table \"%s\" while '%s' is set to '%s': rollback moves main. "
		                              "RESET %s to roll main back",
		                              table_info.name, WAP_BRANCH_CONFIG_VARIABLE, session_branch,
		                              WAP_BRANCH_CONFIG_VARIABLE);
	}
	//! The commit points main at the last snapshot the transaction adds, which would undo the rollback
	if (!alters.empty() || rolls_back || modified_refs.count("main")) {
		throw InvalidInputException("Cannot roll back table \"%s\": it was already changed in this transaction",
		                            table_info.name);
	}
	auto &metadata = table_info.GetMetadata();
	auto target = metadata.FindSnapshotByIdInternal(snapshot_id);
	if (!target) {
		throw InvalidInputException("Cannot roll back to unknown snapshot id: %lld", snapshot_id);
	}

	auto current = metadata.GetLatestSnapshot();
	if (!current) {
		throw InvalidInputException("Cannot roll back table with no current snapshot");
	}
	//! Moving a branch that is a tag is refused, as Java's client-side TableMetadata.Builder does
	//! ("Cannot update branch: %s is a tag", apache-iceberg-1.10.0,
	//! core/src/main/java/org/apache/iceberg/TableMetadata.java:1756).
	auto main = FindRef(metadata, "main");
	if (main && main->type != IcebergRefType::BRANCH) {
		throw InvalidConfigurationException("Iceberg ref 'main' of table \"%s\" is not a branch", table_info.name);
	}
	auto current_snapshot_id = current->snapshot_id;
	if (!IsAncestorOfCurrentSnapshot(metadata, snapshot_id, current_snapshot_id)) {
		throw InvalidInputException("Cannot roll back to snapshot, not an ancestor of the current state: %lld",
		                            snapshot_id);
	}

	// Optimistic concurrency: require main still points at the snapshot we observed.
	requirements.push_back(make_uniq<AssertRefSnapshotId>(current_snapshot_id));
	updates.push_back(make_uniq<SetSnapshotRef>(snapshot_id));
	rolls_back = true;
	modified_refs.insert("main");

	// Iceberg intentionally does not roll current-schema-id back with the snapshot;
	// schema evolution stays forward-only. See https://github.com/apache/iceberg/issues/5591
}

void IcebergTransactionData::VerifyCanChangeRef(const string &ref_name) const {
	if (replaces_table) {
		//! TableReplaceContents detached main from the cached metadata this validation reads.
		throw InvalidInputException("Cannot change refs of table \"%s\": it was replaced in this transaction",
		                            table_info.name);
	}
	VerifyNoSnapshotExpiration(updates, "Ref changes");
	if (!alters.empty()) {
		//! The commit points its target branch (main, or the iceberg_wap_branch) at the transaction's last new
		//! snapshot, which was based on the old refs.
		throw InvalidInputException("Cannot change refs of table \"%s\": its rows were already changed in this "
		                            "transaction",
		                            table_info.name);
	}
	if (modified_refs.count(ref_name)) {
		throw InvalidInputException("Cannot change ref '%s' of table \"%s\": it was already changed in this "
		                            "transaction",
		                            ref_name, table_info.name);
	}
}

int64_t IcebergTransactionData::TableCreateRef(const string &ref_name, IcebergRefType ref_type,
                                               optional<int64_t> snapshot_id,
                                               const IcebergSnapshotRefRetention &retention) {
	if (ref_name == "main") {
		throw InvalidInputException("Cannot create ref 'main': the main branch is managed by writes to the table");
	}
	ValidateIcebergRefName(ref_name, "Iceberg ref name");
	VerifyCanChangeRef(ref_name);
	auto &metadata = table_info.GetMetadata();
	if (metadata.refs && metadata.refs->additional_properties.count(ref_name)) {
		throw InvalidInputException("Iceberg ref '%s' already exists", ref_name);
	}

	int64_t target_snapshot_id;
	if (snapshot_id) {
		if (!metadata.FindSnapshotByIdInternal(*snapshot_id)) {
			throw InvalidInputException("Cannot create %s '%s': unknown snapshot id %lld", RefTypeName(ref_type),
			                            ref_name, *snapshot_id);
		}
		target_snapshot_id = *snapshot_id;
	} else {
		if (modified_refs.count("main")) {
			//! main would be read at its pre-transaction snapshot, not where this transaction moves it.
			throw InvalidInputException("Cannot create %s '%s' at the current snapshot of table \"%s\": main was "
			                            "already changed in this transaction; pass snapshot_id",
			                            RefTypeName(ref_type), ref_name, table_info.name);
		}
		auto main = FindRef(metadata, "main");
		if (!main) {
			throw InvalidInputException("Cannot create %s '%s': table \"%s\" has no current snapshot",
			                            RefTypeName(ref_type), ref_name, table_info.name);
		}
		target_snapshot_id = main->snapshot_id;
		//! The ref is created at the snapshot main had when it was read: a concurrent move of main
		//! aborts the commit instead of silently tagging a snapshot the caller never saw.
		requirements.push_back(make_uniq<AssertRefSnapshotId>("main", target_snapshot_id));
	}

	//! A null snapshot id requires that the ref does not exist when the catalog applies the commit.
	requirements.push_back(make_uniq<AssertRefSnapshotId>(ref_name, optional<int64_t>()));
	updates.push_back(SetSnapshotRef::CreateRef(target_snapshot_id, ref_name, RefTypeName(ref_type), retention));
	modified_refs.insert(ref_name);
	return target_snapshot_id;
}

int64_t IcebergTransactionData::TableDropRef(const string &ref_name, IcebergRefType ref_type) {
	if (ref_name == "main") {
		throw InvalidInputException("Cannot drop the main branch");
	}
	VerifyCanChangeRef(ref_name);
	auto ref = GetExistingRef(table_info.GetMetadata(), ref_name, "Iceberg ref");
	if (ref.type != ref_type) {
		throw InvalidInputException("Iceberg ref '%s' is a %s, not a %s", ref_name, RefTypeName(ref.type),
		                            RefTypeName(ref_type));
	}
	//! A concurrent move of the ref aborts the drop instead of removing a ref we did not inspect.
	requirements.push_back(make_uniq<AssertRefSnapshotId>(ref_name, ref.snapshot_id));
	updates.push_back(make_uniq<RemoveSnapshotRef>(ref_name));
	modified_refs.insert(ref_name);
	return ref.snapshot_id;
}

IcebergRefMove IcebergTransactionData::TableFastForward(const string &branch, const string &to) {
	if (branch.empty() || to.empty()) {
		throw InvalidInputException("Iceberg ref names cannot be empty");
	}
	VerifyCanChangeRef(branch);
	if (branch != to && modified_refs.count(to)) {
		throw InvalidInputException("Cannot fast-forward to ref '%s' of table \"%s\": it was already changed in this "
		                            "transaction",
		                            to, table_info.name);
	}
	auto &metadata = table_info.GetMetadata();
	//! The checks follow Java's client-side UpdateSnapshotReferencesOperation.replaceBranch, which Spark's
	//! fast_forward procedure calls (apache-iceberg-1.10.0,
	//! core/src/main/java/org/apache/iceberg/UpdateSnapshotReferencesOperation.java:117-147): 'to' must exist,
	//! a missing branch is created at its snapshot, the branch must not be a tag, equal snapshots are a no-op,
	//! and otherwise the branch's snapshot must be an ancestor of the target.
	auto target_ref = GetExistingRef(metadata, to, "Ref",
	                                 "to publish a snapshot id use iceberg_set_current_snapshot(snapshot_id := ...)");
	if (!metadata.FindSnapshotByIdInternal(target_ref.snapshot_id)) {
		throw InvalidConfigurationException("Iceberg ref '%s' points at snapshot %lld, which is not in the table "
		                                    "metadata",
		                                    to, target_ref.snapshot_id);
	}
	IcebergRefMove result;
	result.updated_snapshot_id = target_ref.snapshot_id;

	auto existing_branch = FindRef(metadata, branch);
	if (!existing_branch) {
		//! Java creates the missing branch (UpdateSnapshotReferencesOperation.java:124-126 at 1.10.0). Not for
		//! main: an absent main means the table has no current snapshot, and creating main publishes one,
		//! which iceberg_set_current_snapshot does with an explicit assertion that main is absent.
		if (branch == "main") {
			throw InvalidInputException("Cannot fast-forward branch 'main' of table \"%s\": it does not exist; use "
			                            "iceberg_set_current_snapshot to publish a first snapshot",
			                            table_info.name);
		}
		ValidateIcebergRefName(branch, "Iceberg ref name");
		//! The branch must still be absent, and 'to' still at the snapshot it is created at: a concurrent
		//! creation of the branch or move of 'to' aborts the commit.
		requirements.push_back(make_uniq<AssertRefSnapshotId>(branch, optional<int64_t>()));
		requirements.push_back(make_uniq<AssertRefSnapshotId>(to, target_ref.snapshot_id));
		updates.push_back(SetSnapshotRef::CreateRef(target_ref.snapshot_id, branch, "branch", {}));
		modified_refs.insert(branch);
		return result;
	}
	auto &branch_ref = *existing_branch;
	if (branch_ref.type != IcebergRefType::BRANCH) {
		throw InvalidInputException("Ref '%s' is a tag, not a branch", branch);
	}

	result.previous_snapshot_id = branch_ref.snapshot_id;
	if (branch_ref.snapshot_id == target_ref.snapshot_id) {
		//! Nothing to publish (Java: "Nothing to replace"; this includes branch == to), but the result reported
		//! where the branch is: a later change of it in this transaction would contradict that, so it counts as
		//! changed.
		modified_refs.insert(branch);
		return result;
	}
	//! Only a strict extension of the branch's history may be published by fast-forward.
	if (!IsAncestorOfCurrentSnapshot(metadata, branch_ref.snapshot_id, target_ref.snapshot_id)) {
		throw InvalidInputException("Cannot fast-forward: '%s' is not an ancestor of '%s'", branch, to);
	}

	//! Both refs must still be where they were validated: a concurrent move of the branch or the source
	//! aborts the commit instead of publishing a snapshot that was never checked.
	requirements.push_back(make_uniq<AssertRefSnapshotId>(branch, branch_ref.snapshot_id));
	requirements.push_back(make_uniq<AssertRefSnapshotId>(to, target_ref.snapshot_id));
	updates.push_back(make_uniq<SetSnapshotRef>(target_ref.snapshot_id, branch));
	modified_refs.insert(branch);
	return result;
}

IcebergRefMove IcebergTransactionData::TableSetCurrentSnapshot(optional<int64_t> snapshot_id,
                                                               const string &source_ref) {
	if (snapshot_id.has_value() == !source_ref.empty()) {
		throw InvalidInputException("Exactly one of snapshot_id and ref must be provided");
	}
	//! Like rollback, this always moves main (Spark calls ManageSnapshots.setCurrentSnapshot on main, client-side,
	//! ignoring spark.wap.branch: apache-iceberg-1.10.0, spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/
	//! procedures/SetCurrentSnapshotProcedure.java:99). Refuse while the session targets another branch, as
	//! TableRollbackToSnapshot does, rather than move main for a user who expects the WAP branch. fast_forward and
	//! the create/drop functions name their target ref, so they ignore iceberg_wap_branch.
	auto session_branch = GetIcebergWapBranch(context);
	if (session_branch != ICEBERG_MAIN_BRANCH) {
		throw NotImplementedException("Cannot set the current snapshot of table \"%s\" while '%s' is set to '%s': it "
		                              "moves main. RESET %s, or publish with iceberg_fast_forward",
		                              table_info.name, WAP_BRANCH_CONFIG_VARIABLE, session_branch,
		                              WAP_BRANCH_CONFIG_VARIABLE);
	}
	VerifyCanChangeRef("main");
	auto &metadata = table_info.GetMetadata();

	int64_t target_snapshot_id;
	if (!source_ref.empty()) {
		if (source_ref != "main" && modified_refs.count(source_ref)) {
			throw InvalidInputException("Cannot set the current snapshot from ref '%s' of table \"%s\": it was already "
			                            "changed in this transaction",
			                            source_ref, table_info.name);
		}
		target_snapshot_id =
		    GetExistingRef(metadata, source_ref, "Ref", "pass snapshot_id := to select a snapshot id").snapshot_id;
	} else {
		target_snapshot_id = *snapshot_id;
	}
	if (!metadata.FindSnapshotByIdInternal(target_snapshot_id)) {
		throw InvalidInputException("Cannot set the current snapshot to unknown snapshot id: %lld", target_snapshot_id);
	}

	auto main = FindRef(metadata, "main");
	if (main && main->type != IcebergRefType::BRANCH) {
		throw InvalidConfigurationException("Iceberg ref 'main' of table \"%s\" is not a branch", table_info.name);
	}
	IcebergRefMove result;
	result.updated_snapshot_id = target_snapshot_id;
	optional<int64_t> expected_main;
	if (main) {
		result.previous_snapshot_id = main->snapshot_id;
		expected_main = main->snapshot_id;
	}
	if (main && main->snapshot_id == target_snapshot_id) {
		//! Nothing to publish, but a later change of main in this transaction would contradict the
		//! reported result, so it counts as changed.
		modified_refs.insert("main");
		return result;
	}
	if (!source_ref.empty() && source_ref != "main") {
		//! The source ref must still point at the snapshot we resolved it to.
		requirements.push_back(make_uniq<AssertRefSnapshotId>(source_ref, target_snapshot_id));
	}
	requirements.push_back(make_uniq<AssertRefSnapshotId>("main", expected_main));
	updates.push_back(make_uniq<SetSnapshotRef>(target_snapshot_id));
	modified_refs.insert("main");
	return result;
}

} // namespace duckdb
