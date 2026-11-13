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
                                                  vector<IcebergManifestListEntry> &existing_manifest_list) {
	existing_manifest_list.clear();

	auto current_snapshot = metadata.GetLatestSnapshot();
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

void IcebergTransactionData::CacheExistingManifestList(lock_guard<mutex> &guard, const IcebergTableMetadata &metadata) {
	if (!alters.empty()) {
		return;
	}
	base_snapshot_id = LoadExistingManifestList(context, metadata, existing_manifest_list);
	scan_sequence_number = IcebergSnapshot::NextSequenceNumber(metadata.last_sequence_number);
}

void IcebergTransactionData::AddSnapshot(IcebergSnapshotOperationType operation,
                                         vector<IcebergManifestEntry> &&data_files,
                                         IcebergManifestDeletes &&altered_manifests, bool merge_action) {
	//! Serialize snapshot operations so transaction scans retain their insertion order.
	lock_guard<mutex> guard(lock);
	VerifyNoSnapshotExpiration(updates, "Data changes");

	auto &table_metadata = table_info.GetMetadata();
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
                                               IcebergManifestDeletes &&altered_manifests, bool merge_action) {
	//! Serialize snapshot operations so transaction scans retain their insertion order.
	lock_guard<mutex> guard(lock);
	VerifyNoSnapshotExpiration(updates, "Data changes");

	auto &table_metadata = table_info.GetMetadata();
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
                                               IcebergManifestDeletes &&altered_manifests, bool merge_action) {
	//! Serialize snapshot operations so transaction scans retain their insertion order.
	lock_guard<mutex> guard(lock);
	VerifyNoSnapshotExpiration(updates, "Data changes");

	auto &table_metadata = table_info.GetMetadata();

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
		//! The commit points main at the transaction's last new snapshot, which was based on the old refs.
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
