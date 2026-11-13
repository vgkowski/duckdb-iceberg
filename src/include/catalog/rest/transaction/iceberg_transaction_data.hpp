#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/copy_function.hpp"

#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "rest_catalog/objects/add_snapshot_update.hpp"
#include "catalog/rest/api/iceberg_table_update.hpp"
#include "catalog/rest/api/iceberg_table_requirement.hpp"
#include "catalog/rest/api/iceberg_add_snapshot.hpp"
#include "catalog/rest/api/table_update.hpp"
#include "catalog/rest/api/iceberg_create_table_request.hpp"
#include "catalog/rest/transaction/iceberg_transaction_metadata.hpp"

namespace duckdb {

struct IcebergTable;
struct IcebergCreateTableRequest;
class IcebergMergeDeleteCollector;

enum class IcebergRefType : uint8_t { BRANCH, TAG };

//! Outcome of moving a branch: the snapshot it pointed at, and the one it points at after the commit.
struct IcebergRefMove {
	optional<int64_t> previous_snapshot_id;
	int64_t updated_snapshot_id = 0;
};

struct IcebergTransactionData;

//! The snapshot a read of a catalog table without an AT clause (or snapshot option) sees: the head of the session's
//! iceberg_wap_branch, main when unset. Throws if 'transaction_data' targets another branch, or if the branch does
//! not exist and this transaction is not creating it (a read never falls back to main).
IcebergSnapshotScanInfo GetWapReadSnapshot(ClientContext &context, const IcebergTableMetadata &metadata,
                                           optional_ptr<const IcebergTransactionData> transaction_data);

//! The snapshot an explicit lookup (AT clause or snapshot option) of a catalog table selects. A branch that only
//! 'transaction_data's pending changes create is read like a plain read of it under iceberg_wap_branch: its pending
//! state (main's head plus the pending changes), rather than failing as a missing ref.
IcebergSnapshotScanInfo GetLookupSnapshot(const IcebergTableMetadata &metadata, const IcebergSnapshotLookup &lookup,
                                          optional_ptr<const IcebergTransactionData> transaction_data);

//! A table created in this transaction has no branches until its first commit (to main), so a read of it under a
//! non-main iceberg_wap_branch is refused with this error
[[noreturn]] void ThrowWapReadOfCreatedTable(const string &table_name, const string &wap_branch);

struct IcebergTransactionData {
public:
	IcebergTransactionData(ClientContext &context, IcebergTransaction &transaction, const IcebergTable &table_info);

public:
	int64_t GetCommitRetryCount() const;
	bool SupportsAppendRetry() const;
	bool RetryStateMatches(const IcebergTable &table_info) const;
	//! Whether this transaction stages a DELETE snapshot; gates the commit-retry safety check.
	bool ContainsDelete() const;
	void VerifySnapshotExpirationAllowed() const;
	bool IsFileInvalidated(const IcebergFileIdentity &file) const;
	bool HasInvalidatedFiles() const;
	shared_ptr<IcebergMergeDeleteCollector> GetMergeDeleteCollector(idx_t action_count);
	void VerifyMergeDeletesComplete() const;
	//! Check restrictions left by earlier operations. Caller must hold lock.
	void VerifyCanAddSnapshot() const;

	void AddSnapshot(IcebergSnapshotOperationType operation, vector<IcebergManifestEntry> &&data_files,
	                 IcebergManifestDeletes &&altered_manifests, bool merge_action = false);
	//! 'read_snapshot' is the snapshot the statement's scan of the table read the deleted rows from, when known: it
	//! must be the head of the branch the statement commits to
	void AddDeleteSnapshot(partitioned_manifest_entry_map_t &&delete_files, IcebergManifestDeletes &&altered_manifests,
	                       bool merge_action = false,
	                       optional_ptr<const IcebergSnapshotScanInfo> read_snapshot = nullptr);
	void AddUpdateSnapshot(partitioned_manifest_entry_map_t &&delete_files, vector<IcebergManifestEntry> &&data_files,
	                       IcebergManifestDeletes &&altered_manifests, bool merge_action = false,
	                       optional_ptr<const IcebergSnapshotScanInfo> read_snapshot = nullptr);
	// add a schema update for a table
	void TableAddSchema(int32_t schema_id);
	void TableSetCurrentSchema(int32_t schema_id);
	void TableAddAssertCreate();
	void TableAddAssertUUID();
	void TableAddAssertCurrentSchemaId();
	void TableAddAssertLastAssignedFieldId();
	void TableAddAssertLastAssignedPartitionId();
	void TableAddAssertDefaultSpecId();
	void TableAssignUUID();
	void TableAddUpradeFormatVersion();
	void TableAddPartitionSpec();
	void TableAddSortOrder();
	void TableSetDefaultSortOrder();
	void TableSetDefaultSpec();
	void TableSetProperties(const case_insensitive_map_t<string> &properties);
	void TableRemoveProperties(const vector<string> &properties);
	void TableRemoveSnapshots(const vector<int64_t> &snapshot_ids);
	void TableSetLocation();
	//! Roll main back to an ancestor snapshot (Spark rollback_to_snapshot semantics).
	void TableRollbackToSnapshot(int64_t snapshot_id);
	//! Detach main without removing historical snapshots or enumerating their files.
	void TableReplaceContents(IcebergTableMetadata &metadata);
	//! Create a branch or tag at 'snapshot_id' (default: the current snapshot of main). Returns the snapshot id.
	int64_t TableCreateRef(const string &ref_name, IcebergRefType ref_type, optional<int64_t> snapshot_id,
	                       const IcebergSnapshotRefRetention &retention);
	//! Remove a branch or tag other than main. Returns the snapshot id it pointed at.
	int64_t TableDropRef(const string &ref_name, IcebergRefType ref_type);
	//! Spark fast_forward: move 'branch' to the snapshot of 'to' when the branch's snapshot is its ancestor.
	//! A missing branch other than main is created at that snapshot (previous_snapshot_id is then empty).
	IcebergRefMove TableFastForward(const string &branch, const string &to);
	//! Spark set_current_snapshot: point main at a snapshot or at the snapshot of a ref (exactly one), without
	//! ancestry checks.
	IcebergRefMove TableSetCurrentSnapshot(optional<int64_t> snapshot_id, const string &source_ref);

	//! The branch this transaction's snapshots commit to: fixed by the first data-changing statement from
	//! iceberg_wap_branch, "main" before that
	const string &GetTargetBranch() const;
	//! Throws if this transaction's snapshots target another branch than the session's iceberg_wap_branch, so a scan
	//! never mixes the transaction's pending changes with another branch's state
	void VerifyTargetBranch(const string &session_branch) const;
	//! Whether a scan of 'snapshot' must include this transaction's pending changes: only for the head of the
	//! branch they target, so a read of main never sees changes staged for a WAP branch and vice versa
	bool IsVisibleIn(const IcebergSnapshotScanInfo &snapshot) const;
	//! Whether this transaction's changes create 'branch' (it was missing when the first change was staged)
	bool CreatesBranch(const string &branch) const;
	//! Throws if the commit may not land on 'branch_head' of 'metadata': the target branch was deleted (or is missing
	//! while creating branches is disabled), or the commit's first snapshot would have no parent while the table
	//! already has snapshots. Checked on every attempt against the refreshed metadata, so a retry never recreates a
	//! deleted branch.
	void VerifyCommitBranch(const IcebergTableMetadata &metadata, const IcebergBranchHead &branch_head) const;

private:
	//! Ref changes are validated against the pre-transaction refs, so they cannot follow row changes or
	//! another change to the same ref.
	void VerifyCanChangeRef(const string &ref_name) const;
	//! A snapshot without a parent is only committed to a table with no branch other than main, and never creates a
	//! branch: catalogs exempt such a snapshot from the sequence-number check (see the definition)
	[[noreturn]] void ThrowParentlessCommit(const string &branch, bool table_has_other_branches) const;
	//! Fix the target branch from iceberg_wap_branch on the first data-changing statement; later ones must agree.
	//! Called with lock held.
	void BindTargetBranch(lock_guard<mutex> &guard);
	//! Throws unless 'read_snapshot' (if given) is the head of the bound target branch: the scan resolved its branch
	//! at bind, the target at execution, and the two must agree. Called with lock held, after BindTargetBranch.
	void VerifyReadBranch(lock_guard<mutex> &guard, optional_ptr<const IcebergSnapshotScanInfo> read_snapshot) const;
	void CacheExistingManifestList(lock_guard<mutex> &guard, const IcebergTableMetadata &metadata);
	//! Retains one pending delete manifest per partition spec present in 'delete_files'.
	void AddPendingDeleteManifests(IcebergAddSnapshot &add_snapshot, partitioned_manifest_entry_map_t &&delete_files);
	//! Called with lock held, since MERGE action pipelines may finalize concurrently.
	void AddSnapshotUpdate(unique_ptr<IcebergAddSnapshot> add_snapshot, IcebergManifestDeletes &&altered_manifests,
	                       bool merge_action);

	//! Query IDs distinguish MERGE executions within the same transaction.
	optional<transaction_t> merge_query_id;
	//! Index in alters shared by the current MERGE's manifests and file invalidations.
	idx_t merge_alter_index = 0;
	//! The collector is per table and execution, independently of whether the MERGE writes a snapshot.
	optional<transaction_t> merge_delete_query_id;
	shared_ptr<IcebergMergeDeleteCollector> merge_delete_collector;

public:
	string initial_table_uuid;
	int32_t initial_format_version;
	int32_t initial_schema_id;
	int32_t initial_default_spec_id = 0;
	optional_idx initial_default_sort_order_id;
	optional<int64_t> initial_snapshot_id;
	bool replaces_table = false;

	ClientContext &context;
	IcebergTransaction &transaction;
	const IcebergTable &table_info;
	//! Transaction-wide invalidated file paths, tagged with the alter that first invalidated them
	IcebergManifestDeletes manifest_deletes;
	//! schema updates etc.
	vector<unique_ptr<IcebergTableUpdate>> updates;
	vector<unique_ptr<IcebergTableRequirement>> requirements;
	//! Cached manifest list from the source snapshot
	vector<IcebergManifestListEntry> existing_manifest_list;

	//! Each data-changing statement creates an alter; the actions of one MERGE share an alter.
	vector<reference<IcebergAddSnapshot>> alters;
	//! Snapshot this transaction is based on (the tip when the manifest list was first cached).
	//! Drives the delete commit-retry safety check.
	optional<int64_t> base_snapshot_id;
	//! Starting sequence for transaction-local delete applicability, captured with the base manifest list.
	//! This is not an assigned row lineage value.
	sequence_number_t scan_sequence_number = 0;

	//! If we perform an update that relies on the current schema id staying unchanged
	bool assert_schema_id = false;
	//! Whether this transaction explicitly requires the table to be newly created.
	bool has_assert_create = false;
	//! Whether this transaction rolls main back (TableRollbackToSnapshot)
	bool rolls_back = false;
	//! Set explicitly by maintenance; its committed-table plan cannot be combined with later row changes.
	bool has_maintenance_rewrite = false;
	//! A MERGE chose its actions against the snapshot it read, so its commit can't be re-applied to a newer one.
	bool contains_merge = false;
	//! Refs created, moved or removed by this transaction (exact names; main when rolled back).
	//! Each ref may change once: later validation would otherwise see the pre-transaction state.
	unordered_set<string> modified_refs;
	//! Branch the snapshots commit to, see GetTargetBranch
	string target_branch = "main";
	bool target_branch_bound = false;
	//! Whether the target branch existed when it was bound; if so a commit never recreates it from main
	bool target_branch_existed = true;
	//! iceberg_wap_branch_create_missing when the target branch was bound
	bool may_create_branch = true;
	mutable mutex lock;
};

} // namespace duckdb
