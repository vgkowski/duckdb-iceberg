#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

class IcebergSnapshot;

struct IcebergSnapshotScanInfo {
	//! The snapshot to scan
	optional_ptr<const IcebergSnapshot> snapshot;
	//! The schema id assigned to the scan (either from table metadata or the snapshot)
	int32_t schema_id = -1;
	//! Whether this is the transaction-visible head of 'head_branch' (a plain read, or an explicit read of a
	//! branch by name). Uncommitted writes of the current transaction are included only when they target that
	//! branch, see IcebergTransactionData::IsVisibleIn.
	bool is_transaction_head = false;
	//! The branch whose head this is, when is_transaction_head
	string head_branch;
};

} // namespace duckdb
