#pragma once

#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

#include <variant>

namespace duckdb {

enum class SnapshotSource : uint8_t { LATEST, FROM_TIMESTAMP, FROM_ID, FROM_REF };

static constexpr const char *SNAPSHOT_OPTIONS_EXCLUSIVE_ERROR =
    "Only one of 'snapshot_from_id', 'snapshot_from_timestamp' and 'snapshot_from_ref' can be used";

struct IcebergSnapshotLookup {
public:
	using SnapshotFromId = int64_t;
	using SnapshotFromTimestamp = timestamp_ms_t;
	//! A branch or tag, resolved through the table's 'refs'
	struct SnapshotFromRef {
		string name;
	};

	// std::monostate represents "latest" (no extra data needed)
	using variant_t = std::variant<std::monostate, SnapshotFromId, SnapshotFromTimestamp, SnapshotFromRef>;

public:
	static IcebergSnapshotLookup FromLatest() {
		return IcebergSnapshotLookup(std::monostate {});
	}
	static IcebergSnapshotLookup FromSnapshotId(int64_t snapshot_id) {
		return IcebergSnapshotLookup(SnapshotFromId(snapshot_id));
	}
	//! Throws if 'snapshot_timestamp' lies beyond the current time
	static IcebergSnapshotLookup FromTimestamp(timestamp_ms_t snapshot_timestamp);
	//! Throws if 'ref_name' is empty
	static IcebergSnapshotLookup FromRef(const string &ref_name);
	//! A VERSION value: an integer is a snapshot id, any other string is a ref name (as in Spark and Trino)
	static IcebergSnapshotLookup FromVersion(const Value &value);
	static IcebergSnapshotLookup FromAtClause(optional_ptr<BoundAtClause> at);

public:
	SnapshotSource GetSource() const {
		if (std::holds_alternative<std::monostate>(snapshot_source)) {
			return SnapshotSource::LATEST;
		}
		if (std::holds_alternative<SnapshotFromId>(snapshot_source)) {
			return SnapshotSource::FROM_ID;
		}
		if (std::holds_alternative<SnapshotFromRef>(snapshot_source)) {
			return SnapshotSource::FROM_REF;
		}
		return SnapshotSource::FROM_TIMESTAMP;
	}
	bool IsLatest() const {
		return std::holds_alternative<std::monostate>(snapshot_source);
	}
	bool IsFromId() const {
		return std::holds_alternative<SnapshotFromId>(snapshot_source);
	}
	bool IsFromTimestamp() const {
		return std::holds_alternative<SnapshotFromTimestamp>(snapshot_source);
	}
	bool IsFromRef() const {
		return std::holds_alternative<SnapshotFromRef>(snapshot_source);
	}

	int64_t GetSnapshotId() const {
		return std::get<SnapshotFromId>(snapshot_source);
	}
	timestamp_ms_t GetSnapshotTimestamp() const {
		return std::get<SnapshotFromTimestamp>(snapshot_source);
	}
	const SnapshotFromRef &GetSnapshotRef() const {
		return std::get<SnapshotFromRef>(snapshot_source);
	}

private:
	explicit IcebergSnapshotLookup(variant_t source) : snapshot_source(std::move(source)) {
	}

private:
	const variant_t snapshot_source;
};

} // namespace duckdb
