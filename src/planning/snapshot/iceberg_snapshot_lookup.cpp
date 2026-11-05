#include "iceberg_options.hpp"

namespace duckdb {

IcebergSnapshotLookup IcebergSnapshotLookup::FromTimestamp(timestamp_ms_t snapshot_timestamp) {
	//! Throw if the timestamp is in the future.
	auto now = Value::TIMESTAMP(Timestamp::GetCurrentTimestamp())
	               .DefaultCastAs(LogicalType::TIMESTAMP_MS)
	               .GetValue<timestamp_ms_t>();
	if (snapshot_timestamp > now) {
		throw InvalidInputException(
		    "Can not time travel to '%s', it lies in the future (the current timestamp is '%s')",
		    Value::TIMESTAMPMS(snapshot_timestamp).ToString(), Value::TIMESTAMPMS(now).ToString());
	}
	return IcebergSnapshotLookup(SnapshotFromTimestamp(snapshot_timestamp));
}

IcebergSnapshotLookup IcebergSnapshotLookup::FromAtClause(optional_ptr<BoundAtClause> at) {
	if (!at) {
		return FromLatest();
	}

	auto &unit = at->Unit();
	auto &value = at->GetValue();

	if (value.IsNull()) {
		throw InvalidInputException("NULL values can not be used as the 'unit' of a time travel clause");
	}
	if (unit == "version") {
		return FromVersion(value);
	} else if (unit == "timestamp") {
		return FromTimestamp(value.DefaultCastAs(LogicalType::TIMESTAMP_MS).GetValue<timestamp_ms_t>());
	} else {
		throw InvalidInputException(
		    "Unit '%s' for time travel is not valid, supported options are 'version' and 'timestamp'",
		    unit.GetIdentifierName());
	}
}

IcebergSnapshotLookup IcebergSnapshotLookup::FromRef(const string &ref_name) {
	if (ref_name.empty()) {
		throw InvalidInputException("Iceberg ref name can not be empty");
	}
	return IcebergSnapshotLookup(SnapshotFromRef {ref_name});
}

IcebergSnapshotLookup IcebergSnapshotLookup::FromVersion(const Value &value) {
	if (value.IsNull()) {
		throw InvalidInputException("NULL values can not be used as the 'version' of a time travel clause");
	}
	if (value.type().id() != LogicalTypeId::VARCHAR) {
		return FromSnapshotId(value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>());
	}
	//! Like Spark, a string that parses as a snapshot id is a snapshot id; anything else names a ref
	auto snapshot_id = value.DefaultTryCastAs(LogicalType::BIGINT, nullptr, true);
	if (snapshot_id) {
		return FromSnapshotId(snapshot_id->GetValue<int64_t>());
	}
	return FromRef(StringValue::Get(value));
}

} // namespace duckdb
