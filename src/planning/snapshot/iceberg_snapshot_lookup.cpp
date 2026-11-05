#include "iceberg_options.hpp"

#include "duckdb/common/string_util.hpp"

#include <cerrno>
#include <cstdlib>

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

void IcebergSnapshotLookup::VerifyOptionNotNull(const string &option, const Value &value) {
	if (value.IsNull()) {
		throw InvalidInputException("'%s' can not be NULL", option);
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
	auto &str = StringValue::Get(value);
	int64_t snapshot_id;
	if (TryParseSnapshotId(str, snapshot_id)) {
		return FromSnapshotId(snapshot_id);
	}
	return FromRef(str);
}

bool IcebergSnapshotLookup::TryParseSnapshotId(const string &str, int64_t &result) {
	//! Like Spark (Long.parseLong): an optional sign and only digits, in int64 range, is a snapshot id;
	//! any other string, including one that overflows, names a ref
	idx_t pos = (!str.empty() && (str[0] == '+' || str[0] == '-')) ? 1 : 0;
	bool all_digits = pos < str.size();
	for (idx_t i = pos; i < str.size() && all_digits; i++) {
		all_digits = StringUtil::CharacterIsDigit(str[i]);
	}
	if (!all_digits) {
		return false;
	}
	errno = 0;
	char *end = nullptr;
	auto snapshot_id = std::strtoll(str.c_str(), &end, 10);
	if (errno == ERANGE || end != str.c_str() + str.size()) {
		return false;
	}
	result = snapshot_id;
	return true;
}

} // namespace duckdb
