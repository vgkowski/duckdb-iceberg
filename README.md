# DuckDB extension for Apache Iceberg

This repository contains DuckDB's Apache Iceberg extension. It adds support for reading and writing Iceberg tables, inspecting Iceberg metadata, and attaching Iceberg REST catalogs.

User-facing documentation is available on the [Iceberg extension page](https://duckdb.org/docs/extensions/iceberg).

## Row lineage inside transactions

For format version 3 tables, rows inserted within a transaction have NULL `_row_id`
and `_last_updated_sequence_number` until commit. An UPDATE preserves an already
assigned `_row_id`, including through repeated updates in the same transaction,
but its new `_last_updated_sequence_number` remains NULL until commit. Rows from
before a v2-to-v3 upgrade also retain NULL lineage until a v3 snapshot assigns it.

Each data-changing statement publishes a snapshot at commit. A row inserted and
then updated within one transaction can receive a different `_row_id` in each
of those snapshots: the updates cannot preserve an ID that has not been assigned
yet. After commit, further updates preserve its assigned ID. Use a business key
to identify pending rows, rather than relying on their nullable lineage columns.

## Writing to branches (Write-Audit-Publish)

`SET iceberg_wap_branch = 'audit'` is the counterpart of Spark's `spark.wap.branch`.
While it is set, INSERT, DELETE, UPDATE and MERGE on REST catalog tables commit to
the `audit` branch: each commit builds on the branch head, asserts that the branch
still points there, and moves only that branch. Main and the table's current
snapshot stay where they were, so readers of main keep seeing the old state. Reads
without an AT clause read the branch; `AT (VERSION => 'main')` still reads main.

```sql
SET iceberg_wap_branch = 'audit';
INSERT INTO cat.db.events VALUES (3, 'c');
SELECT snapshot_id FROM iceberg_refs(cat.db.events) WHERE name = 'audit';
RESET iceberg_wap_branch;
```

- Reading a branch that does not exist is an error; unlike Spark, it never falls back
  to main, so an audit can not silently look at main's data. A write creates a missing
  branch from main's current snapshot, as Spark's WAP writes do, and the same
  transaction then reads its pending state. Since DELETE, UPDATE and MERGE read the
  branch first, only INSERT creates one. `SET iceberg_wap_branch_create_missing = false`
  makes writes to a missing branch fail too, so a mistyped name can not create a branch.
  A branch is not created while main has no snapshot (insert into main first): its first
  snapshot would have no parent, and Iceberg catalogs skip the sequence-number check for
  such a snapshot, so a concurrent commit could leave the table's last sequence number
  lowered and later deletes not applied to newer rows.
  For the same reason, a table whose main has no snapshot but which has other branches
  or tags (for example an empty table another engine wrote a branch of) takes no commit
  without a parent: inserts into main, and branches created from the missing main, fail
  with "main has no snapshot but the table has other branches or tags". Publish the ref
  you want as main first, for example with `iceberg_set_current_snapshot`. A table with
  no ref other than main takes a first insert into main, including one whose old
  snapshots are unreferenced after a `CREATE OR REPLACE` (the new snapshot's sequence
  number is above all of them). Both checks run when the statement is bound and again on
  the refreshed metadata of every commit attempt. One race remains that no REST
  requirement can rule out: another engine committing a parent-less branch snapshot to a
  table without other refs at the same time as our first insert into main.
  A commit fails if the branch it wrote to was removed while the transaction ran, rather
  than recreating it. If another engine removes the branch and recreates it before the
  commit, a retried append follows the recreated branch (as Spark's appends do); a
  retried DELETE does not, see below.
- Commits to different branches of one table (main included) race on the table-wide
  sequence number. Catalogs built on Iceberg's Java REST handlers (iceberg-rest-fixture,
  checked with 1.10.1; Polaris; Gravitino) reject the slower commit with HTTP 400
  `ValidationException` "Cannot add snapshot with sequence number N older than last
  sequence number M". That rejection commits nothing, so appends, and DELETEs allowed to
  retry, are re-staged on refreshed metadata, but only when the message names a number
  this commit stamped on one of its new snapshots of a table (N within that range, N at
  most M) and the refreshed metadata of that table shows its last sequence number at
  least M, so another commit advanced it; otherwise the catalog's error is reported.
  Catalogs that word or type this rejection differently report the 400 as an error and
  nothing is retried. UPDATE, MERGE and DELETEs that may not retry fail with "conflicts
  with a concurrent commit to another branch", naming the rejected table and quoting the
  catalog's message; re-run them. A retried DELETE is re-applied only if its branch still descends
  from the snapshot it read through appends alone, so a branch reset or recreated
  meanwhile aborts.
- Metadata functions (`iceberg_metadata`, `iceberg_files`, `iceberg_manifests`,
  `iceberg_partitions`, `iceberg_column_stats`, `iceberg_partition_stats`),
  `iceberg_scan` and `iceberg_scan_plan` of a catalog table read the branch too, unless
  a snapshot option is given. While the transaction is creating the branch they fail
  instead, since they do not include its pending changes.
- A ref that is a tag is rejected, for writes and reads.
- Unlike Spark, the table does not need `write.wap.enabled`: the setting is never
  silently ignored. Like `spark.wap.branch` it is per session: `SET GLOBAL` is refused.
- The branch is fixed for a table by the first data-changing statement of a
  transaction. A table created in the transaction, and `iceberg_rewrite_data_files`,
  can not target a branch; reading such a table under the setting is an error too. `iceberg_rollback_to_snapshot` moves main, so it is refused
  while the setting names another branch.
- Schema, partition spec, sort order and property changes are table-wide in Iceberg:
  an `ALTER TABLE` in a WAP session changes the table for main and every branch, not
  just the WAP branch.
- Empty, `'main'` or `RESET` target main. Names that are numbers (a numeric VERSION
  is a snapshot id, parsed like Java's `Long.parseLong`), contain non-ASCII digits
  (which `Long.parseLong` also reads as digits), spell `main` with other
  capitalization (ref names are case-sensitive), contain control characters or start
  or end with whitespace are rejected.
- Publishing (moving main to the audited branch head) is not done by this setting. Use
  `iceberg_fast_forward` (coming with the fast-forward work) or another engine, such as
  Spark's `CALL system.fast_forward('db.events', 'main', 'audit')`.

## Development setup

Clone the repository with its DuckDB and extension tooling submodules:

```shell
git clone --recurse-submodules https://github.com/duckdb/duckdb-iceberg.git
cd duckdb-iceberg
```

If the repository is already cloned, initialize the submodules with:

```shell
git submodule update --init --recursive
```

### Prerequisites

Building requires a C++ toolchain, CMake, and [vcpkg](https://vcpkg.io/en/getting-started.html). The repository supplies its own vcpkg manifest and ports; pass the vcpkg toolchain file when building.

Catalog-backed tests and test-data generation have additional requirements:

- Python 3 and the packages in `scripts/requirements.txt`
- Java compatible with the selected PySpark runtime
- Docker and Docker Compose for the local REST catalogs
- Git for catalog repositories cloned under `.catalogs/`

The catalog data targets create `.venv-spark4` and install the pinned Python requirements automatically. `make install_requirements` installs those requirements into the currently active Python environment instead.

## Building

Set the vcpkg toolchain path for each invocation or export it in your shell:

```shell
export VCPKG_TOOLCHAIN_PATH=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
make debug
```

Common build targets are:

```shell
make debug       # Debug build with assertions and sanitizers
make release     # Optimized release build; also the default `make` target
make reldebug    # Optimized build with debug information
make relassert   # Optimized build with assertions
```

For a release build, the resulting DuckDB shell and loadable extension are:

```text
build/release/duckdb
build/release/extension/iceberg/iceberg.duckdb_extension
```

The top-level `Makefile` delegates standard build, test, and formatting targets to `extension-ci-tools/makefiles/duckdb_extension.Makefile`. It builds Iceberg together with the required test extensions configured in `extension_config.cmake`.

## Running tests

Build the matching configuration before running its tests. The default test target uses the release test binary:

> [!CAUTION]
> Tests are not assumed to be isolated from one another. Catalog-backed tests in particular can mutate shared catalogs, warehouses, schemas, tables, and generated files. Do not run test files or suites concurrently unless you have verified that they use independent resources and cannot affect each other. This includes separate unittest processes, pytest workers such as `pytest-xdist`, and overlapping local or CI jobs.

```shell
make release
make test
```

Other configurations can be tested explicitly:

```shell
make debug
make test_debug

make reldebug
make test_reldebug
```

Use `T` to narrow a Make-based run, or invoke the sqllogictest runner directly:

```shell
make test_debug T=test/sql/local/iceberg_scans/

./build/debug/test/unittest \
  test/sql/local/iceberg_scans/iceberg_scan.test
```

Run formatting checks and fixes with:

```shell
make format-check
make format-fix
```

## Local REST catalogs

The catalog targets start local services, generate compatible Iceberg test data, and record the selected catalog in `.catalogs/.active_catalog`.

| Catalog | Start services | Start and generate data | Notes |
| --- | --- | --- | --- |
| Apache Iceberg REST fixture | `make fixture` | `make fixture-data` | Uses the Compose setup in `scripts/` |
| Apache Iceberg REST fixture (latest) | `make fixture-latest` | `make fixture-latest-data` | Tracks `apache/iceberg-rest-fixture:latest` in a separate compatibility lane |
| Apache Gravitino | `make gravitino` | `make gravitino-data` | Uses the standalone Gravitino REST server and SeaweedFS Compose setup in `scripts/gravitino/`; may add `seaweedfs` to `/etc/hosts` with `sudo` |
| Lakekeeper | `make lakekeeper` | `make lakekeeper-data` | Clones a pinned revision, applies the repository patch, and may add `seaweedfs` to `/etc/hosts` with `sudo` |
| Nessie | `make nessie` | `make nessie-data` | Uses Nessie's `catalog-auth-s3` Compose setup |
| Apache Polaris | `make polaris` | `make polaris-data` | Clones the `release/1.4.x` branch and uses its RustFS quickstart |
| Test-only v2/v3 mock | `make mock` | Not required | Python standard library and local warehouse; [SQL testing and skip policy](scripts/mock_rest_catalog/README.md) |

Starting a catalog stops the catalog currently named in `.catalogs/.active_catalog`. To stop one explicitly, use `make <catalog>-stop`. Catalog clones, runtime state, and generated data are kept in ignored directories.

To generate only the data needed by one registered generator, pass a pytest `-k` expression through `TEST`:

```shell
TEST=all_types_table make fixture-data
```

The fixture also supports generation for local file-based tests:

```shell
make fixture-data-local
```

### Catalog-backed sqllogictests

Each REST catalog has a config in `test/configs/`. A config initializes credentials and the `my_datalake` attachment, statically loads required extensions, sets `CATALOG_TEST_CONFIG_SETUP`, and lists catalog-specific skips where necessary.

After starting a catalog, resolve its config from `.catalogs/.active_catalog` by running the helper directly:

```shell
./build/debug/test/unittest --order lex \
  "$PWD/test/sql/local/catalog_test_config_setup/*" \
  --test-config "$(scripts/catalog_test_config.sh)"
```

To run one file:

```shell
./build/debug/test/unittest \
  --test-config "$(scripts/catalog_test_config.sh)" \
  test/sql/local/catalog_test_config_setup/catalog_agnostic/create/test_create_table.test
```

`scripts/catalog_test_config.sh` accepts `fixture`, `fixture-latest`, `gravitino`, `lakekeeper`, `nessie`, `polaris`, or `mock`. It reports an error for a missing, empty, local-only, or unknown active-catalog marker.

For the mock, run the catalog-agnostic SQL suite excluding `.test_slow`, with capability flags and skips controlled by `test/configs/mock.json`. You can pass that config directly after `make mock`; see the [mock catalog instructions](scripts/mock_rest_catalog/README.md). It has no Spark generator or Python integration profile. Stop it with `make mock-stop`.

### Catalog-backed Python tests

The Python integration tests also use the active catalog and require a built unittest binary:

```shell
python3 -m pytest -vv test/python \
  --unittest-binary ./build/debug/test/unittest \
  --test-python-verbosity verbose
```

Verbose mode prints the resolved catalog and Spark runtime and explains capability-based skips. Cloud-only tests under `test/python/cloud/` are excluded unless that directory is requested explicitly.

## Test data generation

The generator under `scripts/data_generators/` uses PySpark and Iceberg to populate the active catalog. Generator cases live under `scripts/data_generators/tests/`, and intermediate results are written below `data/generated/intermediates/`.

See [scripts/data_generators/README.md](scripts/data_generators/README.md) for instructions on adding generator cases, catalog connections, skips, and expected failures.

## Acknowledgements

This extension was initially developed as part of a customer project for [RelationalAI](https://relational.ai/), who agreed to open source it. We thank RelationalAI for supporting open source development and enabling us to share the extension with the community.
