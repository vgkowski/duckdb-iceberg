# DuckDB extension for Apache Iceberg

This repository contains DuckDB's Apache Iceberg extension. It adds support for reading and writing Iceberg tables, inspecting Iceberg metadata, and attaching Iceberg REST catalogs.

User-facing documentation is available on the [Iceberg extension page](https://duckdb.org/docs/extensions/iceberg).

## Tracking written data files

Set `iceberg_store_written_files = true` before a write to collect the data files it
stages in the connection's temporary table `temp.main.iceberg_written_files`.
Each row contains `query_id`, `table`, `file_path`, `record_count`,
`file_size_in_bytes`, `operation`, and `written_at`.

The operation is `INSERT`, `CTAS`, `UPDATE`, `MERGE_INSERT`, `MERGE_UPDATE`,
`COPY_ON_WRITE_DELETE`, or `REWRITE_DATA_FILES`. MERGE's insert and update actions
share a query ID but have separate operation labels. Operations that write no new
data files produce no rows.

`written_at` is a `TIMESTAMPTZ` captured when a batch of files is reported after
staging. Files in the same batch share the timestamp; separate statements within
a transaction capture their own times. Use `query_id` and `file_path` to break
timestamp ties. For example, after enabling tracking and performing writes:

```sql
SELECT query_id, operation, written_at, "table", file_path,
       record_count, file_size_in_bytes
FROM temp.main.iceberg_written_files
WHERE operation IN ('INSERT', 'CTAS')
ORDER BY written_at, query_id, file_path;
```

The `IcebergDataFileWritten` log type exposes the same operation and `written_at`
fields independently of the storage setting. These describe staged files:
rollback removes the temporary table's rows, while logs remain. The temporary
catalog commits before the Iceberg catalog, so a failed Iceberg commit can leave
tracking rows behind; a client requiring committed files must also check that
the write committed successfully.

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

## Branches and tags

Tables in an attached REST catalog can be read at a branch or tag with
`AT (VERSION => 'name')`, and their refs managed with:

```sql
SELECT * FROM iceberg_create_branch('cat.ns.tbl', 'dev', snapshot_id := 123,
    max_ref_age_ms := 86400000, max_snapshot_age_ms := 3600000, min_snapshots_to_keep := 2);
SELECT * FROM iceberg_create_tag('cat.ns.tbl', 'v1', max_ref_age_ms := 86400000);
SELECT * FROM iceberg_drop_branch('cat.ns.tbl', 'dev');
SELECT * FROM iceberg_drop_tag('cat.ns.tbl', 'v1');
SELECT * FROM iceberg_fast_forward('cat.ns.tbl', 'main', 'dev');
SELECT * FROM iceberg_set_current_snapshot('cat.ns.tbl', snapshot_id := 123);
SELECT * FROM iceberg_set_current_snapshot('cat.ns.tbl', ref := 'dev');
```

- Without `snapshot_id`, a new ref points at the current snapshot of `main`.
  Retention values must be positive; tags accept only `max_ref_age_ms`.
- Ref changes are staged in the transaction and become visible only after it
  commits. Until then, reads in the same transaction see the refs as they were
  when the transaction read the table: `AT (VERSION => ...)` does not see a new
  or moved ref, and a plain read still shows the old `main` after
  `iceberg_fast_forward` or `iceberg_set_current_snapshot` moved it.
- Ref changes are published in one atomic catalog commit that asserts every ref
  involved (and `main`, for a ref created at its current snapshot) still points
  where it was read, so a concurrent change aborts the commit. Such a commit is
  never retried, and that includes appends to other tables committed in the
  same transaction.
- Snapshot ids and retention values must be integers; fractional values are
  refused, not rounded.
- `iceberg_fast_forward(table, branch, to)` moves `branch` forward to the
  snapshot of `to`, which must be a descendant. As in Spark, fast-forwarding a
  branch that does not exist creates it at that snapshot (`previous_ref` is
  NULL), and a branch already at that snapshot is left unchanged
  ([UpdateSnapshotReferencesOperation.java:117-147](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/core/src/main/java/org/apache/iceberg/UpdateSnapshotReferencesOperation.java#L117-L147),
  client-side). `main` is never created this way.
- `iceberg_set_current_snapshot` moves `main` to any snapshot, without an
  ancestry check. Pass exactly one of `snapshot_id` and `ref`, as in Spark
  ([SetCurrentSnapshotProcedure.java:46-90](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/SetCurrentSnapshotProcedure.java#L46-L90)).
- A ref argument is always a ref name, even when it looks like a number
  (`ref := '123'` names a ref; use `snapshot_id := 123` for a snapshot id).
- `main` cannot be created or dropped. It must be a branch: moving a `main`
  that is a tag is refused. A ref can change only once per transaction. Refs
  cannot change after the table's rows were changed in the same transaction,
  and rows cannot change after a ref change.
- Ref names are case-sensitive. A name made only of digits, with an optional
  leading sign (such as `123` or `-7`), is refused on create: `AT (VERSION => '123')`
  reads snapshot id 123, so such a ref could not be read by name. New ref names
  are also refused if they contain non-ASCII digits (Java's `Long.parseLong` accepts
  every character for which `Character.digit` is non-negative, so it reads them as
  digits, client-side:
  [Long.parseLong](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Long.html#parseLong(java.lang.String,int))),
  spell `main` with other
  capitalization (it would be a separate branch easily mistaken for main), contain
  control characters, start or end with whitespace, or are longer than 255 bytes.
- Spark's `IF EXISTS`, `IF NOT EXISTS` and `OR REPLACE` forms of the branch and
  tag DDL are out of scope: creating an existing ref or dropping a missing one
  is an error.
- Known gap: a branch or tag created with `snapshot_id :=` at a snapshot other
  than the head of `main` can be silently removed if another engine expires that
  snapshot concurrently. The catalog drops refs whose snapshot was removed
  ([TableMetadata.java:1444-1451](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/core/src/main/java/org/apache/iceberg/TableMetadata.java#L1444-L1451),
  server-side), and an expiration commit asserts no ref it does not move. Java's
  client has the same gap: it adds no requirement for `remove-snapshots`
  ([UpdateRequirements.java:96-112](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/core/src/main/java/org/apache/iceberg/UpdateRequirements.java#L96-L112),
  client-side), and the REST spec has no requirement type that would close it.
  If the expiration commits first, creating the ref fails because its snapshot
  is unknown ([TableMetadata.java:1312-1315](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/core/src/main/java/org/apache/iceberg/TableMetadata.java#L1312-L1315),
  server-side). Refs created at the head of `main` are not affected: both
  commits assert `main`, and expiration never removes its head.

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
  snapshot would have no parent. REST catalog servers built on Iceberg's Java library
  check a new snapshot's sequence number only when it has a parent, then adopt it as the
  table's last sequence number (server-side, `TableMetadata.Builder.addSnapshot`,
  [TableMetadata.java#L1250-L1259 at 1.10.1](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/TableMetadata.java#L1250-L1259)).
  This client stamps a commit's first attempt from the metadata it loaded when the
  statement was bound, so if another commit advanced the table meanwhile, such a server
  would set a last sequence number that is not above the one it had, and later deletes
  would not apply to newer rows.
  For the same reason, a table whose main has no snapshot but which has other branches
  (for example an empty table another engine wrote a branch of) takes no commit without a
  parent: inserts into main, and branches created from the missing main, fail with "main
  has no snapshot but the table has other branches". Drop the other branches, or set
  main with another engine first. Tags do not block this, since Iceberg engines do not
  commit to tags. A table with no branch other than main takes a first insert into main,
  including one whose old snapshots are unreferenced after a table replacement (the new
  snapshot's sequence number is above all of them). Both checks run when the statement
  is bound and again at every commit attempt, but they only see the metadata this
  transaction loaded: the metadata read when the transaction first used the table, or
  the metadata a retry refreshed. A branch another engine creates after that load is not
  detected, and neither are snapshots it adds without a ref (as with Spark's
  `spark.wap.id`), because the REST requirements can not assert a table's last sequence
  number (the `TableRequirement` types,
  [rest-catalog-open-api.yaml#L3093-L3105 at 1.10.0](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/open-api/rest-catalog-open-api.yaml#L3093-L3105)).
  Avoid writing to a table from two engines at once until main has a snapshot.
- Table replacement (the transaction support prepared for `CREATE OR REPLACE`, which
  SQL does not reach yet) detaches main, so the replacement's first snapshot has no
  parent. Writing data into a replacement is therefore refused when the table has
  branches other than main ("Cannot write the replacement contents"); drop those
  branches in an earlier transaction first. A replacement without data keeps the other
  refs, and a replacement can not write to a WAP branch.
- A commit that creates a branch from main also asserts that main still points at the
  snapshot the branch starts from (its parent, whose manifest list the commit carries
  forward). If main moved, the catalog answers 409: an append is retried onto main's new
  head, other commits fail. This is stricter than Java's client, which asserts only the
  ref a commit changes
  ([UpdateRequirements.java#L117-L129 at 1.10.0](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/core/src/main/java/org/apache/iceberg/UpdateRequirements.java#L117-L129),
  client-side): without it, another engine could expire main's old head and delete its
  files before the branch lands on them.
  A commit fails if the branch it wrote to was removed while the transaction ran, rather
  than recreating it. If another engine removes the branch and recreates it before the
  commit, a retried append follows the recreated branch; a retried DELETE does not, see
  below. Likewise, if this transaction creates the branch and another writer creates it
  first, a retried append lands on top of the other writer's branch head. Java clients
  such as Spark do the same, client-side: every commit attempt refreshes and takes the
  branch's latest snapshot as its parent
  ([SnapshotProducer.java#L253-L256 at 1.10.1](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/SnapshotProducer.java#L253-L256)).
- Commits to different branches of one table (main included) race on the table-wide
  sequence number. How the slower commit is rejected depends on the catalog server, not
  on the client:
  - Servers built on Iceberg's Java library before 1.11 answer HTTP 400
    `ValidationException` "Cannot add snapshot with sequence number N older than last
    sequence number M", which Iceberg clients do not retry
    ([apache/iceberg#15001](https://github.com/apache/iceberg/issues/15001)). This
    extension reports that error as is; re-run the transaction. Examples are
    `apache/iceberg-rest-fixture:1.10.1`, this repository's fixture catalog, against
    which the WAP writes and this race were run
    (`test/sql/local/catalog_test_config_setup/catalog_specifc/fixture/wap_branch_race.test`),
    and Polaris 1.4, which pins Iceberg 1.10.1 (`gradle/libs.versions.toml` on
    `release/1.4.x`) and was only tested for branch reads.
  - Servers built on Iceberg's Java library 1.11 or later answer HTTP 409 instead
    ([apache/iceberg#15126](https://github.com/apache/iceberg/pull/15126)), which the
    usual conflict retry handles for appends; commits that are never retried (UPDATE,
    MERGE) report the 409 as is.
  - Other servers (for example Lakekeeper or Nessie) were not tested with WAP writes.

  A retried DELETE is re-applied only if its branch still descends from the snapshot it
  read through appends alone, so a branch reset or recreated meanwhile aborts.
- Metadata functions (`iceberg_metadata`, `iceberg_files`, `iceberg_manifests`,
  `iceberg_partitions`, `iceberg_column_stats`, `iceberg_partition_stats`),
  `iceberg_scan` and `iceberg_scan_plan` of a catalog table read the branch too, unless
  a snapshot option is given. While the transaction is creating the branch they fail
  instead, since they do not include its pending changes. A query of the table with
  `AT (VERSION => '<branch>')` while the transaction is creating that branch reads its
  pending state, like a plain read.
- A ref that is a tag is rejected, for writes and reads.
- Unlike Spark, the table does not need `write.wap.enabled`: the setting is never
  silently ignored. Like `spark.wap.branch` it is per session: `SET GLOBAL` is refused,
  for `iceberg_wap_branch_create_missing` too.
- The branch is fixed for a table by the first data-changing statement of a
  transaction. It is fixed per table, so one transaction can commit different tables
  to different branches: `BEGIN; SET iceberg_wap_branch = 'a'; INSERT INTO t1 ...;
  SET iceberg_wap_branch = 'b'; INSERT INTO t2 ...; COMMIT;` commits `t1` to `a` and
  `t2` to `b` atomically. A table created in the transaction, and `iceberg_rewrite_data_files`,
  can not target a branch; reading such a table under the setting is an error too.
- `iceberg_rollback_to_snapshot` and `iceberg_set_current_snapshot` always move main,
  so they are refused while the setting names another branch, rather than moving main
  for a session that expects its changes to go to the WAP branch. (Spark's
  `rollback_to_snapshot` and `set_current_snapshot` move main whatever
  `spark.wap.branch` is, client-side:
  [RollbackToSnapshotProcedure.java#L88](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/RollbackToSnapshotProcedure.java#L88),
  [SetCurrentSnapshotProcedure.java#L99](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/SetCurrentSnapshotProcedure.java#L99)
  at 1.10.0.) `iceberg_create_branch`, `iceberg_create_tag`, `iceberg_drop_branch`,
  `iceberg_drop_tag` and `iceberg_fast_forward` name the ref they change, so they ignore
  the setting. As for main, one transaction can not both change a table's rows (on any
  branch) and change its refs.
- Schema, partition spec, sort order and property changes are table-wide in Iceberg:
  an `ALTER TABLE` in a WAP session changes the table for main and every branch, not
  just the WAP branch.
- Empty, `'main'` or `RESET` target main. Other names follow the rules for new ref
  names (see above): numbers (refused even out of `int64` range), names with non-ASCII
  digits, `main` in other capitalization, control characters, or leading or trailing
  whitespace are rejected.
- Publishing (moving main to the audited branch head) is not done by this setting. Use
  `SELECT * FROM iceberg_fast_forward('cat.db.events', 'main', 'audit')` (it works with
  the setting still set), the counterpart of Spark's
  `CALL system.fast_forward('db.events', 'main', 'audit')`.

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
