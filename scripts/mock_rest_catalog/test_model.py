"""Regression tests for metadata-only snapshot removal in the REST mock."""

import copy
import tempfile
import unittest
from pathlib import Path

from scripts.mock_rest_catalog.model import Catalog, CatalogError


class SnapshotRemovalTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.catalog = Catalog(Path(self.directory.name) / "warehouse")
        self.catalog.create_namespace({"namespace": ["default"]})
        self.key = (("default",), "snapshots")
        self.catalog.create(
            self.key[0],
            {"name": self.key[1], "schema": {"type": "struct", "schema-id": 0, "fields": []}},
        )
        updates = []
        for identifier in (10, 20, 30):
            updates.extend(
                [
                    {
                        "action": "add-snapshot",
                        "snapshot": {
                            "snapshot-id": identifier,
                            "sequence-number": identifier // 10,
                            "schema-id": 0,
                            "timestamp-ms": identifier,
                            "manifest-list": f"{identifier}.avro",
                            "summary": {"operation": "append"},
                        },
                    },
                    self.ref(identifier),
                ]
            )
        self.commit(updates)

    @staticmethod
    def ref(identifier):
        return {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": identifier}

    @staticmethod
    def removal(identifiers):
        return {"action": "remove-snapshots", "snapshot-ids": identifiers}

    def commit(self, updates):
        return self.catalog.commit(self.key, {"requirements": [], "updates": updates})

    def test_removal_preserves_files_and_trims_history_through_last_gap(self):
        self.commit([self.ref(10), self.ref(30)])
        before = copy.deepcopy(self.catalog.tables[self.key])
        metadata_files = list(Path(self.directory.name).rglob("*.metadata.json"))
        result = self.commit([self.removal([10, 10, 999])])["metadata"]
        self.assertEqual([s["snapshot-id"] for s in result["snapshots"]], [20, 30])
        self.assertEqual([s["snapshot-id"] for s in result["snapshot-log"]], [30])
        self.assertEqual(result["current-snapshot-id"], 30)
        self.assertEqual(result["refs"], before["metadata"]["refs"])
        self.assertEqual(result["last-sequence-number"], 3)
        self.assertTrue(all(path.is_file() for path in metadata_files))
        self.assertEqual(len(list(Path(self.directory.name).rglob("*.metadata.json"))), len(metadata_files) + 1)
        self.assertEqual(self.commit([self.removal([10, 999])])["metadata"]["snapshots"], result["snapshots"])

    def test_removing_current_snapshot_clears_main(self):
        result = self.commit([self.removal([30])])["metadata"]
        self.assertEqual([s["snapshot-id"] for s in result["snapshots"]], [10, 20])
        self.assertFalse(result["refs"])
        self.assertIsNone(result.get("current-snapshot-id"))
        self.assertEqual(result["snapshot-log"], [])

    def test_removing_main_retains_snapshots_and_allows_a_new_lineage(self):
        before = copy.deepcopy(self.catalog.tables[self.key]["metadata"])
        remove_main = {"action": "remove-snapshot-ref", "ref-name": "main"}
        result = self.commit([remove_main])["metadata"]
        self.assertFalse(result["refs"])
        self.assertIsNone(result.get("current-snapshot-id"))
        self.assertEqual(result["snapshots"], before["snapshots"])
        self.assertEqual(result["snapshot-log"], before["snapshot-log"])
        self.assertEqual(result["last-sequence-number"], before["last-sequence-number"])
        self.assertIsNone(self.commit([remove_main])["metadata"].get("current-snapshot-id"))
        snapshot = copy.deepcopy(before["snapshots"][-1])
        snapshot.update({"snapshot-id": 40, "sequence-number": 4, "timestamp-ms": 40})
        result = self.commit([{"action": "add-snapshot", "snapshot": snapshot}, self.ref(40)])["metadata"]
        self.assertEqual(result["current-snapshot-id"], 40)
        self.assertNotIn("parent-snapshot-id", result["snapshots"][-1])
        self.assertEqual(result["snapshots"][:-1], before["snapshots"])

    def test_invalid_update_does_not_publish_main_removal(self):
        before = copy.deepcopy(self.catalog.tables[self.key])
        with self.assertRaises(CatalogError):
            self.commit([{"action": "remove-snapshot-ref", "ref-name": "main"}, self.ref(999)])
        self.assertEqual(self.catalog.tables[self.key], before)

    def test_removing_main_preserves_v3_row_id_allocation(self):
        snapshot = copy.deepcopy(self.catalog.tables[self.key]["metadata"]["snapshots"][-1])
        snapshot.update(
            {"snapshot-id": 40, "sequence-number": 4, "timestamp-ms": 40, "first-row-id": 0, "added-rows": 2}
        )
        self.commit(
            [
                {"action": "upgrade-format-version", "format-version": 3},
                {"action": "add-snapshot", "snapshot": snapshot},
                self.ref(40),
            ]
        )
        removed = self.commit([{"action": "remove-snapshot-ref", "ref-name": "main"}])["metadata"]
        self.assertEqual(removed["next-row-id"], 2)
        self.assertEqual(removed["last-sequence-number"], 4)
        snapshot.update(
            {"snapshot-id": 50, "sequence-number": 5, "timestamp-ms": 50, "first-row-id": 2, "added-rows": 1}
        )
        result = self.commit([{"action": "add-snapshot", "snapshot": snapshot}, self.ref(50)])["metadata"]
        self.assertEqual(result["current-snapshot-id"], 50)
        self.assertEqual(result["next-row-id"], 3)
        self.assertEqual(result["snapshots"][:-1], removed["snapshots"])

    def test_removing_other_refs_does_not_remove_main(self):
        branch = {"action": "set-snapshot-ref", "ref-name": "branch", "type": "branch", "snapshot-id": 20}
        before = self.commit([branch])["metadata"]
        self.assertIn("branch", before["refs"])
        result = self.commit([{"action": "remove-snapshot-ref", "ref-name": "branch"}])["metadata"]
        self.assertNotIn("branch", result["refs"])
        self.assertEqual(result["refs"]["main"], before["refs"]["main"])
        self.assertEqual(result["current-snapshot-id"], 30)
        self.assertEqual(result["snapshots"], before["snapshots"])
        # Like Java's TableMetadata.Builder.removeRef, removing an unknown ref is a no-op.
        self.assertEqual(
            self.commit([{"action": "remove-snapshot-ref", "ref-name": "branch"}])["metadata"]["refs"], result["refs"]
        )
        with self.assertRaises(CatalogError) as error:
            self.commit([{"action": "remove-snapshot-ref", "ref-name": ""}])
        self.assertEqual(error.exception.code, 400)

    def test_later_invalid_update_does_not_publish_removal(self):
        before = copy.deepcopy(self.catalog.tables[self.key])
        with self.assertRaises(CatalogError) as error:
            self.commit([self.removal([10]), self.ref(999)])
        self.assertEqual(error.exception.code, 400)
        self.assertIn("Unknown snapshot reference", str(error.exception))
        self.assertEqual(self.catalog.tables[self.key], before)

    def snapshot(self, identifier, sequence_number, parent=None, **extra):
        snapshot = copy.deepcopy(self.catalog.tables[self.key]["metadata"]["snapshots"][-1])
        snapshot.pop("parent-snapshot-id", None)
        snapshot.update({"snapshot-id": identifier, "sequence-number": sequence_number, "timestamp-ms": identifier})
        if parent is not None:
            snapshot["parent-snapshot-id"] = parent
        snapshot.update(extra)
        return {"action": "add-snapshot", "snapshot": snapshot}

    def test_add_snapshot_sequence_and_parent_checks_match_java(self):
        # Like Java's TableMetadata.Builder.addSnapshot: a stale sequence number with a parent is a
        # ValidationException (400) carrying Java's message.
        before = copy.deepcopy(self.catalog.tables[self.key])
        with self.assertRaises(CatalogError) as error:
            self.commit([self.snapshot(40, 3, parent=30)])
        self.assertEqual(error.exception.code, 400)
        self.assertIn(
            "Cannot add snapshot with sequence number 3 older than last sequence number 3", str(error.exception)
        )
        self.assertEqual(self.catalog.tables[self.key], before)
        # The parent is not validated.
        result = self.commit([self.snapshot(40, 4, parent=999)])["metadata"]
        self.assertEqual(result["snapshots"][-1]["parent-snapshot-id"], 999)
        # Stricter than Java on purpose: a parent-less snapshot must also raise last-sequence-number.
        before = copy.deepcopy(self.catalog.tables[self.key])
        with self.assertRaises(CatalogError) as error:
            self.commit([self.snapshot(50, 4)])
        self.assertEqual(error.exception.code, 400)
        self.assertIn("Mock catalog refuses a sequence number 4", str(error.exception))
        self.assertEqual(self.catalog.tables[self.key], before)
        result = self.commit([self.snapshot(50, 5)])["metadata"]
        self.assertEqual(result["last-sequence-number"], 5)

    def test_row_id_overlap_is_a_validation_error(self):
        self.commit(
            [
                {"action": "upgrade-format-version", "format-version": 3},
                self.snapshot(40, 4, parent=30, **{"first-row-id": 0, "added-rows": 2}),
            ]
        )
        before = copy.deepcopy(self.catalog.tables[self.key])
        with self.assertRaises(CatalogError) as error:
            self.commit([self.snapshot(50, 5, parent=40, **{"first-row-id": 1, "added-rows": 1})])
        self.assertEqual(error.exception.code, 400)
        self.assertIn("first-row-id is behind table next-row-id: 1 < 2", str(error.exception))
        self.assertEqual(self.catalog.tables[self.key], before)

    def test_malformed_snapshot_ids_do_not_publish(self):
        before = copy.deepcopy(self.catalog.tables[self.key])
        for identifiers in (None, 10, [True], ["10"], [10.5], [None], [1 << 63]):
            with self.subTest(identifiers=identifiers):
                with self.assertRaises(CatalogError) as error:
                    self.commit([self.removal(identifiers)])
                self.assertEqual(error.exception.code, 400)
                self.assertIn("snapshot-ids", str(error.exception))
                self.assertEqual(self.catalog.tables[self.key], before)


if __name__ == "__main__":
    unittest.main()
