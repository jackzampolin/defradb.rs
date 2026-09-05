import unittest
from benchmarks.matrix import matrix
from benchmarks.cases import Case


class MatrixTests(unittest.TestCase):
    def test_all_families_and_unique_names(self):
        for profile in ("smoke","screen","scale"):
            cases=matrix(profile)
            self.assertEqual({c["family"] for c in cases},{f"B{i}" for i in range(9)})
            self.assertEqual(len(cases),len({c["name"] for c in cases}))
            self.assertEqual({c["engine"] for c in cases},{"native","native-clients","protocol","zelda","gpu"})

    def test_smoke_protocols_only_gate_for_declared_memory(self):
        for row in matrix():
            if row["engine"]!="protocol":continue
            try:Case(**row["config"]).validate()
            except ValueError as error:
                self.assertIn("resident estimate",str(error))
                self.assertTrue(row["config"]["servers"]>=32 or row["config"]["index_workers"]>=16)

    def test_budget_and_circuit_bounds_before_allocation(self):
        with self.assertRaisesRegex(ValueError,"resident estimate"):Case(rows=1000000,row_bytes=1024).validate()
        with self.assertRaisesRegex(ValueError,"compaction circuit"):Case(candidate="mpc-compact-dense",rows=512).validate()
        with self.assertRaisesRegex(ValueError,"unknown"):Case(candidate="unimplemented").validate()


if __name__=="__main__":unittest.main()
