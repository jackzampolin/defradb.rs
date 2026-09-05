import unittest

from total_work import bootstrap_ratio, field_preflight, many_server_preflight, matrix, summarize


class AccountingTests(unittest.TestCase):
    def test_more_workers_do_not_reduce_aggregate_index(self):
        report = field_preflight(1_000_000_000,16,1)
        self.assertEqual(report["aggregate_index_bytes"],8_000_000_000)
        placements = report["role_placement"]
        self.assertEqual(len({p["aggregate_index_bytes"] for p in placements}),1)
        self.assertLess(placements[-1]["max_index_bytes_per_worker"], placements[0]["max_index_bytes_per_worker"])
        self.assertFalse(report["portable_online_bytes_pass"])

    def test_many_server_dimensions_are_admissible(self):
        import math
        for p in many_server_preflight(256):
            self.assertGreaterEqual(math.comb(p["m"]+p["d"],p["m"]),256)
            self.assertGreater(p["t"]*p["servers"],p["d"])
            self.assertGreater(p["q"],p["servers"])
            self.assertEqual(p["encoded_symbols_per_server"], p["derivatives"]*p["q"]**p["m"])

    def test_fresh_run_uncertainty_and_missing_measurements(self):
        self.assertIsNone(bootstrap_ratio([1]*4,[2]*4))
        self.assertEqual(bootstrap_ratio([1]*5,[2]*5),[.5,.5])
        self.assertEqual(summarize([dict(status="timeout")]),[])

    def test_screen_uses_representable_field_domain(self):
        for c in matrix("screen"):
            self.assertLess((c["rows"]+c["fanout"]-1)//c["fanout"],2**c["field_bits"])


if __name__ == "__main__":
    unittest.main()
