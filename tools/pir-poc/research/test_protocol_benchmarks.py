import tempfile
import unittest
from pathlib import Path

from benchmarks.fields import Index, corpus, ids
from benchmarks.hermite import Client, encode
from benchmarks.mpc import Search
from benchmarks.oram import PathOram
from benchmarks.servers import Store, private_row
from benchmarks.transport import Endpoint, totals
from benchmarks.cases import Case, run


class ProtocolTests(unittest.TestCase):
    def test_bit_owners_store_only_assigned_planes(self):
        with tempfile.TemporaryDirectory() as directory:
            result=run(Case(candidate="field-index",rows=16,queries=2,index_workers=2),Path(directory))
        index_stats=result["store_stats"][2:]
        self.assertEqual(len(index_stats),4)
        self.assertEqual(sum(s["stored_bytes"] for s in index_stats),16*2*2)
        self.assertTrue(all(s["stored_bytes"]==16 for s in index_stats))
        self.assertEqual(result["completed_logical_queries"],2)

    def test_delta_compacts_mid_run_and_closes_short_final_cycle(self):
        for queries,phase in ((4,"closing-base-delta-compaction"),(5,"base-delta-compaction")):
            with tempfile.TemporaryDirectory() as directory:
                result=run(Case(candidate="base-delta",rows=16,queries=queries,update_every=2,compact_every=2),Path(directory))
            self.assertIn(phase,{p["role"] for p in result["exporter_phases"]})
            self.assertEqual(result["completed_logical_queries"],queries)

    def test_representations_match_with_partial_bytes(self):
        _,values,_,_ = corpus(19,32,3)
        for mode in ("bitmap","planes","runs","postings"):
            for group in ([1] if mode=="planes" else [1,2,4,8]):
                index = Index(values,16,group,mode)
                for value in range(max(values)+2):
                    self.assertEqual(list(ids(index.equality(value))),[i for i,v in enumerate(values) if v==value])
                self.assertEqual(list(ids(index.bounded_range(1,3))),[i for i,v in enumerate(values) if 1<=v<=3])

    def test_hermite_reconstructs_every_row_and_constant_direction(self):
        records,_,_,_ = corpus(8,16,1)
        for servers in (4,8):
            parameters,table = encode(records,servers)
            client = Client(parameters)
            for i,row in enumerate(records):
                v,points = client.query(i)
                self.assertEqual(client.recover(v,[table[p] for p in points]),row)
                self.assertEqual(client.recover(0,[table[i]]*servers),row)

    def test_served_dense_counts_complete_network(self):
        records,_,_,_ = corpus(19,16,1)
        roles = [Endpoint(Store,role=f"dense-{i}") for i in range(2)]
        try:
            for role in roles:
                role.call("publish",records)
            for i in (0,18,3):
                self.assertEqual(private_row(roles,i,19),records[i])
        finally:
            result = totals(roles)
        self.assertGreater(result["server_cpu_ms"],0)
        self.assertGreater(result["client_to_server_bytes"],2*19*16)

    def test_real_mpc_intersection_range_compaction_and_refresh(self):
        fields = [3,1,0,3,2,1,4,0]
        search = Search(fields,4)
        try:
            for value in (3,9,0):
                node = search.query(value)
                expected = [i for i,v in enumerate(fields) if v==value]
                self.assertEqual(list(ids(search.reconstruct(node))),expected)
                self.assertEqual(sorted(search.compact(node,3)),expected)
            node = search.query(1,3)
            self.assertEqual(list(ids(search.reconstruct(node))),[i for i,v in enumerate(fields) if 1<=v<=3])
            fields[0] = 1
            search.publish(fields)
            self.assertEqual(list(ids(search.reconstruct(search.query(1)))),[0,1,5])
        finally:
            report = totals(search.endpoints)
        self.assertGreater(report["inter_server_bytes"],0)

    def test_oram_rewrites_updates_checkpoints_and_rejects_reuse_after_failure(self):
        records,_,_,_ = corpus(16,32,1)
        role = Endpoint(Store,role="oram")
        try:
            client = PathOram(role,records)
            for i in [0,15,0,7,15]:
                self.assertEqual(client.access(i),records[i])
            replacement = bytes([11])*32
            client.access(7,replacement)
            self.assertEqual(client.access(7),replacement)
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory)/"state"
                _,digest = client.persist(path)
                client.restore(path,digest,client.epoch)
                old_epoch = client.epoch
                client.access(0)
                with self.assertRaises(ValueError):
                    client.restore(path,digest,old_epoch)
            with self.assertRaises(ConnectionError):
                client.access(0,interrupt=True)
            with self.assertRaises(ValueError):
                client.access(0)
        finally:
            totals([role])


if __name__ == "__main__":
    unittest.main()
