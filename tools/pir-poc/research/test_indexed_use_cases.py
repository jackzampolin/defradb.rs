import unittest
from benchmarks.use_case_layouts import PackedPresence,DirectoryPresence,PublicPresence
from test_private_indexes import LocalMemory
from run_indexed_use_cases import matrix


class IndexedUseCaseTests(unittest.TestCase):
    def test_epoch_presence_collision_and_replacement(self):
        for keys in ([0,7,65535,65536,131079],[1,8,65534]):
            data=[[k,i,i%4,'aa'*8] for i,k in enumerate(keys)]
            for layout in (PackedPresence,DirectoryPresence,PublicPresence):
                index=layout(data,4);view=index.view();memory=LocalMemory(index.rows)
                for key in [*range(100),65534,65535,65536,131079]:
                    before=memory.reads
                    expected=[[key,0,0,'01']] if key%65536 in {k%65536 for k in keys} else []
                    self.assertEqual(view.query(memory,key),expected)
                    self.assertEqual(memory.reads-before,0 if layout is PublicPresence else 1)
    def test_matrix_covers_all_result_shapes_and_reuse(self):
        configs=list(matrix('/tmp/corpus','/tmp/bridge'))
        self.assertEqual(len({c.workload for c in configs}),11)
        self.assertTrue(any(c.backend=='singlepass' and c.queries_per_client==256 for c in configs))
        self.assertTrue(any(c.hot_records for c in configs))


if __name__=='__main__':unittest.main()
