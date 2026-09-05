import unittest
from benchmarks.cold_layouts import DirectoryBlocks,AuthDirectoryBlocks
from test_private_indexes import LocalMemory


class LayoutTests(unittest.TestCase):
    def test_directory_complete_duplicate_groups(self):
        data=[[i//3*2654435761+17,i,i%4,'ab'*8] for i in range(96)]
        for group in (1,4,16,64):
            index=DirectoryBlocks(data,group);m=LocalMemory(index.rows)
            for key in [r[0] for r in data]+[0,18,2**64-1]:
                before=m.reads
                self.assertEqual(sorted(index.view().query(m,key)),sorted(r for r in data if r[0]==key))
                self.assertEqual(m.reads-before,1)
    def test_predecessor_boundaries_tamper_and_root_change(self):
        data=[[i*7+17,i,i%4,'cd'*8] for i in range(37)]
        for group in (1,4,16,64):
            index=AuthDirectoryBlocks(data,group);m=LocalMemory(index.rows)
            for key in range(300):
                eligible=[r for r in data if r[0]<=key]
                self.assertEqual(index.view().query(m,key,expected_root=index.root),[eligible[-1]] if eligible else [])
            damaged=bytearray(m.rows[0]);damaged[20]^=1;m.rows[0]=bytes(damaged)
            with self.assertRaises(ValueError):index.view().query(m,17,expected_root=index.root)
            changed=[r.copy() for r in data];changed[0][3]='ef'*8
            new=AuthDirectoryBlocks(changed,group)
            with self.assertRaises(ValueError):index.view().query(LocalMemory(new.rows),17,expected_root=index.root)


if __name__=='__main__':unittest.main()
