"""Independent exhaustive oracles, transcript shape, updates and real RPC checks."""
import json
import os
import unittest

from benchmarks.private_indexes import FAMILIES, AuthOrdered, compress_mask, decompress_mask, client_view
from benchmarks.private_memory import Memory


class LocalMemory:
    def __init__(self,rows):self.rows=list(rows);self.reads=0
    def read(self,at):self.reads+=1;return self.rows[at]
    def write(self,at,row):self.rows[at]=row


class IndexTests(unittest.TestCase):
    def data(self):return [[(i*7)%16,i,i%4,bytes([i]*8).hex()] for i in range(16)]
    def test_exhaustive_index_answers_and_fixed_accesses(self):
        data=self.data()
        for family in FAMILIES:
            for group in (1,2,4,8):
                with self.subTest(family=family,group=group):
                    index=FAMILIES[family](data,5,group,4);memory=LocalMemory(index.table.rows);counts=[]
                    for key in range(32):
                        before=memory.reads
                        result=client_view(index).query(memory,key,secondary=key%4,expected_root=getattr(index,'root_hash',None))
                        eligible=[r for r in data if (r[0]<=key if family=='authenticated' else r[0]==key) and (family!='bitmap' or r[2]==key%4)]
                        expected=[max(eligible,key=lambda r:r[0])] if family=='authenticated' and eligible else eligible
                        self.assertEqual(sorted(result),sorted(expected));counts.append(memory.reads-before)
                    self.assertEqual(len(set(counts)),1)
    def test_wavelet_arbitrary_ranges_and_boundaries(self):
        data=self.data();index=FAMILIES['wavelet'](data,5,4,16);memory=LocalMemory(index.table.rows)
        for low in range(-1,34):
            for high in range(low,34):
                expected=[r for r in data if low<=r[0]<=high]
                self.assertEqual(index.query(memory,low,high=high,count_only=True),len(expected))
                self.assertEqual(sorted(index.query(memory,low,high=high)),sorted(expected))
    def test_compression_roundtrip(self):
        for mask in range(1<<12):self.assertEqual(decompress_mask(compress_mask(mask,12)),mask)
    def test_radix_leaf_cutoffs(self):
        data=[[i*2654435761%(2**32-1),i,i%4,'aa'*8] for i in range(40)]
        for cutoff in (0,8,16,28,32):
            index=FAMILIES['radix'](data,32,4,2,leaf_bits=cutoff);memory=LocalMemory(index.table.rows)
            for key in [r[0] for r in data]+[2**32-1]:
                before=memory.reads
                self.assertEqual(client_view(index).query(memory,key),[r for r in data if r[0]==key])
                self.assertEqual(memory.reads-before,index.branch_levels+1)
    def test_duplicate_postings_absence_and_overflow(self):
        data=[[i//2,i,i%4,'ff'*8] for i in range(16)]
        for family in ('radix','hash','posting','wavelet'):
            index=FAMILIES[family](data,4,4,2);memory=LocalMemory(index.table.rows)
            for key in range(16):self.assertEqual(sorted(index.query(memory,key)),sorted(r for r in data if r[0]==key))
        for family in ('radix','hash','posting'):
            with self.assertRaises(ValueError):FAMILIES[family](data,4,4,1)
    def test_authenticated_updates_and_tampering(self):
        data=self.data();index=AuthOrdered(data,5);memory=LocalMemory(index.table.rows)
        live={r[0]:r for r in data}
        for key in (0,7,15,2,9,4,12,1,3,5,6,8,10,11,13,14):
            old=index.root_hash;index.update(memory,key,None);live.pop(key)
            with self.assertRaises(ValueError):index.query(memory,key,expected_root=old)
            for q in range(-1,33):
                eligible=[r for k,r in live.items() if k<=q]
                expected=[max(eligible,key=lambda r:r[0])] if eligible else []
                self.assertEqual(index.query(memory,q,expected_root=index.root_hash),expected)
        row=[7,99,1,'fa'*8];index.update(memory,7,row)
        self.assertEqual(index.query(memory,8,expected_root=index.root_hash),[row])
        root=memory.rows[index.root];node=json.loads(root);node[5]=123
        memory.rows[index.root]=index.table.packed(node)
        with self.assertRaises(ValueError):index.query(memory,8,expected_root=index.root_hash)
    def test_real_private_memory_repeated_accesses(self):
        rows=[bytes([i])*17 for i in range(19)]
        for backend in ('dense','path','singlepass'):
            memory=Memory(rows,backend,partitions=4)
            try:
                for q in range(80):
                    at=(q*q)%len(rows);self.assertEqual(memory.read(at),rows[at])
                memory.write(3,b'x'*17)
                if backend=='singlepass':
                    with self.assertRaises(ValueError):memory.read(3)
                else:self.assertEqual(memory.read(3),b'x'*17)
            finally:
                result=memory.close();self.assertGreater(result['server_cpu_ms'],0)
    @unittest.skipUnless(os.environ.get('PIR_INDEX_BRIDGE'),'set PIR_INDEX_BRIDGE to run compiled artifact tests')
    def test_compiled_stores_and_ramen_epoch_updates(self):
        # Non-power-of-two record count and 15-byte limb boundaries. Repeated
        # reads/writes cross many Ramen epochs and exercise automatic rebuilds.
        rows=[bytes([i])*31 for i in range(19)]
        for backend in ('dense-native','path-native','singlepass-native','ramen'):
            memory=Memory(rows,backend,os.environ['PIR_INDEX_BRIDGE'])
            try:
                for q in range(48):self.assertEqual(memory.read(q*q%19),rows[q*q%19])
                memory.write(3,b'\xff'*31)
                if backend=='singlepass-native':
                    with self.assertRaises(ValueError):memory.read(3)
                else:self.assertEqual(memory.read(3),b'\xff'*31)
            finally:
                result=memory.close()
                self.assertGreater(result['server_cpu_ms'],0)
                for role in result['roles']:
                    self.assertLessEqual(sum(p['cpu_ms'] for p in role['phases']),role['process_cpu_ms']+1)


if __name__=='__main__':unittest.main()
