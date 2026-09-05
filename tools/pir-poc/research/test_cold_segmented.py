import unittest
from benchmarks.cold_segmented import BitOwners,PackedWavelet,XorDictionary,mask_encode,mask_decode


class Memory:
    def __init__(self,index):self.index=index;self.calls=[]
    def read_table(self,table,address):
        self.calls.append(table);return self.index.tables[table][address]
    def read_xor(self,addresses):
        self.calls.append(0);out=0
        for at in addresses:out^=int.from_bytes(self.index.rows[at],'little')
        return out.to_bytes(self.index.width,'little')


class Tests(unittest.TestCase):
    def test_array_run_containers_and_prefix_filtering(self):
        for mask,kind in (((1<<3)|(1<<4000),b'A'),(((1<<1024)-1)<<100,b'R')):
            encoded=mask_encode(mask,4096,'compressed');self.assertEqual(encoded[:1],kind)
            self.assertEqual(mask_decode(encoded+b'\0'*17,4096),mask)
        data=[[i//2*37,i,i%4,'ab'*8] for i in range(128)]
        index=BitOwners(data,4,bits=16,block=16,prefix_only=True);schedule=None
        for key in range(2500):
            memory=Memory(index);self.assertEqual(sorted(index.view().query(memory,key)),sorted(r for r in data if r[0]==key))
            if schedule is None:schedule=memory.calls
            self.assertEqual(memory.calls,schedule)
    def test_complete_and_fixed_schedule(self):
        data=[[(i//2*37+17)%256,i,i%4,'ab'*8] for i in range(64)]
        for kind in (BitOwners,PackedWavelet):
            for group in (1,2,4,8):
                for block in (4,16):
                    index=kind(data,group,bits=8,block=block);schedule=None
                    for key in range(256):
                        memory=Memory(index);answer=index.view().query(memory,key)
                        self.assertEqual(sorted(answer),sorted(r for r in data if r[0]==key))
                        if schedule is None:schedule=memory.calls
                        self.assertEqual(memory.calls,schedule)
    def test_xor_duplicate_payload_and_absence(self):
        data=[[i//2*1009+17,i,i%4,'ac'*32] for i in range(512)]
        index=XorDictionary(data)
        for key in [r[0] for r in data]+list(range(1000)):
            self.assertEqual(sorted(index.view().query(Memory(index),key)),sorted(r for r in data if r[0]==key))


if __name__=='__main__':unittest.main()
