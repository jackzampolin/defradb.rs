"""Complete-search, fixed-schedule, and fresh-client isolation regression tests."""
import os
import unittest
from benchmarks.cold_search import BinaryTags,JsonTags,BinaryPatricia,Config,run
from test_private_indexes import LocalMemory


class ColdTests(unittest.TestCase):
    def test_compressed_bit_navigation_and_padded_absence(self):
        data=[[i//2*2654435761+17,i,i%4,'ab'*8] for i in range(100)]
        for group in (1,2,4,8):
            index=BinaryPatricia(data,group);memory=LocalMemory(index.rows)
            for key in [r[0] for r in data]+[0,1,2**64-1]:
                start=memory.reads
                self.assertEqual(sorted(index.view().query(memory,key)),sorted(r for r in data if r[0]==key))
                self.assertEqual(memory.reads-start,index.depth)
    def test_collision_continuations_and_absent_keys(self):
        data=[[i//7,i,i%4,bytes([i%256]*8).hex()] for i in range(63)]
        for cls in (BinaryTags,JsonTags):
            index=cls(data,2);view=index.view();self.assertNotIn('rows',vars(view))
            memory=LocalMemory(index.rows);counts=[]
            for key in range(16):
                before=memory.reads
                self.assertEqual(sorted(view.query(memory,key)),sorted(r for r in data if r[0]==key))
                counts.append(memory.reads-before)
            self.assertGreater(index.pages,1);self.assertEqual(len(set(counts)),1)

    def test_fresh_process_clients_and_charged_setup(self):
        binary=os.environ.get('PIR_COLD_NATIVE')
        if not binary:self.skipTest('PIR_COLD_NATIVE is required for integration')
        for backend in ('dense','singlepass'):
            result=run(Config(rows=16,clients=4,backend=backend),binary)
            self.assertTrue(result['correct'])
            self.assertEqual(len({c['pid'] for c in result['clients']}),4)
            for c in result['clients']:
                self.assertGreater(c['setup_wire'][1],0)
                self.assertEqual(len(c['samples']),1)
                if backend=='singlepass':self.assertGreater(c['server_phase_cpu_ms']['setup'],0)
                self.assertEqual(c['budget_failures'],[])

    def test_actual_finite_encoding_and_fresh_clients(self):
        binary=os.environ.get('PIR_COLD_FINITE')
        if not binary:self.skipTest('PIR_COLD_FINITE is required for actual encoding integration')
        result=run(Config(rows=16,clients=4,backend='finite'),binary)
        self.assertTrue(result['correct'])
        for client in result['clients']:
            self.assertEqual(client['budget_failures'],[])
            self.assertGreater(client['server_phase_cpu_ms']['setup'],0)

    def test_segmented_and_fused_native_services(self):
        binary=os.environ.get('PIR_COLD_NATIVE')
        if not binary:self.skipTest('PIR_COLD_NATIVE required')
        for family in ('xor','directory','auth-directory','bit-owners','prefix-owner','packed-wavelet'):
            result=run(Config(family=family,rows=16,clients=4,group=4,field_bits=8,
                              fanout=1 if family=='auth-directory' else 2),binary)
            self.assertTrue(result['correct'])
            for c in result['clients']:
                self.assertEqual(c['budget_failures'],[])
                self.assertIn('VmHWM',c['client_process']['rss_method'])


if __name__=='__main__':unittest.main()
