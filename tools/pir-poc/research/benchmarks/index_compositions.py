"""Isolated, complete-answer experiments for the six private-index families."""
import argparse
from dataclasses import dataclass, asdict
import hashlib
import json
import math
from pathlib import Path
import random
import secrets
import time
import traceback

from .mpc import Search
from .private_indexes import FAMILIES, client_view
from .private_memory import Memory
from .transport import parallel_calls, process_stats, totals


@dataclass
class Config:
    family: str = 'radix'
    backend: str = 'path'
    rows: int = 64
    row_bytes: int = 32
    queries: int = 16
    fanout: int = 2
    slots: int = 4
    group: int = 4
    seed: int = 1
    distribution: str = 'shuffled'
    count_only: bool = False
    range_width: int = 2
    update_every: int = 0
    mutation: str = 'value'
    partitions: int = 4
    field_bits: int = 0
    key_layout: str = 'contiguous'
    radix_leaf_bits: int = 0
    client_state_cap: int = 64<<20
    client_online_cap: int = 1<<20
    resident_cap: int = 2<<30


class Intersection:
    def __init__(self,size):self.search=Search([0]*size,1)
    def __call__(self,a,b):
        s=self.search;s.next=2
        for destination,value in enumerate((a,b)):
            x,y=secrets.randbits(s.n),secrets.randbits(s.n);shares=(x,y,x^y^value)
            inputs=[[destination,[shares[i].to_bytes(s.width,'little'),shares[(i+1)%3].to_bytes(s.width,'little')],destination==0] for i in range(3)]
            parallel_calls(s.endpoints,'shared-input',inputs)
        return s.reconstruct(s.binary('and',0,1))
    def wire(self):return sum(e.sent for e in self.search.endpoints),sum(e.received for e in self.search.endpoints)
    def mark(self):return {str(e.process.pid):e.calls for e in self.search.endpoints}
    def close(self):return totals(self.search.endpoints)


def run(c,binary=None):
    if c.family not in FAMILIES or c.backend not in ('dense','path','singlepass','ramen','dense-native','path-native','singlepass-native'):raise ValueError('unknown composition')
    if not 2<=c.rows<=65536 or not 8<=c.row_bytes<=2008 or not 1<=c.queries<=10000:raise ValueError('execution dimensions')
    if c.fanout<1 or c.slots<c.fanout or c.group not in (1,2,4,8,16,32,64,128):raise ValueError('index dimensions')
    if c.family=='radix' and c.group>8:raise ValueError('radix width bounded to 8 bits')
    if c.family=='authenticated' and c.fanout!=1:raise ValueError('unique authenticated keys required')
    if c.mutation not in ('value','key','delete','insert'):raise ValueError('mutation')
    rng=random.Random(c.seed)
    keys=[i//c.fanout for i in range(c.rows)]
    if c.distribution=='shuffled':rng.shuffle(keys)
    elif c.distribution!='clustered':raise ValueError('distribution')
    bits=c.field_bits or max(1,(max(keys)+2).bit_length())
    if not 1<=bits<=64 or max(keys)+2>=1<<bits:raise ValueError('field domain too small')
    if c.family in ('posting','bitmap','authenticated') and bits>16:raise ValueError('direct directory pilot bounded to 16-bit domain')
    if c.key_layout=='scattered':keys=[(k*2654435761+17)%((1<<bits)-1) for k in keys]
    elif c.key_layout!='contiguous':raise ValueError('key layout')
    if c.family=='authenticated' and len(set(keys))!=len(keys):raise ValueError('scattered key collision')
    data=[[key,i,i%4,hashlib.shake_256(str(i).encode()).digest(c.row_bytes).hex()] for i,key in enumerate(keys)]
    if c.mutation=='insert':
        if c.family!='authenticated':raise ValueError('insert pilot requires fixed authenticated key slots')
        live=data[:-max(1,c.rows//4)]
    else:live=list(data)
    def build():
        kwargs={'leaf_bits':c.radix_leaf_bits} if c.family=='radix' else {}
        return FAMILIES[c.family](live,bits,c.group,c.slots,**kwargs)
    cpu=time.process_time_ns();index=build()
    build_cpu=(time.process_time_ns()-cpu)/1e6
    logical_bytes=len(index.table.rows)*index.table.width
    if logical_bytes*32>c.resident_cap:raise ValueError('conservative index resident preflight')
    if c.backend.startswith('singlepass') and logical_bytes>64<<20:raise ValueError('setup download cap')
    memory=None;mpc=None;samples=[];updates=[];components=[]
    setup_start=time.process_time_ns()
    try:
        memory=Memory(index.table.rows,c.backend,binary,c.partitions)
        if c.family=='bitmap':mpc=Intersection(index.block)
        setup_cpu=(time.process_time_ns()-setup_start)/1e6
        setup_wire=memory.wire()
        if mpc:setup_wire=tuple(a+b for a,b in zip(setup_wire,mpc.wire()))
        for q in range(c.queries):
            forced_query=None;forced_secondary=None
            if c.update_every and q and q%c.update_every==0:
                start=time.process_time_ns();old_root=getattr(index,'root_hash',None)
                key=keys[q%len(keys)];at=next((i for i,r in enumerate(live) if r[0]==key),None)
                old_key=key
                if c.mutation=='delete':
                    row=None
                    if at is not None:live.pop(at)
                elif c.mutation=='insert':
                    available=next((r for r in data if r[0] not in {x[0] for x in live}),None)
                    if available is None:raise ValueError('insert reserve exhausted')
                    row=list(available);key=row[0];live.append(row)
                elif c.mutation=='key':
                    at=q%len(live);row=list(live[at]);old_key=row[0]
                    occupied={r[0] for r in live}
                    key=next((k for k in range(1<<bits) if k not in occupied),None)
                    if key is None:raise ValueError('no reserved key slot')
                    row[0]=key;live[at]=row
                else:
                    if at is None:raise ValueError('update key missing')
                    row=list(live[at]);row[3]=(bytes([q%256])+bytes.fromhex(row[3])[1:]).hex();live[at]=row
                if c.family=='authenticated' and not c.backend.startswith('singlepass'):
                    if c.mutation=='key':index.update(memory,old_key,None)
                    index.update(memory,key,row)
                    if old_root!=index.root_hash:
                        try:index.query(memory,key,expected_root=old_root)
                        except ValueError:pass
                        else:raise AssertionError('stale root was accepted')
                    kind='incremental-path'
                else:
                    components.append(memory.close())
                    index=build()
                    memory=Memory(index.table.rows,c.backend,binary,c.partitions);kind='full-generation-and-client-refresh'
                updates.append(dict(query=q,controller_cpu_ms=(time.process_time_ns()-start)/1e6,kind=kind))
                forced_query=key;forced_secondary=row[2] if row else 0
            # Fixed schedule alternates present, repeated, upper absence, lower
            # boundary and range queries. No target is sent in plaintext.
            key=keys[(q//2*7)%len(keys)] if q%4!=3 else (1<<bits)-1
            secondary=data[(q//2*7)%len(data)][2]
            if forced_query is not None:key=forced_query;secondary=forced_secondary
            high=key+c.range_width-1 if c.family=='wavelet' else key
            if c.family=='authenticated':
                eligible=[r for r in live if r[0]<=key];expected=[max(eligible,key=lambda r:r[0])] if eligible else []
            else:
                expected=[r for r in live if key<=r[0]<=high and (c.family!='bitmap' or r[2]==secondary)]
            if len(expected)>c.slots and not c.count_only:raise ValueError('oracle complete-answer overflow')
            view=client_view(index)
            before=memory.wire();before_mpc=mpc.wire() if mpc else (0,0)
            phase_before=memory.mark() | (mpc.mark() if mpc else {})
            start=time.process_time_ns();wall=time.perf_counter_ns();reads=memory.reads
            answer=view.query(memory,key,high=high,secondary=secondary,count_only=c.count_only,intersect=mpc,
                               expected_root=getattr(index,'root_hash',None))
            client_cpu=(time.process_time_ns()-start)/1e6;wall_ms=(time.perf_counter_ns()-wall)/1e6
            # Correctness oracle is deliberately outside query timing.
            if c.family=='wavelet' and c.count_only:
                if answer!=len(expected):raise AssertionError('range count mismatch')
            elif sorted(answer,key=lambda r:r[1])!=sorted(expected,key=lambda r:r[1]):raise AssertionError('complete answer mismatch')
            after=memory.wire();after_mpc=mpc.wire() if mpc else (0,0)
            phase_after=memory.mark() | (mpc.mark() if mpc else {})
            upload=after[0]-before[0]+after_mpc[0]-before_mpc[0]
            download=after[1]-before[1]+after_mpc[1]-before_mpc[1]
            samples.append(dict(query=q,client_cpu_ms=client_cpu,wall_ms=wall_ms,upload_bytes=upload,download_bytes=download,
                private_record_reads=memory.reads-reads,matches=len(expected),correct=True,
                role_phase_ranges={pid:[start,phase_after[pid]] for pid,start in phase_before.items()}))
        components.append(memory.close());memory=None
        if mpc:components.append(mpc.close());mpc=None
    finally:
        if memory:memory.close()
        if mpc:mpc.close()
    roles=[r for component in components for r in component['roles']]
    by_pid={str(r['pid']):r for r in roles}
    for sample in samples:
        sample['all_server_cpu_ms']=sum(sum(p['cpu_ms'] for p in by_pid[pid]['phases'][start:end]) for pid,(start,end) in sample['role_phase_ranges'].items())
    phase_cpu=sum(p['cpu_ms'] for r in roles for p in r.get('phases',[]) if p['phase'] not in ('publish','init','close','stats','seed'))
    server_cpu=sum(c['server_cpu_ms'] for c in components)
    # Controller is both public publisher and honest client in this harness.
    # Report their combined cost conservatively, never hide it as free setup.
    lifecycle_controller=build_cpu+setup_cpu+sum(u['controller_cpu_ms'] for u in updates)
    navigation_bytes=len(json.dumps({k:v for k,v in vars(client_view(index)).items() if k!='table'}))
    max_state=max(c.get('client_state_bytes',0) for c in components)+navigation_bytes
    budget_failures=[]
    if max_state>c.client_state_cap:budget_failures.append('persistent-client-state')
    if any(max(s['upload_bytes'],s['download_bytes'])>c.client_online_cap for s in samples):budget_failures.append('online-client-wire')
    if any(s['client_cpu_ms']>1000 for s in samples):budget_failures.append('online-client-cpu')
    if setup_cpu>10000:budget_failures.append('setup-controller-cpu')
    if setup_wire[1]>64<<20:budget_failures.append('setup-download')
    peak=process_stats()['peak_rss_bytes']
    if peak>128<<20:budget_failures.append('controller-transient-rss')
    if sum(c.get('aggregate_peak_role_rss_bytes',0) for c in components)>c.resident_cap:budget_failures.append('aggregate-role-rss')
    return dict(config=asdict(c),correct=True,budget_failures=budget_failures,qualification='local prototype; no production/cross-language ranking',
        index_records=len(index.table.rows),record_bytes=index.table.width,logical_index_bytes=logical_bytes,
        build_cpu_ms=build_cpu,initial_setup_controller_cpu_ms=setup_cpu,initial_setup_wire_bytes=setup_wire,
        lifecycle_controller_cpu_ms=lifecycle_controller,server_cpu_ms=server_cpu,
        all_server_cpu_per_answer_ms=server_cpu/len(samples),
        server_plus_lifecycle_controller_per_answer_ms=(server_cpu+lifecycle_controller)/len(samples),
        all_participant_cpu_per_answer_ms=(server_cpu+lifecycle_controller+sum(s['client_cpu_ms'] for s in samples))/len(samples),
        active_server_phase_cpu_ms=phase_cpu,client_state_bytes=max_state,controller_peak_rss_bytes=peak,
        samples=samples,updates=updates,components=components,
        implementation_sha256={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in Path(__file__).parent.iterdir() if p.suffix in ('.py','.rs')},
        assumptions=['loopback/pipe transport; actual isolated role processes; no WAN projection',
            'honest client; visited index nodes/candidates may be disclosed to client',
            'public dimensions, output bound, workload class and writer update schedule',
            'Ramen scalar limbs charged separately; no vector-block optimization assumed',
            'Path ORAM single owner; Python SinglePass follows repository show-and-shuffle',
            'authenticated variant uses SHA-256 trusted fresh root, not production Poseidon witnesses'])


def main():
    p=argparse.ArgumentParser();p.add_argument('config',type=Path);p.add_argument('output',type=Path);p.add_argument('--ramen-binary')
    a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    try:result=run(Config(**json.loads(a.config.read_text())),a.ramen_binary)
    except Exception as e:
        (a.output/'failure.json').write_text(json.dumps(dict(error=str(e),traceback=traceback.format_exc()),indent=2));raise
    (a.output/'result.json').write_text(json.dumps(result,indent=2))


if __name__=='__main__':main()
