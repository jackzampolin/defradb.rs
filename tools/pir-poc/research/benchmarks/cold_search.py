"""Fresh-process clients searching persistent native replicas directly.

The parent is a public-metadata publisher and trusted test oracle. Client spawn
arguments contain public navigation and a predicate, never source records or
expected results. JSON/base64 RPC overhead is deliberately reported.
"""
import argparse
from dataclasses import dataclass, asdict, replace
import hashlib
import json
import multiprocessing as mp
from multiprocessing.reduction import DupFd
import itertools
import math
import secrets
import os
from pathlib import Path
import select
import struct
import time
import traceback
import subprocess
import resource

from .native_memory import NativeEndpoint,encode as native_encode,decode as native_decode
from .private_memory import Memory
from .private_indexes import FAMILIES, client_view
from .transport import socket_pair, send, receive, process_stats
from .cold_layouts import DirectoryBlocks, AuthDirectoryBlocks, CanonicalDirectoryBlocks
from .cold_segmented import BitOwners, PackedWavelet, XorDictionary
from .servers import private_row
from .use_case_layouts import PackedPresence, DirectoryPresence, PublicPresence


def fresh_process_stats():
    stats=process_stats()
    stats['inherited_rusage_peak_bytes']=stats['peak_rss_bytes']
    for line in Path('/proc/self/status').read_text().splitlines():
        if line.startswith('VmHWM:'):stats['peak_rss_bytes']=int(line.split()[1])*1024
    stats['rss_method']='Linux VmHWM for current process image'
    return stats


@dataclass
class Config:
    workload: str = ''
    hot_records: int = 0
    family: str = 'binary-tags'
    backend: str = 'dense'
    rows: int = 1024
    row_bytes: int = 32
    clients: int = 8
    queries_per_client: int = 1
    fanout: int = 2
    group: int = 4
    leaf_bits: int = 8
    partitions: int = 4
    seed: int = 1
    field_bits: int = 16
    block: int = 32
    distribution: str = 'clustered'
    key_layout: str = 'linear'
    canonical_file: str = ''
    verifier: str = ''
    finite_m: int = 0
    finite_d: int = 0


class ColdMemory(Memory):
    def read_xor(self,addresses):
        if self.backend!='dense':
            result=0
            for at in addresses:result^=int.from_bytes(self.read(at),'little')
            return result.to_bytes(self.width,'little')
        self.reads+=1;width=(self.n+7)//8;one=os.urandom(width);mask=int.from_bytes(one,'little')
        for at in addresses:mask^=1<<at
        replies=[e.call('dense',q) for e,q in zip(self.endpoints,(one,mask.to_bytes(width,'little')))]
        return (int.from_bytes(replies[0],'little')^int.from_bytes(replies[1],'little')).to_bytes(self.width,'little')
    def read_table(self,table,address):
        self.reads+=1
        return private_row(self.endpoints[2*table:2*table+2],address,self.table_dimensions[table][0])


class BinaryTags:
    """Fixed complete bucket pages; hash collisions checked by full 64-bit key.

    This synthetic tag projection is not a production tag-encryption adapter.
    All continuation pages are always privately fetched, including absent keys.
    """
    def __init__(self, data, page_slots=4):
        self.row_bytes=len(bytes.fromhex(data[0][3]))
        self.page_slots=page_slots
        self.bucket_count=1 << max(1, (len(data)//page_slots-1).bit_length())
        buckets=[[] for _ in range(self.bucket_count)]
        for row in data:buckets[self.bucket(row[0])].append(row)
        self.pages=max(1,max((len(b)+page_slots-1)//page_slots for b in buckets))
        self.width=page_slots*(17+self.row_bytes)
        self.rows=[]
        for bucket in buckets:
            for page in range(self.pages):
                records=bucket[page*page_slots:(page+1)*page_slots]
                encoded=b''.join(struct.pack('<BQQ',1,r[0],r[1])+bytes.fromhex(r[3]) for r in records)
                self.rows.append(encoded.ljust(self.width,b'\0'))
    def bucket(self,key):
        return int.from_bytes(hashlib.sha256(struct.pack('<Q',key)).digest()[:8],'little')%self.bucket_count
    def view(self):
        v=object.__new__(type(self));v.__dict__={k:x for k,x in vars(self).items() if k!='rows'};return v
    def query(self,memory,key,**unused):
        found=[];step=17+self.row_bytes
        for page in range(self.pages):
            data=memory.read(self.bucket(key)*self.pages+page)
            for start in range(0,len(data),step):
                valid,k,i=struct.unpack_from('<BQQ',data,start)
                if valid and k==key:found.append([k,i,i%4,data[start+17:start+step].hex()])
        return found


class JsonTags(BinaryTags):
    """Identical buckets, continuations, and predicates; JSON layout control."""
    def __init__(self,data,page_slots=4):
        super().__init__(data,page_slots)
        buckets=[[] for _ in range(self.bucket_count)]
        for row in data:buckets[self.bucket(row[0])].append(row)
        pages=[json.dumps(bucket[p*page_slots:(p+1)*page_slots],separators=(',',':')).encode()
               for bucket in buckets for p in range(self.pages)]
        self.width=max(map(len,pages));self.rows=[p.ljust(self.width,b' ') for p in pages]
    def query(self,memory,key,**unused):
        result=[]
        for p in range(self.pages):
            result.extend(r for r in json.loads(memory.read(self.bucket(key)*self.pages+p)) if r[0]==key)
        return result


class BinaryPatricia:
    """Compressed bit-group tree with a fixed, publicly padded query schedule."""
    def __init__(self,data,group=4):
        if group not in (1,2,4,8):raise ValueError('bit step')
        self.group=group;self.payload=len(bytes.fromhex(data[0][3]));self.stride=17+self.payload
        cells=[b'\0'];self.depth=0
        def build(records,shift,depth):
            self.depth=max(self.depth,depth)
            # Leaf bound is independent of query. Oversized same-key groups
            # remain complete and determine the public record padding width.
            if len(records)<=4 or len({r[0] for r in records})==1 or shift<0:
                blob=struct.pack('<BI',2,len(records))+b''.join(struct.pack('<BQQ',1,r[0],r[1])+bytes.fromhex(r[3]) for r in records)
            else:
                while shift>=0:
                    groups={}
                    for r in records:groups.setdefault((r[0]>>shift)&((1<<group)-1),[]).append(r)
                    if len(groups)>1:break
                    shift-=group
                if shift<0:raise AssertionError('distinct keys must branch')
                children=[0]*(1<<group)
                for digit,rs in groups.items():children[digit]=build(rs,shift-group,depth+1)
                blob=struct.pack('<BB',1,shift)+struct.pack('<'+'I'*len(children),*children)
            at=len(cells);cells.append(blob);return at
        self.root=build(data,((64-1)//group)*group,1)
        self.width=max(map(len,cells));self.rows=[b.ljust(self.width,b'\0') for b in cells]
    def view(self):
        v=object.__new__(BinaryPatricia);v.__dict__={k:x for k,x in vars(self).items() if k!='rows'};return v
    def query(self,memory,key,**unused):
        at=self.root;result=[]
        for _ in range(self.depth):
            blob=memory.read(at)
            if blob[0]==1:
                digit=(key>>blob[1])&((1<<self.group)-1)
                at=struct.unpack_from('<I',blob,2+4*digit)[0]
            elif blob[0]==2:
                count=struct.unpack_from('<I',blob,1)[0]
                for i in range(count):
                    start=5+i*self.stride;_,k,identifier=struct.unpack_from('<BQQ',blob,start)
                    if k==key:result.append([k,identifier,identifier%4,blob[start+17:start+self.stride].hex()])
                at=0
            else:at=0
        return result


class Remote:
    def __init__(self,sock):self.sock=sock;self.sent=self.received=self.calls=0;self.stage='setup'
    def call(self,command,value=None):
        self.calls+=1
        self.sent+=send(self.sock,dict(command=command,value=value,stage=self.stage))
        response,size=receive(self.sock);self.received+=size
        if 'error' in response:raise RuntimeError(response['error'])
        return response['value']


class DirectNative:
    """Fresh client owns duplicated pipe descriptors, never server process state."""
    def __init__(self,pair):
        self.writer=os.fdopen(pair[0].detach(),'w',buffering=1)
        self.reader=os.fdopen(pair[1].detach(),'r',buffering=1)
        self.sent=self.received=self.calls=0;self.stage='setup';self.phase_ids={'setup':[],'online':[]}
    def call(self,command,value=None):
        self.phase_ids[self.stage].append(self.calls);self.calls+=1
        line=json.dumps(dict(command=command,value=value),default=native_encode,separators=(',',':'))+'\n'
        self.sent+=len(line.encode());self.writer.write(line);self.writer.flush()
        line=self.reader.readline();self.received+=len(line.encode())
        if not line:raise EOFError('native replica disconnected')
        response=json.loads(line,object_hook=native_decode)
        return response['value']
    def close(self):self.writer.close();self.reader.close()


class FiniteMemory(Memory):
    def setup(self):
        self.parameters=self.endpoints[0].call('parameters')
        other=self.endpoints[1].call('parameters')
        if other!=self.parameters:raise ValueError('replica parameter mismatch')
        self.m=self.parameters['M'];self.d=self.parameters['D']
        if self.parameters['N']!=self.n or self.parameters['Record_len']!=self.width:raise ValueError('finite dimensions')
        self.cloud=sorted(sum(1<<i for i in choice) for degree in range(self.d//2+1)
                          for choice in itertools.combinations(range(self.m),degree))
        self.client_state_bytes=len(self.cloud)*36
    def read(self,address):
        if not 0<=address<self.n:raise ValueError('finite address')
        self.reads+=1;rank=address;remaining=self.d;state=0
        for pos in range(self.m):
            if remaining==0:break
            zeros=math.comb(self.m-pos-1,remaining) if remaining<=self.m-pos-1 else 0
            if rank>=zeros:rank-=zeros;remaining-=1;state|=1<<pos
        r=secrets.randbits(self.m)
        a=self.endpoints[0].call('finite',r);b=self.endpoints[1].call('finite',r^state)
        if len(a)!=len(self.cloud)*self.width or len(b)!=len(a):raise ValueError('finite response width')
        result=0
        for i,point in enumerate(self.cloud):
            if point&state==point:
                start=i*self.width;end=start+self.width
                result^=int.from_bytes(a[start:end],'little')^int.from_bytes(b[start:end],'little')
        return result.to_bytes(self.width,'little')


def client(descriptors,control,config,dimensions,view,queries):
    try:
        # Spawn preserves the descriptor's nonblocking flag, but not Python's
        # timeout attribute. Restore it before the first framed receive.
        control.settimeout(120)
        start=time.process_time_ns();wall=time.perf_counter_ns()
        memory=object.__new__(FiniteMemory if config.backend=='finite' else ColdMemory)
        memory.table_dimensions=dimensions
        memory.n,memory.width=dimensions[0] if dimensions else (0,0);memory.backend=config.backend
        memory.endpoints=[DirectNative(pair) for pair in descriptors];memory.reads=memory.writes=0
        memory.client_state_bytes=0
        metadata=json.dumps(dict(view=vars(view),tables=dimensions),sort_keys=True,separators=(',',':'))
        meta=Remote(control)
        if meta.call('metadata')!=metadata:raise ValueError('public metadata mismatch')
        if config.backend=='singlepass':memory.setup_hints(config.partitions)
        if config.backend=='finite':memory.setup()
        setup_cpu=(time.process_time_ns()-start)/1e6
        setup_wire=tuple(a+b for a,b in zip(memory.wire(),(meta.sent,meta.received)));samples=[]
        for e in memory.endpoints:e.stage='online'
        for key in queries:
            before=memory.wire();start=time.process_time_ns();qwall=time.perf_counter_ns();reads=memory.reads
            answer=view.query(memory,key,expected_root=getattr(view,'root_hash',None))
            verification_cpu=0
            if config.family=='canonical-directory':
                if len(answer)!=1:raise AssertionError('sentinel or predecessor witness required')
                before_child=resource.getrusage(resource.RUSAGE_CHILDREN)
                verified=subprocess.run([config.verifier,'verify',str(key),view.root_hash,answer[0][3]],capture_output=True,text=True,timeout=30)
                after_child=resource.getrusage(resource.RUSAGE_CHILDREN)
                verification_cpu=1000*((after_child.ru_utime+after_child.ru_stime)-(before_child.ru_utime+before_child.ru_stime))
                if verified.returncode or not json.loads(verified.stdout)['correct']:raise ValueError('canonical witness rejected')
            samples.append(dict(answer=answer,client_online_cpu_ms=(time.process_time_ns()-start)/1e6,
                canonical_verifier_cpu_ms=verification_cpu,
                online_wall_ms=(time.perf_counter_ns()-qwall)/1e6,
                wire=[a-b for a,b in zip(memory.wire(),before)],private_reads=memory.reads-reads))
        for sample in samples:sample['client_online_cpu_ms']+=sample['canonical_verifier_cpu_ms']
        client_stats=fresh_process_stats()
        if config.family=='canonical-directory':
            client_stats['canonical_child_peak_rss_bytes']=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss*1024
            client_stats['peak_rss_bytes']+=client_stats['canonical_child_peak_rss_bytes']
            client_stats['rss_method']+=' plus conservative child high-water mark'
        send(control,dict(samples=samples,client_setup_cpu_ms=setup_cpu,setup_wire=setup_wire,
            client_state_bytes=memory.client_state_bytes+len(metadata.encode()),
            client_lifecycle_wall_ms=(time.perf_counter_ns()-wall)/1e6,client_process=client_stats,
            endpoint_counters=[dict(sent=e.sent,received=e.received,calls=e.calls,phase_ids=e.phase_ids) for e in memory.endpoints]))
    except Exception:
        send(control,dict(error=traceback.format_exc()))
    finally:
        if 'memory' in locals():
            for e in memory.endpoints:e.close()
        control.close()


def run(c,binary):
    if c.backend not in ('dense','singlepass','finite'):raise ValueError('cold-safe backend required')
    if c.rows<4 or c.rows>262144 or c.row_bytes<8 or c.row_bytes>2008:raise ValueError('dimensions')
    if not 1<=c.clients<=4096 or not 1<=c.queries_per_client<=1024:raise ValueError('client dimensions')
    if not 1<=c.fanout<=1024 or c.rows%c.fanout:raise ValueError('fanout')
    start=time.process_time_ns()
    # Even keys leave holes for authenticated predecessor queries.
    scatter=c.family in ('binary-tags','json-tags','binary-patricia','directory','auth-directory','xor')
    keys=[((i*2654435761+17)%(1<<64) if scatter else i*2) for i in range(c.rows//c.fanout)]
    segmented=c.family in ('bit-owners','bit-owners-raw','prefix-owner','packed-wavelet')
    if segmented:
        if c.backend!='dense':raise ValueError('segmented pilot uses Dense pairs')
        if c.rows//c.fanout>=1<<c.field_bits:raise ValueError('field domain')
        keys=[(i*40503+17)%(1<<c.field_bits) for i in range(c.rows//c.fanout)]
    if c.key_layout=='hashed':
        if segmented and c.field_bits<64:raise ValueError('hashed-tag fixture requires 64 bits')
        keys=[int.from_bytes(hashlib.sha256(f'cold-tag:{c.seed}:{i}'.encode()).digest()[:8],'little') for i in range(c.rows//c.fanout)]
        if len(set(keys))!=len(keys):raise ValueError('fixture key collision')
    elif c.key_layout!='linear':raise ValueError('key layout')
    data=[[keys[i//c.fanout],i,i%4,hashlib.shake_256(f'{c.seed}:{i}'.encode()).digest(c.row_bytes).hex()] for i in range(c.rows)]
    if not 0<=c.hot_records<=c.rows:raise ValueError('hot records')
    for row in data[:c.hot_records]:row[0]=keys[0]
    if c.distribution=='shuffled':
        import random
        random.Random(c.seed).shuffle(data)
    elif c.distribution=='sorted':data.sort(key=lambda r:(r[0],r[1]))
    elif c.distribution!='clustered':raise ValueError('distribution')
    presence=c.family in ('packed-presence','directory-presence','public-presence')
    if presence:
        if c.backend!='dense':raise ValueError('presence comparison uses Dense or public download')
        index={'packed-presence':PackedPresence,'directory-presence':DirectoryPresence,'public-presence':PublicPresence}[c.family](data,c.group)
        rows=index.rows;view=index.view()
    elif c.family=='canonical-directory':
        corpus=json.loads(Path(c.canonical_file).read_text());data=corpus['data'];keys=[r[0] for r in data]
        index=CanonicalDirectoryBlocks(data,c.group);index.root_hash=corpus['root'];rows=index.rows;view=index.view()
    elif segmented:
        kwargs={'bits':c.field_bits,'block':c.block}
        is_bitmap=c.family.startswith('bit-owners') or c.family=='prefix-owner'
        if is_bitmap:kwargs['mode']='bits' if c.family.endswith('-raw') else 'compressed'
        if c.family=='prefix-owner':kwargs['prefix_only']=True
        index=(BitOwners if is_bitmap else PackedWavelet)(data,c.group,**kwargs)
        tables=index.tables;view=index.view();rows=tables[0]
    elif c.family in ('binary-tags','json-tags','binary-patricia','directory','auth-directory','xor'):
        index={'binary-tags':BinaryTags,'json-tags':JsonTags,'binary-patricia':BinaryPatricia,
               'directory':DirectoryBlocks,'auth-directory':AuthDirectoryBlocks,'xor':XorDictionary}[c.family](data,c.group);rows=index.rows;view=index.view()
    else:
        if c.family not in ('posting','hash','radix','authenticated'):raise ValueError('unsupported family')
        if c.family=='authenticated' and c.fanout!=1:raise ValueError('unique tree values required')
        bits=(max(keys)+2).bit_length()
        kwargs={'leaf_bits':c.leaf_bits} if c.family=='radix' else {}
        index=FAMILIES[c.family](data,bits,c.group,max(c.fanout,4),**kwargs)
        rows=index.table.rows;view=client_view(index)
    if not segmented:tables=[rows] if rows else []
    dimensions=[(len(t),len(t[0])) for t in tables]
    logical=sum(n*w for n,w in dimensions)
    metadata=json.dumps(dict(view=vars(view),tables=dimensions),sort_keys=True,separators=(',',':'))
    if logical>64<<20:raise ValueError('bounded real-run index exceeds 64 MiB')
    if c.backend=='finite' and c.finite_m and c.family=='directory':
        cloud=sum(math.comb(c.finite_m,d) for d in range(c.finite_d//2+1))
        if cloud*len(rows[0])*4>1<<20:raise ValueError('finite online-download preflight: two replies with native hex framing exceed 1 MiB')
    if c.backend=='singlepass' and logical*2>64<<20:raise ValueError('setup-download preflight: native hex framing alone exceeds 64 MiB')
    build_cpu=(time.process_time_ns()-start)/1e6
    endpoints=[];records=[];gateway_cpu=0
    # Trusted oracle only; avoid an O(N) test-controller scan for every warm query.
    exact={}
    for row in data:exact.setdefault(row[0],[]).append(row)
    present_buckets={row[0]%65536 for row in data} if presence else set()
    publish_start=time.process_time_ns()
    try:
        for table,table_rows in enumerate(tables):
            for role in range(2):
                e=NativeEndpoint(binary,f'cold-native-{table}-{role}');endpoints.append(e)
                if c.backend=='finite' and c.finite_m:e.call('configure',[c.finite_m,c.finite_d])
                e.call('publish',table_rows)
        publication_gateway_cpu=(time.process_time_ns()-publish_start)/1e6
        for number in range(c.clients):
            predicates=[]
            for q in range(c.queries_per_client):
                position=number*c.queries_per_client+q
                key=keys[(position*17)%len(keys)]
                # Include the hot group as well as absent and ordinary predicates.
                predicates.append(keys[0] if c.hot_records and position%4==1 else key if position%4 else key+1)
            parent,child=socket_pair()
            descriptors=[(DupFd(e.process.stdin.fileno()),DupFd(e.process.stdout.fileno())) for e in endpoints]
            starts=[e.calls for e in endpoints]
            p=mp.get_context('spawn').Process(target=client,args=(descriptors,child,replace(c,canonical_file=''),dimensions,view,predicates))
            before_wire=[sum(e.sent for e in endpoints),sum(e.received for e in endpoints)]
            phase_cpu={'setup':0.,'online':0.};relay_cpu={'setup':0.,'online':0.}
            phase_ids={'setup':[],'online':[]}
            wall=time.perf_counter_ns();p.start();child.close()
            deadline=time.monotonic()+120
            try:
                while True:
                    if time.monotonic()>deadline:raise TimeoutError('cold client deadline')
                    ready,_,_=select.select([parent],[],[],1)
                    if not ready and not p.is_alive():raise RuntimeError('client exited without result')
                    if parent in ready:
                        start=time.process_time_ns();result,_=receive(parent)
                        if result.get('command')=='metadata':
                            send(parent,dict(value=metadata));relay_cpu['setup']+=(time.process_time_ns()-start)/1e6
                        else:break
                p.join(10)
                if p.exitcode!=0:raise RuntimeError('client failed to exit')
                if 'error' in result:raise RuntimeError(result['error'])
                for role,(e,counters) in enumerate(zip(endpoints,result['endpoint_counters'])):
                    e.sent+=counters['sent'];e.received+=counters['received'];e.calls+=counters['calls']
                    for stage,ids in counters['phase_ids'].items():phase_ids[stage].extend([role,starts[role]+at] for at in ids)
                for key,sample in zip(predicates,result['samples']):
                    if presence:
                        expected=[[key,0,0,'01']] if key%65536 in present_buckets else []
                    elif c.family in ('authenticated','auth-directory','canonical-directory'):
                        eligible=[r for r in data if r[0]<=key];expected=[max(eligible,key=lambda r:r[0])] if eligible else []
                    else:expected=exact.get(key,[])
                    if sorted(sample.pop('answer'),key=lambda r:r[1])!=sorted(expected,key=lambda r:r[1]):raise AssertionError('complete predicate result mismatch')
                    sample.update(correct=True,matches=len(expected))
                result.update(client=number,pid=p.pid,server_phase_cpu_ms=phase_cpu,gateway_cpu_ms=relay_cpu,
                    server_phase_ids=phase_ids,
                    spawned_client_wall_ms=(time.perf_counter_ns()-wall)/1e6,
                    native_wire=[sum(getattr(e,name) for e in endpoints)-before_wire[i] for i,name in enumerate(('sent','received'))])
                result['budget_failures']=[]
                if result['client_process']['peak_rss_bytes']>128<<20:result['budget_failures'].append('client-rss')
                if result['client_state_bytes']>64<<20:result['budget_failures'].append('client-state')
                if result['setup_wire'][1]>64<<20:result['budget_failures'].append('setup-download')
                if result['client_setup_cpu_ms']>10000:result['budget_failures'].append('setup-cpu')
                if any(max(s['wire'])>1<<20 for s in result['samples']):result['budget_failures'].append('online-wire')
                if any(s['client_online_cpu_ms']>1000 for s in result['samples']):result['budget_failures'].append('online-cpu')
                records.append(result);gateway_cpu+=sum(relay_cpu.values())
            finally:
                if p.is_alive():p.terminate();p.join(10)
                parent.close()
        store_stats=[e.call('stats') for e in endpoints]
    finally:
        roles=[e.close() for e in endpoints]
    # Close reports include response serialization, unlike the early cpu_ms
    # value inside an individual response. Use those completed phase samples.
    for record in records:
        record['server_phase_cpu_ms']={stage:sum(roles[role]['phases'][at]['cpu_ms'] for role,at in ids)
                                      for stage,ids in record['server_phase_ids'].items()}
    count=c.clients*c.queries_per_client
    return dict(config=asdict(c),correct=True,clients=records,roles=roles,store_stats=store_stats,
        build_cpu_ms=build_cpu,publication_gateway_cpu_ms=publication_gateway_cpu,
        logical_index_bytes=logical,index_records=sum(n for n,w in dimensions),record_bytes=max((w for n,w in dimensions),default=0),table_dimensions=dimensions,
        all_server_process_cpu_ms=sum(r['process_cpu_ms'] for r in roles),
        server_and_gateway_lifecycle_cpu_per_answer_ms=(sum(r['process_cpu_ms'] for r in roles)+gateway_cpu+publication_gateway_cpu+build_cpu)/count,
        cold_service_cpu_per_answer_ms=sum(sum(r['server_phase_cpu_ms'].values())+sum(r['gateway_cpu_ms'].values()) for r in records)/count,
        qualification='synthetic complete value search; fresh clients connect directly to persistent native replicas; coordinator only publishes metadata and verifies returned results; SHA256 tree is not canonical Mizu witness',
        client_metadata_note='public navigation charged through metadata RPC; fresh client receives no source records or expected answers')


def main():
    parser=argparse.ArgumentParser();parser.add_argument('config',type=Path);parser.add_argument('output',type=Path);parser.add_argument('--native',required=True)
    args=parser.parse_args();args.output.mkdir(parents=True,exist_ok=False)
    try:result=run(Config(**json.loads(args.config.read_text())),args.native)
    except Exception:
        (args.output/'failure.txt').write_text(traceback.format_exc());raise
    (args.output/'result.json').write_text(json.dumps(result,indent=2))


if __name__=='__main__':main()
