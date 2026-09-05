"""Private fixed-width memory backends. Research adapters, not serving code.

Dense: two noncolluding replicas. Path: one honest stateful client. Ramen:
three parties / one passive corruption, scalar-field artifact accesses. All
record limbs and automatic Ramen epoch work are executed, not modeled away.
"""
from array import array
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
from pathlib import Path
import secrets
import socket
import subprocess
import time

from .oram import PathOram
from .native_memory import NativeEndpoint
from .servers import Store, private_row
from .transport import Endpoint, totals

RAMEN_REVISION = "e39e55625fea803c8d369f31988e7cbe8d656c7a"
MODULUS = 340282366920938462946865773367900766209


class Memory:
    def __init__(self, rows, backend, binary=None, partitions=4):
        self.n, self.width = len(rows), len(rows[0])
        assert self.n and all(len(r)==self.width for r in rows)
        native=backend.endswith('-native');backend=backend.removesuffix('-native')
        self.backend, self.endpoints, self.samples = backend, [], []
        self.reads = self.writes = 0
        self.client_state_bytes = 0
        if backend == "ramen":
            self.ramen = Ramen(rows, binary)
            return
        self.endpoints = [(NativeEndpoint(Path(binary).with_name('native_store'),f'{backend}-native-{i}') if native else Endpoint(Store,role=f"{backend}-{i}")) for i in range(1 if backend=="path" else 2)]
        try:
            if backend == "path":
                self.oram = PathOram(self.endpoints[0],rows)
                self.client_state_bytes = self.oram.n*4+self.oram.stash_limit*(self.width+16)
            elif backend in ("dense", "singlepass"):
                for e in self.endpoints: e.call("publish",rows)
                if backend == "singlepass": self.setup_hints(partitions)
            else: raise ValueError(backend)
        except Exception:
            totals(self.endpoints)
            raise

    def setup_hints(self, partitions):
        # Same show-and-shuffle algorithm as src/single_pass.rs. Fetch the
        # generation over the metered transport; do not use the publisher copy.
        self.partitions = max(2,partitions)
        self.length = math.ceil(self.n/self.partitions)
        self.forward, self.inverse = [], []
        for _ in range(self.partitions):
            order=list(range(self.length));secrets.SystemRandom().shuffle(order)
            inverse=[0]*self.length
            for p,v in enumerate(order): inverse[v]=p
            self.forward.append(array('I',order));self.inverse.append(array('I',inverse))
        self.hints=[0]*self.length
        # Public sequential chunks bound transient client download buffers.
        # Each source row contributes once, to inverse[partition][local].
        for start in range(0,self.n,1024):
            rows=self.endpoints[0].call('read',list(range(start,min(start+1024,self.n))))
            for offset,row in enumerate(rows):
                p,local=divmod(start+offset,self.length)
                self.hints[self.inverse[p][local]]^=int.from_bytes(row,'little')
        self.client_state_bytes=8*self.length*self.partitions+self.length*(self.width+32)
        self.valid=True

    def read(self, address):
        if not 0<=address<self.n: raise ValueError("memory address")
        self.reads+=1
        if self.backend=="ramen": return self.ramen.access(address)
        if self.backend=="path": return self.oram.access(address)
        if self.backend=="dense": return private_row(self.endpoints,address,self.n)
        if not self.valid: raise ValueError("discarded SinglePass generation")
        p,local=divmod(address,self.length)
        h=self.inverse[p][local]
        shown=[self.forward[i][h] for i in range(self.partitions)]
        shown[p]=secrets.randbelow(self.length)
        positions=[secrets.randbelow(self.length) for _ in range(self.partitions)]
        refresh=[self.forward[i][positions[i]] for i in range(self.partitions)]
        try:
            replies=[]
            for e,indices in zip(self.endpoints,(refresh,shown)):
                # Pad out-of-domain partition tails with an actual zero record
                # via Store's partition command, preserving request shape.
                replies.append(e.call("partition-read",[self.length,indices]))
            result=self.hints[h]
            for i in range(self.partitions):
                if i==p: continue
                a,b=int.from_bytes(replies[0][i],"little"),int.from_bytes(replies[1][i],"little")
                result^=b
                self.hints[h]^=a^b;self.hints[positions[i]]^=a^b
                f,inv=self.forward[i],self.inverse[i]
                x,y=f[h],f[positions[i]]
                f[h],f[positions[i]]=y,x;inv[x],inv[y]=positions[i],h
            return result.to_bytes(self.width,"little")
        except Exception:
            self.valid=False
            raise

    def write(self,address,row):
        if len(row)!=self.width: raise ValueError("replacement width")
        self.writes+=1
        if self.backend=="ramen": self.ramen.access(address,row)
        elif self.backend=="path": self.oram.access(address,row)
        else:
            # Public writer/update addresses are an explicit separate channel.
            for e in self.endpoints: e.call("write",[(address,row)])
            if self.backend=="singlepass": self.valid=False

    def wire(self):
        if self.backend=="ramen":return self.ramen.sent,self.ramen.received
        return sum(e.sent for e in self.endpoints),sum(e.received for e in self.endpoints)

    def mark(self):
        if self.backend=='ramen':return {str(p.pid):len(phases) for p,phases in zip(self.ramen.processes,self.ramen.phases)}
        return {str(e.process.pid):e.calls for e in self.endpoints}

    def close(self):
        if self.backend=="ramen": return self.ramen.close()
        stats=[e.call("stats") for e in self.endpoints]
        result=totals(self.endpoints)
        result['storage']=stats
        result['client_state_bytes']=self.client_state_bytes
        if self.backend=="path":result['oram']=self.oram.stats()
        return result


class Ramen:
    def __init__(self,rows,binary):
        if not binary or not Path(binary).is_file(): raise ValueError("build the pinned Ramen bridge first")
        self.width=len(rows[0]);self.limbs=math.ceil(self.width/15)
        count=len(rows)*self.limbs
        self.capacity=4**max(2,math.ceil(math.log(count,4)))
        if self.capacity>65536:raise ValueError("Ramen scalar pilot exceeds 65536 field cells")
        self.processes=[];self.phases=[[] for _ in range(3)];self.sent=self.received=0
        sockets=[];ports=[]
        for _ in range(3):
            s=socket.socket();s.bind(('127.0.0.1',0));sockets.append(s);ports.append(s.getsockname()[1])
        for s in sockets:s.close()
        for i in range(3):
            self.processes.append(subprocess.Popen([str(binary),str(i),','.join(map(str,ports))],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1))
        values=[int.from_bytes(row[j:j+15],'little') for row in rows for j in range(0,self.width,15)]
        values += [0]*(self.capacity-len(values))
        shares=[[] for _ in range(3)]
        for value in values:
            for i,v in enumerate(self.share(value)):shares[i].append(str(v))
        self.exchange('init',shares)

    @staticmethod
    def share(value):
        a,b=secrets.randbelow(MODULUS),secrets.randbelow(MODULUS)
        return a,b,(value-a-b)%MODULUS

    def exchange(self,command,values):
        for p,value in zip(self.processes,values):
            line=json.dumps(dict(command=command,values=value),separators=(',',':'))+'\n'
            self.sent+=len(line.encode());p.stdin.write(line);p.stdin.flush()
        answers=[]
        for i,p in enumerate(self.processes):
            line=p.stdout.readline();self.received+=len(line.encode())
            if not line:raise RuntimeError(f"Ramen role {i} failed: {p.stderr.read()}")
            answer=json.loads(line);self.phases[i].append(dict(phase=command,**answer));answers.append(answer['value'])
        return answers

    def access(self,address,replacement=None):
        requests=[[] for _ in range(3)]
        for j in range(self.limbs):
            op=self.share(int(replacement is not None));at=self.share(address*self.limbs+j)
            value=self.share(0 if replacement is None else int.from_bytes(replacement[j*15:(j+1)*15],'little'))
            for i in range(3):requests[i].append([str(op[i]),str(at[i]),str(value[i])])
        answers=self.exchange('access',requests)
        result=b''.join((sum(int(a[j]) for a in answers)%MODULUS).to_bytes(15,'little') for j in range(self.limbs))
        return result[:self.width]

    def close(self):
        completed=self.exchange('close',[None]*3)
        for p in self.processes:
            p.wait(timeout=30)
            if p.returncode:raise RuntimeError(p.stderr.read())
            p.stdin.close();p.stdout.close();p.stderr.close()
        roles=[dict(role=f'ramen-{i}',pid=self.processes[i].pid,**phases[-1],phases=completed[i] if isinstance(completed[i],list) else phases[:-1]) for i,phases in enumerate(self.phases)]
        return dict(roles=roles,server_cpu_ms=sum(r['process_cpu_ms'] for r in roles),
            aggregate_peak_role_rss_bytes=sum(r['peak_rss_bytes'] for r in roles),
            inter_server_bytes=sum(r['peer_sent_bytes'] for r in roles),client_to_server_bytes=self.sent,
            server_to_client_bytes=self.received,field_cells_per_role=self.capacity,limbs_per_record=self.limbs,
            security='pinned Ramen; three processes; one passive corruption; client reconstructs nodes',client_state_bytes=0)
