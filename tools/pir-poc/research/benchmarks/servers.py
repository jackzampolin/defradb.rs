"""Server roles for Dense, single-plane search and replicated Boolean MPC.

The MPC primitive is the semi-honest replicated multiplication of ABY3,
Mohassel/Rindal, https://eprint.iacr.org/2018/403, over GF(2). Party i
holds (x_i,x_(i+1)). Fresh pairwise PRG masks sum to zero. One corruption
is tolerated; the local benchmark controller is the trusted client.
"""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import os
import struct
import time

from .fields import Index
from .transport import receive_exact


class Store:
    def __init__(self):
        self.rows = []
        self.read_bytes = self.write_bytes = 0

    def handle(self, command, value):
        if command == "partition-read":
            length, indices = value
            if length < 1 or any(not 0 <= i < length for i in indices):
                raise ValueError("partition index")
            result = [self.rows[p*length+i] if p*length+i < len(self.rows) else bytes(len(self.rows[0])) for p,i in enumerate(indices)]
            self.read_bytes += sum(map(len,result))
            return result
        if command == "register":
            if len(value)!=(len(self.rows)+7)//8:raise ValueError("registered selector length")
            self.registered=value
            return True
        if command == "registered":
            return self.handle("dense",self.registered)
        if command == "publish":
            self.rows = value
            self.write_bytes += sum(map(len,value))
            return len(self.rows)
        if command == "read":
            result = [self.rows[i] for i in value]
            self.read_bytes += sum(map(len,result))
            return result
        if command == "write":
            for i,entry in value:
                self.rows[i] = entry
                self.write_bytes += len(entry)
            return True
        if command == "dense":
            if len(value) != (len(self.rows)+7)//8:
                raise ValueError("selector length")
            selector = int.from_bytes(value,"little") & ((1<<len(self.rows))-1)
            result = 0
            while selector:
                bit = selector & -selector
                row = self.rows[bit.bit_length()-1]
                result ^= int.from_bytes(row,"little")
                self.read_bytes += len(row)
                selector ^= bit
            return result.to_bytes(len(self.rows[0]),"little")
        if command == "stats":
            return dict(stored_bytes=sum(map(len,self.rows))+len(getattr(self,"registered",b"")),logical_read_bytes=self.read_bytes,
                        logical_write_bytes=self.write_bytes)
        raise ValueError(f"unknown store command {command}")


class FieldStore:
    """PIR over a compressed-at-rest group index; every selected bucket decoded.

    Random selector shares determine the scan independently of the predicate.
    Replies always contain the complete fixed-width N-bit bitmap.
    """
    def __init__(self):self.index=None;self.decoded_bytes=0

    def handle(self,command,value):
        if command=="publish":
            fields,bits,group,representation=value
            self.index=Index(fields,bits,group,representation)
            return True
        if command=="select":
            group,selector=value
            width=min(self.index.group,self.index.bits-group*self.index.group)
            if len(selector)!=(2**width+7)//8:raise ValueError("group selector width")
            result=0
            for bucket in range(1<<width):
                if selector[bucket//8]&(1<<(bucket%8)):
                    result^=self.index.selected(group,bucket)
                    self.decoded_bytes+=(self.index.n+7)//8
            return result.to_bytes((self.index.n+7)//8,"little")
        if command=="stats":return dict(stored_bytes=self.index.bytes,decoded_bitmap_bytes=self.decoded_bytes)
        raise ValueError(command)


class MpcRole:
    def __init__(self, role, incoming, outgoing, fabric_mbps=0):
        incoming.settimeout(120)
        outgoing.settimeout(120)
        self.role,self.incoming,self.outgoing = role,incoming,outgoing
        self.fabric_mbps=fabric_mbps
        self.seeds = None
        self.values = {}
        self.peer_sent_bytes = 0
        self.gate = 0
        self.generation = 0
        self.index = None

    def exchange(self, own, width):
        # All peers send concurrently; a sender thread prevents large-frame
        # cyclic socket-buffer deadlock. The CPU meter includes that thread.
        frame = struct.pack("!QQ",self.generation,self.gate)+own.to_bytes(width,"little")
        def transmit():
            start=time.perf_counter();self.outgoing.sendall(frame)
            if self.fabric_mbps:
                remaining=len(frame)*8/(self.fabric_mbps*1e6)-(time.perf_counter()-start)
                if remaining>0:time.sleep(remaining)
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(transmit)
            received = receive_exact(self.incoming,16+width)
            pending.result()
        if received[:16] != frame[:16]:
            raise ValueError("MPC transcript generation/gate mismatch")
        self.peer_sent_bytes += len(frame)
        return int.from_bytes(received[16:],"little")

    def handle(self, command, value):
        if command == "shared-input":
            destination, pair, clear = value
            if clear:
                self.values.clear()
            self.values[destination] = tuple(int.from_bytes(v,"little") for v in pair)
            return True
        if command == "seed":
            if self.seeds is not None:
                raise ValueError("pairwise seeds already established")
            own = os.urandom(32)
            other = self.exchange(int.from_bytes(own,"little"),32).to_bytes(32,"little")
            self.seeds = (own,other)
            return True
        if command == "publish":
            fields,bits = value
            self.index = Index(fields,bits,1,"planes")
            self.generation += 1
            self.values.clear()
            return self.index.bytes
        if command == "input":
            # Two shares of each secret query bit; public planes are added
            # only to component zero. All operators execute the same schedule.
            if isinstance(value,list):
                value = dict(pairs=value,base=0,clear=True)
            if value["clear"]:
                self.values.clear()
            self.gate += 1
            for bit,pair in enumerate(value["pairs"]):
                components = []
                for j,share in enumerate(pair):
                    component = self.index.mask if share else 0
                    if (self.role+j)%3 == 0:
                        component ^= self.index.mask ^ self.index.tables[bit][0]
                    components.append(component)
                self.values[value["base"]+bit] = tuple(components)
            return True
        if command == "xor":
            a,b,destination = value
            self.values[destination] = tuple(x^y for x,y in zip(self.values[a],self.values[b]))
            return True
        if command == "public":
            destination,blob = value
            number = int.from_bytes(blob,"little")
            self.values[destination] = tuple(number if (self.role+j)%3 == 0 else 0 for j in (0,1))
            return True
        if command == "project":
            source,destination,positions = value
            self.values[destination] = tuple(sum(((x>>p)&1)<<i for i,p in enumerate(positions)) for x in self.values[source])
            return True
        if command == "merge":
            left,right,destination,pairs = value
            self.values[destination] = tuple(sum((((a>>i)&1)<<p)|(((b>>i)&1)<<q) for i,(p,q) in enumerate(pairs)) for a,b in zip(self.values[left],self.values[right]))
            return True
        if command == "and":
            left,right,destination = value
            x0,x1 = self.values[left]
            y0,y1 = self.values[right]
            self.gate += 1
            domain = struct.pack("!QQ",self.generation,self.gate)
            width = (self.index.n+7)//8
            masks = [int.from_bytes(hashlib.shake_256(seed+domain).digest(width),"little") for seed in self.seeds]
            z = (x0&y0) ^ (x0&y1) ^ (x1&y0) ^ masks[0] ^ masks[1]
            other = self.exchange(z,width)
            self.values[destination] = (z,other)
            return True
        if command == "output":
            return self.values[value][0].to_bytes((self.index.n+7)//8,"little")
        if command == "output-prefix":
            node,count = value
            return (self.values[node][0]&((1<<count)-1)).to_bytes((count+7)//8,"little")
        if command == "stats":
            return dict(index_bytes=self.index.bytes,peer_sent_bytes=self.peer_sent_bytes,gates=self.gate,
                        owned_share_bytes=sum((a.bit_length()+b.bit_length()+15)//8 for a,b in self.values.values()))
        raise ValueError(f"unknown MPC command {command}")


class IndexStore:
    def __init__(self):
        self.index = None

    def handle(self,command,value):
        if command == "publish":
            self.rows,self.fields,self.secondary,self.active,config = value
            self.index = Index(self.fields,config["bits"],config["group"],config["format"])
            self.other = Index(self.secondary,config["bits"],config["group"],config["format"]) if config["predicate"]=="conjunction" else None
            self.live = sum(bool(v)<<i for i,v in enumerate(self.active))
            self.slots = config["slots"]
            return self.index.bytes+(self.other.bytes if self.other else 0)
        if command == "query":
            from .fields import ids
            predicate,low,high,other = value
            result = self.index.bounded_range(low,high) if predicate == "range" else self.index.equality(low)
            if predicate == "conjunction":
                result &= self.other.equality(other)
            selected = list(ids(result & self.live))
            if len(selected)>self.slots:
                raise ValueError("complete result exceeds public padding budget")
            return [(i,self.rows[i]) for i in selected]+[(-1,bytes(len(self.rows[0])))]*(self.slots-len(selected))
        if command == "stats":
            return dict(index_bytes=self.index.bytes+(self.other.bytes if self.other else 0),source_bytes=sum(map(len,self.rows)),
                        representation=self.index.mode,private=False)
        raise ValueError("unknown index command")


def private_row(endpoints, index, rows):
    width = (rows+7)//8
    one = os.urandom(width)
    two = (int.from_bytes(one,"little") ^ (1<<index)).to_bytes(width,"little")
    answers = [e.call("dense",q) for e,q in zip(endpoints,(one,two))]
    return (int.from_bytes(answers[0],"little") ^ int.from_bytes(answers[1],"little")).to_bytes(len(answers[0]),"little")
