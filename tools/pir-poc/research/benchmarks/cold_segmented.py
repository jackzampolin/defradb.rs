"""Private per-owner tables. Owners are logical colocated processes, not a cluster.

Two independent Dense replicas per table conceal each selector. No database
confidentiality is promised: clients may learn extra block contents. Every query
uses the same public owner schedule and padded payload-read bound.
"""
import hashlib
import math
import struct
from collections import Counter


def pack_rows(data):
    return struct.pack('<I',len(data))+b''.join(struct.pack('<QQ',r[0],r[1])+bytes.fromhex(r[3]) for r in data)


def unpack_rows(blob,payload):
    count=struct.unpack_from('<I',blob)[0];step=16+payload
    if 4+count*step>len(blob):raise ValueError('record overflow')
    result=[]
    for i in range(count):
        at=4+i*step;k,identifier=struct.unpack_from('<QQ',blob,at)
        result.append([k,identifier,identifier%4,blob[at+16:at+step].hex()])
    return result


def padded(rows):
    width=max(map(len,rows));return [r.ljust(width,b'\0') for r in rows]


def mask_encode(mask,n,mode):
    positions=[i for i in range(n) if mask>>i&1]
    bits=b'B'+mask.to_bytes((n+7)//8,'little')
    arrays=b'A'+struct.pack('<I',len(positions))+b''.join(struct.pack('<I',i) for i in positions)
    runs=[]
    for i in positions:
        if runs and runs[-1][0]+runs[-1][1]==i:runs[-1][1]+=1
        else:runs.append([i,1])
    run=b'R'+struct.pack('<I',len(runs))+b''.join(struct.pack('<II',*r) for r in runs)
    return bits if mode=='bits' else min((bits,arrays,run),key=len)


def mask_decode(blob,n):
    if blob[:1]==b'B':return int.from_bytes(blob[1:1+(n+7)//8],'little')
    count=struct.unpack_from('<I',blob,1)[0];mask=0
    for j in range(count):
        if blob[:1]==b'A':mask|=1<<struct.unpack_from('<I',blob,5+4*j)[0]
        elif blob[:1]==b'R':
            at,length=struct.unpack_from('<II',blob,5+8*j);mask|=((1<<length)-1)<<at
        else:raise ValueError('container')
    return mask


class BitOwners:
    def __init__(self,data,group=2,bits=16,block=32,mode='compressed',prefix_only=False):
        if not prefix_only and bits%group:raise ValueError('whole bit groups required')
        if not 1<=group<=16:raise ValueError('bounded group width')
        self.bits=bits;self.group=group;self.payload=len(bytes.fromhex(data[0][3]))
        self.shifts=[bits-group] if prefix_only else list(range(0,bits,group))
        blocks=[data[i:i+block] for i in range(0,len(data),block)]
        self.blocks=len(blocks);self.tables=[];bounds=[]
        for shift in self.shifts:
            masks=[0]*(1<<group)
            for i,rows in enumerate(blocks):
                for r in rows:masks[(r[0]>>shift)&((1<<group)-1)]|=1<<i
            bounds.append(max(m.bit_count() for m in masks))
            self.tables.append(padded([mask_encode(m,self.blocks,mode) for m in masks]))
        # Intersection cannot exceed any operand, including absent predicates.
        self.candidate_bound=min(bounds);self.payload_table=len(self.tables)
        self.tables.append(padded([pack_rows([]),*[pack_rows(b) for b in blocks]]))
    def view(self):
        v=object.__new__(type(self));v.__dict__={k:x for k,x in vars(self).items() if k!='tables'};return v
    def query(self,memory,key,**unused):
        mask=(1<<self.blocks)-1
        for owner,shift in enumerate(self.shifts):
            mask&=mask_decode(memory.read_table(owner,(key>>shift)&((1<<self.group)-1)),self.blocks)
        candidates=[i+1 for i in range(self.blocks) if mask>>i&1]
        if len(candidates)>self.candidate_bound:raise AssertionError('unsafe padding bound')
        found=[]
        for at in candidates+[0]*(self.candidate_bound-len(candidates)):
            found.extend(r for r in unpack_rows(memory.read_table(self.payload_table,at),self.payload) if r[0]==key)
        return found


class PackedWavelet:
    def __init__(self,data,group=2,bits=16,block=32):
        if bits%group:raise ValueError('whole bit groups required')
        self.bits=bits;self.group=group;self.block=block;self.arity=1<<group
        self.n=len(data);self.payload=len(bytes.fromhex(data[0][3]));self.tables=[];self.offsets=[]
        records=list(data)
        for shift in reversed(range(0,bits,group)):
            digits=[(r[0]>>shift)&(self.arity-1) for r in records]
            totals=[digits.count(d) for d in range(self.arity)]
            offsets=[sum(totals[:d]) for d in range(self.arity)];self.offsets.append(offsets)
            counts=[0]*self.arity;rows=[]
            for start in range(0,self.n+1,block):
                chunk=digits[start:start+block]
                rows.append(struct.pack('<'+'I'*self.arity,*counts)+bytes(chunk).ljust(block,b'\0'))
                for d in chunk:counts[d]+=1
            self.tables.append(rows)
            records=[r for d in range(self.arity) for r in records if (r[0]>>shift)&(self.arity-1)==d]
        # Wavelet matrix final order is radix-reversed; the final interval is
        # therefore resolved against this actual permutation, not sorted keys.
        self.output_bound=max(Counter(r[0] for r in data).values())
        self.payload_table=len(self.tables)
        self.tables.append(padded([pack_rows([]),*[pack_rows([r]) for r in records]]))
    def view(self):
        v=object.__new__(type(self));v.__dict__={k:x for k,x in vars(self).items() if k!='tables'};return v
    def rank(self,memory,level,position,digit):
        blob=memory.read_table(level,position//self.block)
        before=struct.unpack_from('<I',blob,4*digit)[0]
        return before+blob[4*self.arity:4*self.arity+position%self.block].count(digit)
    def query(self,memory,key,**unused):
        left,right=0,self.n
        for level,shift in enumerate(reversed(range(0,self.bits,self.group))):
            digit=(key>>shift)&(self.arity-1);offset=self.offsets[level][digit]
            left,right=offset+self.rank(memory,level,left,digit),offset+self.rank(memory,level,right,digit)
        if right-left>self.output_bound:raise AssertionError('report overflow')
        result=[]
        for i in range(self.output_bound):
            at=left+i+1 if left+i<right and key<1<<self.bits else 0
            result.extend(unpack_rows(memory.read_table(self.payload_table,at),self.payload))
        return result


class XorDictionary:
    """Peelable XOR retrieval, with a 256-bit record digest for absent queries.

    This is probabilistic absence detection, not an authenticated nonmembership
    proof. Retry seeds are public. Three addresses can form one Dense selector.
    """
    def __init__(self,data,group=16):
        self.payload=len(bytes.fromhex(data[0][3]));grouped={}
        for row in data:grouped.setdefault(row[0],[]).append(row)
        self.part=max(2,math.ceil(len(grouped)*1.4/3));self.seed=0
        raw={k:pack_rows(rs) for k,rs in grouped.items()};width=max(map(len,raw.values()))
        values={k:v.ljust(width,b'\0') for k,v in raw.items()}
        values={k:v+hashlib.sha256(b'cold-xor-v1'+v).digest() for k,v in values.items()}
        for seed in range(100):
            self.seed=seed;edges={k:self.locations(k) for k in values};nodes=[set() for _ in range(3*self.part)]
            for k,locations in edges.items():
                for i in locations:nodes[i].add(k)
            todo=[i for i,node in enumerate(nodes) if len(node)==1];peeled=[]
            while todo:
                i=todo.pop()
                if len(nodes[i])!=1:continue
                k=next(iter(nodes[i]));peeled.append((i,k))
                for at in edges[k]:
                    nodes[at].remove(k)
                    if len(nodes[at])==1:todo.append(at)
            if len(peeled)==len(values):break
        else:raise ValueError('peeling failed after bounded retries')
        table=[0]*(3*self.part);self.width=width+32
        for at,k in reversed(peeled):
            value=int.from_bytes(values[k],'little')
            for other in edges[k]:value^=table[other]
            table[at]=value
        self.rows=[v.to_bytes(self.width,'little') for v in table]
    def locations(self,key):
        h=hashlib.shake_256(struct.pack('<QQ',self.seed,key)).digest(24)
        return [i*self.part+int.from_bytes(h[8*i:8*i+8],'little')%self.part for i in range(3)]
    def view(self):
        v=object.__new__(type(self));v.__dict__={k:x for k,x in vars(self).items() if k!='rows'};return v
    def query(self,memory,key,**unused):
        blob=memory.read_xor(self.locations(key))
        if hashlib.sha256(b'cold-xor-v1'+blob[:-32]).digest()!=blob[-32:]:return []
        return [r for r in unpack_rows(blob[:-32],self.payload) if r[0]==key]
