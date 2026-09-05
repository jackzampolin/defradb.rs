"""Six complete index compositions over interchangeable private memory.

Public dimensions and fixed access schedules hide missing keys and output
cardinality. The honest client may see visited nodes/candidate metadata; these
are query-private retrieval experiments, not symmetric database privacy.
"""
import bisect
import hashlib
import json
import math
import struct
import copy


def client_view(index):
    """Navigation metadata only; never give query code the publisher's table."""
    view=copy.copy(index)
    view.table=None
    if isinstance(index,AuthOrdered):view.hashes={index.root:index.root_hash}
    for name in ('data','leaves','parent','payload'):
        if hasattr(view,name):setattr(view,name,{})
    return view


def encode(value):
    return json.dumps(value,separators=(',',':')).encode()


class Table:
    def __init__(self): self.items=[None]
    def add(self,item): self.items.append(item);return len(self.items)-1
    def finish(self,minimum_width=0):
        self.width=max(minimum_width,max(map(lambda x:len(encode(x)),self.items)))
        self.rows=[encode(x).ljust(self.width,b' ') for x in self.items]
        return self.rows
    def packed(self,item):
        blob=encode(item)
        if len(blob)>self.width:raise ValueError('updated node exceeds fixed record capacity')
        return blob.ljust(self.width,b' ')


def read(memory,address):return json.loads(memory.read(address))


class Radix:
    def __init__(self,data,bits,group=4,slots=4,leaf_bits=0):
        self.table=Table();self.bits=bits;self.group=group;self.slots=slots
        self.levels=math.ceil(bits/group);self.group=group
        self.branch_levels=max(0,self.levels-math.ceil(leaf_bits/group))
        def build(entries,level):
            if level==self.branch_levels:
                if any(sum(r[0]==key for r in entries)>slots for key in {r[0] for r in entries}):raise ValueError('radix leaf overflow')
                return self.table.add(['leaf',entries])
            buckets=[[] for _ in range(1<<group)]
            shift=(self.levels-1-level)*group
            for row in entries:buckets[(row[0]>>shift)&((1<<group)-1)].append(row)
            return self.table.add(['node',[build(e,level+1) if e else 0 for e in buckets]])
        self.root=build(data,0)
        self.table.finish()
    def query(self,memory,key,**_):
        address=self.root
        for level in range(self.branch_levels):
            node=read(memory,address)
            digit=(key>>((self.levels-1-level)*self.group))&((1<<self.group)-1)
            address=node[1][digit] if node else 0
        leaf=read(memory,address)
        return [] if leaf is None else [r for r in leaf[1] if r[0]==key]


class HashIndex:
    """Two-choice bounded buckets, with deterministic *public* build retries.

    Query always retrieves both buckets and a padded overflow bucket. Salt is
    public; query bucket addresses are never public. Overflow fails publication.
    """
    def __init__(self,data,bits,group=4,slots=4):
        self.table=Table();self.slots=slots;self.bucket_size=max(2,group)
        self.buckets=1<<(max(2,math.ceil(len(data)/self.bucket_size)*2)-1).bit_length()
        grouped={}
        for r in data:grouped.setdefault(r[0],[]).append(r)
        if any(len(v)>slots for v in grouped.values()):raise ValueError('hash posting overflow')
        for salt in range(128):
            self.salt=salt;cells=[[] for _ in range(self.buckets)];overflow=[]
            for key,rows in grouped.items():
                a,b=self.addresses(key)
                at=min((a,b),key=lambda i:len(cells[i]))
                if len(cells[at])<self.bucket_size:cells[at].append([key,rows])
                else:overflow.append([key,rows])
            if len(overflow)<=self.bucket_size:break
        else:raise ValueError('bounded hash publication overflow')
        self.base=len(self.table.items)
        for bucket in cells:self.table.add(bucket)
        self.overflow=self.table.add(overflow);self.table.finish()
    def addresses(self,key):
        h=hashlib.sha256(struct.pack('<QQ',self.salt,key)).digest()
        return int.from_bytes(h[:8],'little')%self.buckets,int.from_bytes(h[8:16],'little')%self.buckets
    def query(self,memory,key,**_):
        a,b=self.addresses(key);result={}
        for address in (self.base+a,self.base+b,self.overflow):
            for k,rows in read(memory,address):
                if k==key:
                    for row in rows:result[row[1]]=row
        return list(result.values())


class PostingIndex:
    """Public bounded integer dictionary; one complete padded inline page/key."""
    def __init__(self,data,bits,group=4,slots=4):
        self.table=Table();self.slots=slots;self.domain=1<<bits
        grouped={}
        for r in data:grouped.setdefault(r[0],[]).append(r)
        if any(len(v)>slots for v in grouped.values()):raise ValueError('posting overflow')
        for key in range(self.domain):self.table.add(grouped.get(key,[]))
        self.table.finish()
    def query(self,memory,key,**_):return read(memory,key+1) if 0<=key<self.domain else read(memory,0) or []


def compress_mask(mask,size):
    positions=[i for i in range(size) if mask>>i&1]
    runs=[]
    for p in positions:
        if runs and runs[-1][0]+runs[-1][1]==p:runs[-1][1]+=1
        else:runs.append([p,1])
    variants=[['bits',str(mask)],['array',positions],['runs',runs]]
    return min(variants,key=lambda x:len(encode(x)))


def decompress_mask(value):
    kind,data=value
    if kind=='bits':return int(data)
    if kind=='array':return sum(1<<i for i in data)
    if kind=='runs':return sum(((1<<length)-1)<<start for start,length in data)
    raise ValueError('unknown mask representation')


class BlockBitmap:
    """Private two-level block directory + compressed local bitplanes + MPC AND.

    Primary equality chooses row blocks; secondary equality filters within each
    block. Directory fanout is padded to the maximum over all primary keys.
    Payloads are privately fetched for every public output slot.
    """
    def __init__(self,data,bits,group=4,slots=4):
        self.table=Table();self.slots=slots;self.block=max(4,group);self.bits=bits;self.domain=1<<bits
        self.payload={r[1]:self.table.add(r) for r in data}
        self.payload_base=1
        if sorted(self.payload)!=list(range(len(data))):raise ValueError('contiguous row IDs required')
        directory={}
        for start in range(0,len(data),self.block):
            block=data[start:start+self.block]
            planes=[compress_mask(sum(((r[2]>>bit)&1)<<i for i,r in enumerate(block)),self.block) for bit in range(bits)]
            for key in sorted({r[0] for r in block}):
                mask=sum((r[0]==key)<<i for i,r in enumerate(block))
                at=self.table.add([start,compress_mask(mask,self.block),planes])
                directory.setdefault(key,[]).append(at)
        self.max_blocks=max(map(len,directory.values()),default=1)
        self.directory_base=len(self.table.items)
        for key in range(self.domain):self.table.add(directory.get(key,[]))
        self.table.finish()
    def query(self,memory,key,secondary=0,intersect=None,**_):
        directory=read(memory,self.directory_base+key) if 0<=key<self.domain else read(memory,0) or []
        matches=[]
        for address in directory+[0]*(self.max_blocks-len(directory)):
            block=read(memory,address)
            start,primary,planes=(0,0,[0]*self.bits) if block is None else (block[0],decompress_mask(block[1]),[decompress_mask(p) for p in block[2]])
            mask=(1<<self.block)-1
            for bit,plane in enumerate(planes):mask &= plane if secondary>>bit&1 else ((1<<self.block)-1)^plane
            result=intersect(primary,mask) if intersect else primary&mask
            matches.extend(start+i for i in range(self.block) if result>>i&1)
        if len(matches)>self.slots:raise ValueError('complete result overflow')
        result=[]
        for at in matches+[-1]*(self.slots-len(matches)):
            row=read(memory,self.payload_base+at if at>=0 else 0)
            if row is not None:result.append(row)
        return result


class Wavelet:
    """Wavelet matrix with privately fetched sampled rank blocks.

    Range count uses four rank accesses per bit. Reporting uses a separate
    sorted covering array and exactly `slots` further private reads.
    """
    def __init__(self,data,bits,group=4,slots=4):
        self.table=Table();self.bits=bits;self.slots=slots;self.n=len(data);self.block=max(4,group)
        values=[r[0] for r in data];self.levels=[]
        for bit in reversed(range(bits)):
            ones=[v>>bit&1 for v in values];base=len(self.table.items);count=0
            for start in range(0,self.n+1,self.block):
                chunk=ones[start:start+self.block]
                self.table.add([count,str(sum(v<<i for i,v in enumerate(chunk)))])
                count+=sum(chunk)
            zeros=self.n-sum(ones);self.levels.append((base,zeros))
            values=[v for v in values if not v>>bit&1]+[v for v in values if v>>bit&1]
        self.sorted_base=len(self.table.items)
        for r in sorted(data,key=lambda r:(r[0],r[1])):self.table.add(r)
        self.table.finish()
    def rank(self,memory,level,position):
        base,_=self.levels[level];before,mask=read(memory,base+position//self.block)
        return before+(int(mask)&((1<<(position%self.block))-1)).bit_count()
    def less(self,memory,key):
        # Out-of-domain thresholds execute the same schedule, then select a
        # boundary answer locally. No early return leaks the bound.
        clipped=min(max(key,0),(1<<self.bits)-1);left=0;right=self.n;count=0
        for level,(_,zeros) in enumerate(self.levels):
            a,b=self.rank(memory,level,left),self.rank(memory,level,right)
            if clipped>>(self.bits-1-level)&1:
                count+=(right-left)-(b-a);left,right=zeros+a,zeros+b
            else:left,right=left-a,right-b
        return 0 if key<=0 else self.n if key>=1<<self.bits else count
    def query(self,memory,key,high=None,count_only=False,**_):
        low=self.less(memory,key);end=self.less(memory,(key if high is None else high)+1)
        if count_only:return end-low
        if end-low>self.slots:raise ValueError('range output exceeds complete-result padding')
        result=[]
        for i in range(self.slots):
            row=read(memory,self.sorted_base+low+i if low+i<end else 0)
            if row is not None:result.append(row)
        return result


class AuthOrdered:
    """Merkle-authenticated ordered segment tree, trusted fresh root.

    Fixed key slots allow incremental value updates, deletion and reinsertion.
    Public writer addresses are allowed. It is NOT the production Poseidon
    witness format, nor an authenticated multiwriter service.
    """
    def __init__(self,data,bits,group=4,slots=4):
        if len({r[0] for r in data})!=len(data):raise ValueError('authenticated lane requires unique keys')
        self.table=Table();self.bits=bits;self.domain=1<<bits;self.data={r[0]:r for r in data}
        self.leaves={};self.parent={};self.hashes={}
        def build(lo,hi):
            if hi-lo==1:
                row=self.data.get(lo);node=['leaf',lo,row];at=self.table.add(node);self.leaves[lo]=at
            else:
                mid=(lo+hi)//2;a,b=build(lo,mid),build(mid,hi)
                at=self.table.add(['node',a,b,self.hashes[a],self.hashes[b],self.maximum(a),self.maximum(b)])
                self.parent[a]=self.parent[b]=at
            self.hashes[at]=self.digest(self.table.items[at]);return at
        self.root=build(0,self.domain)
        # Allow new values and maxima to use the full declared domain/width.
        self.table.finish(max(256,max((len(encode(r)) for r in data),default=0)+64))
    @staticmethod
    def digest(node):return hashlib.sha256(b'pir-auth-ordered-v1'+encode(node)).hexdigest()
    def maximum(self,at):
        node=self.table.items[at]
        return node[1] if node[0]=='leaf' and node[2] else -1 if node[0]=='leaf' else max(node[5],node[6])
    @property
    def root_hash(self):return self.hashes[self.root]
    def query(self,memory,key,expected_root=None,**_):
        if expected_root!=self.root_hash:raise ValueError('stale root rejected')
        at=self.root;expected=expected_root
        # Descend toward predecessor; parent maxima authenticate the choice.
        # If right max > bound it can still contain smaller keys, so use public
        # key intervals and remember the best fully eligible left subtree.
        lo,hi=0,self.domain;fallback=None
        for _ in range(self.bits):
            node=read(memory,at)
            if self.digest(node)!=expected:raise ValueError('node authentication failed')
            mid=(lo+hi)//2
            if key>=mid:
                if node[5]>=0:fallback=(node[5],)
                at,expected,lo=node[2],node[4],mid
            else:at,expected,hi=node[1],node[3],mid
        leaf=read(memory,at)
        if self.digest(leaf)!=expected:raise ValueError('leaf authentication failed')
        selected=leaf[1] if leaf[2] and leaf[1]<=key else (fallback[0] if fallback else -1)
        # A second full verified path resolves the predecessor candidate. This
        # also pads exact hits and lower-bound absence to the identical shape.
        at=self.root;expected=expected_root;lo=0;hi=self.domain
        for _ in range(self.bits):
            node=read(memory,at)
            if self.digest(node)!=expected:raise ValueError('node authentication failed')
            mid=(lo+hi)//2
            if selected>=mid:at,expected,lo=node[2],node[4],mid
            else:at,expected,hi=node[1],node[3],mid
        leaf=read(memory,at)
        if self.digest(leaf)!=expected:raise ValueError('leaf authentication failed')
        return [] if selected<0 else [leaf[2]]
    def update(self,memory,key,row):
        at=self.leaves[key];self.table.items[at]=['leaf',key,row]
        while True:
            node=self.table.items[at]
            if node[0]=='node':
                a,b=node[1:3];node[3:]=[self.hashes[a],self.hashes[b],self.maximum(a),self.maximum(b)]
            self.hashes[at]=self.digest(node);memory.write(at,self.table.packed(node))
            if at not in self.parent:break
            at=self.parent[at]


FAMILIES={'radix':Radix,'hash':HashIndex,'bitmap':BlockBitmap,'wavelet':Wavelet,'posting':PostingIndex,'authenticated':AuthOrdered}
