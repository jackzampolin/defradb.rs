"""Cold-search layouts with bounded public navigation and block proofs.

SHA-256 authentication is an explicit research proxy, not Mizu's canonical
Poseidon indexed tree. No symmetric database privacy is claimed for leaf blocks.
"""
import base64
import bisect
import hashlib
import math
import struct


class DirectoryBlocks:
    def __init__(self,data,group=16):
        self.group=group;self.payload=len(bytes.fromhex(data[0][3]));self.stride=17+self.payload
        # Keep all duplicates in the same group; no silent truncated postings.
        grouped={}
        for row in data:grouped.setdefault(row[0],[]).append(row)
        keys=sorted(grouped);groups=[keys[i:i+group] for i in range(0,len(keys),group)]
        self.anchor_blob=base64.b64encode(b''.join(struct.pack('<Q',g[0]) for g in groups)).decode()
        self.rows=[]
        for g in groups:
            records=[row for key in g for row in grouped[key]]
            self.rows.append(struct.pack('<I',len(records))+b''.join(struct.pack('<BQQ',1,r[0],r[1])+bytes.fromhex(r[3]) for r in records))
        self.width=max(map(len,self.rows));self.rows=[r.ljust(self.width,b'\0') for r in self.rows]
    def view(self):
        v=object.__new__(type(self));v.__dict__={k:x for k,x in vars(self).items() if k!='rows'};return v
    def address(self,key):
        b=base64.b64decode(self.anchor_blob);anchors=struct.unpack('<'+'Q'*(len(b)//8),b)
        return max(0,bisect.bisect_right(anchors,key)-1)
    def unpack(self,blob,offset=4):
        count=struct.unpack_from('<I',blob,offset-4)[0];out=[]
        if offset+count*self.stride>len(blob):raise ValueError('invalid record count')
        for i in range(count):
            at=offset+i*self.stride;_,key,identifier=struct.unpack_from('<BQQ',blob,at)
            out.append([key,identifier,identifier%4,blob[at+17:at+self.stride].hex()])
        return out
    def query(self,memory,key,**unused):
        return [r for r in self.unpack(memory.read(self.address(key))) if r[0]==key]


class AuthDirectoryBlocks(DirectoryBlocks):
    def __init__(self,data,group=16):
        if len({r[0] for r in data})!=len(data):raise ValueError('unique indexed-tree keys required')
        super().__init__(data,group)
        count=1<<(len(self.rows)-1).bit_length();self.levels=(count-1).bit_length()
        anchors=list(struct.unpack('<'+'Q'*(len(base64.b64decode(self.anchor_blob))//8),base64.b64decode(self.anchor_blob)))
        # Header authenticates the query interval, including the successor
        # boundary needed when the predecessor is the final row in this block.
        bases=[]
        for i,row in enumerate(self.rows):
            lower=0 if i==0 else anchors[i]
            upper=anchors[i+1] if i+1<len(anchors) else 0
            bases.append(struct.pack('<BQQ',int(upper==0),lower,upper)+row)
        self.base_width=17+self.width
        bases.extend([bytes(self.base_width)]*(count-len(bases)))
        levels=[[self.digest(b) for b in bases]]
        while len(levels[-1])>1:
            level=levels[-1];levels.append([self.pair(level[i],level[i+1]) for i in range(0,len(level),2)])
        self.root=levels[-1][0].hex();rows=[]
        for i,base in enumerate(bases):
            proof=b''.join(levels[level][(i>>level)^1] for level in range(self.levels))
            rows.append(base+proof)
        self.rows=rows;self.width=len(rows[0])
    @staticmethod
    def digest(blob):return hashlib.sha256(b'cold-block-leaf-v1'+blob).digest()
    @staticmethod
    def pair(a,b):return hashlib.sha256(b'cold-block-node-v1'+a+b).digest()
    def query(self,memory,key,expected_root=None,**unused):
        if expected_root!=self.root:raise ValueError('stale committed root')
        address=self.address(key);blob=memory.read(address)
        if len(blob)!=self.width:raise ValueError('proof record size')
        h=self.digest(blob[:self.base_width])
        for level in range(self.levels):
            sibling=blob[self.base_width+32*level:self.base_width+32*(level+1)]
            h=self.pair(sibling,h) if (address>>level)&1 else self.pair(h,sibling)
        if h.hex()!=expected_root:raise ValueError('invalid block proof')
        last,lower,upper=struct.unpack_from('<BQQ',blob)
        if not (lower<=key and (last or key<upper)):raise ValueError('incorrect predecessor interval')
        records=self.unpack(blob[:self.base_width],21)
        eligible=[r for r in records if r[0]<=key]
        return [max(eligible,key=lambda r:r[0])] if eligible else []
    @property
    def root_hash(self):return self.root


class CanonicalDirectoryBlocks(DirectoryBlocks):
    """Keep original canonical witness bytes and their original committed root."""
    def query(self,memory,key,**unused):
        rows=self.unpack(memory.read(self.address(key)))
        eligible=[r for r in rows if r[0]<=key]
        return [max(eligible,key=lambda r:r[0])] if eligible else []
