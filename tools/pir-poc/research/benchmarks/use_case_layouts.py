"""Matched epoch-presence controls; collisions require separate private payload reads.

The complete public bitmap is query-independent and shared by every subscriber.
These fixtures measure one immutable epoch, not registration or notification timing.
"""
import base64
from .cold_layouts import DirectoryBlocks


class PackedPresence:
    def __init__(self,data,group=16):
        bitmap=bytearray(8192)
        for row in data:
            bucket=row[0]%65536
            bitmap[bucket//8]|=1<<(bucket%8)
        self.rows=[bytes([b]) for b in bitmap]
    def view(self):
        v=object.__new__(type(self));v.__dict__={k:x for k,x in vars(self).items() if k!='rows'}
        return v
    def query(self,memory,key,**unused):
        bucket=key%65536
        hit=memory.read(bucket//8)[0]>>(bucket%8)&1
        return [[key,0,0,'01']] if hit else []


class DirectoryPresence(DirectoryBlocks):
    def __init__(self,data,group=16):
        buckets=sorted({row[0]%65536 for row in data})
        super().__init__([[b,b,b%4,'01'] for b in buckets],group)
    def query(self,memory,key,**unused):
        bucket=key%65536
        hit=any(r[0]==bucket for r in self.unpack(memory.read(self.address(bucket))))
        return [[key,0,0,'01']] if hit else []


class PublicPresence(PackedPresence):
    def __init__(self,data,group=16):
        super().__init__(data,group)
        self.bitmap=base64.b64encode(b''.join(self.rows)).decode()
        self.rows=[]
    def query(self,memory,key,**unused):
        bucket=key%65536
        hit=base64.b64decode(self.bitmap)[bucket//8]>>(bucket%8)&1
        return [[key,0,0,'01']] if hit else []
