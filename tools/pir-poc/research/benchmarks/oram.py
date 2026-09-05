"""Path ORAM, Z=5, encrypted fixed-size buckets and a bounded client map.

Construction: Stefanov et al., https://elaineshi.com/docs/pathoram.pdf, §3.
This nonrecursive variant is admitted only while its complete position map and
stash fit the declared client budget. Every access reads AND writes the full
path, uses fresh AES-GCM nonces and remaps the leaf. One serialized owner;
multi-client access requires that owner, whose CPU/state are never omitted.
"""
from array import array
import hashlib
import json
import os
from pathlib import Path
import secrets
import struct

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class PathOram:
    def __init__(self, endpoint, records, max_client_bytes=64<<20, stash_limit=256):
        self.endpoint = endpoint
        self.n = len(records)
        self.width = len(records[0])
        self.leaves = 1 << (self.n-1).bit_length()
        self.depth = self.leaves.bit_length()-1
        self.z = 5
        self.stash_limit = stash_limit
        if self.n*4+stash_limit*(self.width+16) > max_client_bytes:
            raise ValueError("Path ORAM position map/stash exceed client budget")
        self.positions = array("I",(secrets.randbelow(self.leaves) for _ in records))
        self.stash = {}
        self.key = AESGCM.generate_key(bit_length=256)
        self.aead = AESGCM(self.key)
        self.max_stash = 0
        self.accesses = 0
        self.epoch = 0
        self.valid = True
        self.max_client_bytes = max_client_bytes
        # Linear initial placement: each record goes deepest on its assigned
        # path. The upload is a complete fixed-size encrypted tree.
        buckets = [[] for _ in range(2*self.leaves-1)]
        for i,data in enumerate(records):
            for node in reversed(self.path(self.positions[i])):
                if len(buckets[node]) < self.z:
                    buckets[node].append((i,data))
                    break
            else:
                self.stash[i] = data
        self.check_stash()
        endpoint.call("publish",[self.encrypt(node,bucket) for node,bucket in enumerate(buckets)])

    def path(self, leaf):
        node = self.leaves-1+leaf
        path = [node]
        while node:
            node = (node-1)//2
            path.append(node)
        return list(reversed(path))

    def encrypt(self, node, records):
        records = records+[(2**64-1,bytes(self.width))]*(self.z-len(records))
        plain = b"".join(struct.pack("<Q",i)+row for i,row in records)
        nonce = os.urandom(12)
        return nonce+self.aead.encrypt(nonce,plain,struct.pack("<Q",node))

    def decrypt(self, node, ciphertext):
        plain = self.aead.decrypt(ciphertext[:12],ciphertext[12:],struct.pack("<Q",node))
        if len(plain) != self.z*(8+self.width):
            raise ValueError("ORAM bucket size")
        return [(struct.unpack_from("<Q",plain,i)[0],plain[i+8:i+8+self.width])
                for i in range(0,len(plain),8+self.width)]

    def check_stash(self):
        self.max_stash = max(self.max_stash,len(self.stash))
        if len(self.stash) > self.stash_limit:
            self.valid = False
            raise ValueError("Path ORAM stash overflow; result is not admitted")

    def access(self, address, replacement=None, interrupt=False):
        if not self.valid or not 0 <= address < self.n:
            raise ValueError("invalid ORAM state/address")
        leaf = self.positions[address]
        self.positions[address] = secrets.randbelow(self.leaves)
        path = self.path(leaf)
        try:
            encrypted = self.endpoint.call("read",path)
            for node,ciphertext in zip(path,encrypted):
                for i,row in self.decrypt(node,ciphertext):
                    if i != 2**64-1:
                        if not 0 <= i < self.n:
                            raise ValueError("invalid ORAM block id")
                        self.stash[i] = row
            answer = self.stash[address]
            if replacement is not None:
                if len(replacement) != self.width:
                    raise ValueError("replacement width")
                self.stash[address] = replacement
            writes = []
            for level in range(self.depth,-1,-1):
                node = path[level]
                selected = [i for i in self.stash if self.path(self.positions[i])[level] == node][:self.z]
                bucket = [(i,self.stash.pop(i)) for i in selected]
                writes.append((node,self.encrypt(node,bucket)))
            if interrupt:
                raise ConnectionError("injected failure before writeback; generation must be rebuilt")
            self.endpoint.call("write",writes)
            self.check_stash()
            self.accesses += 1
            self.epoch += 1
            return answer
        except Exception:
            # Never roll back a possibly observed leaf and reuse that state.
            self.valid = False
            raise

    def persist(self, path):
        if not self.valid:
            raise ValueError("cannot persist invalid ORAM state")
        plain = json.dumps(dict(positions=list(self.positions),stash={str(i):v.hex() for i,v in self.stash.items()},epoch=self.epoch)).encode()
        nonce = os.urandom(12)
        blob = nonce+self.aead.encrypt(nonce,plain,b"pir-path-oram-state-v1")
        with Path(path).open("xb") as stream:
            stream.write(blob)
            stream.flush()
            os.fsync(stream.fileno())
        return len(blob),hashlib.sha256(blob).hexdigest()

    def restore(self, path, expected_digest, expected_epoch):
        blob = Path(path).read_bytes()
        if hashlib.sha256(blob).hexdigest() != expected_digest:
            raise ValueError("state checkpoint digest mismatch")
        plain = self.aead.decrypt(blob[:12],blob[12:],b"pir-path-oram-state-v1")
        state = json.loads(plain)
        if state["epoch"] != expected_epoch or expected_epoch != self.epoch:
            raise ValueError("state checkpoint rollback rejected")
        self.positions = array("I",state["positions"])
        self.stash = {int(i):bytes.fromhex(v) for i,v in state["stash"].items()}
        self.check_stash()

    def stats(self):
        return dict(protocol="Path ORAM nonrecursive Z=5",accesses=self.accesses,
                    client_position_bytes=self.positions.itemsize*len(self.positions),
                    max_stash_records=self.max_stash,stash_bound=self.stash_limit,
                    padded_bucket_bytes=self.z*(self.width+8)+28,
                    buckets_per_access=self.depth+1,valid=self.valid,
                    security="computational; single honest serialized owner; semi-honest storage",
                    integrity="AES-GCM bucket authenticity; trusted checkpoint epoch required; no malicious-server freshness proof")
