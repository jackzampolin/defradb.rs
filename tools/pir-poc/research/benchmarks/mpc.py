"""Three-party bit-sliced search and fixed Batcher compaction circuit."""
import os
import secrets

from .servers import MpcRole
from .transport import Endpoint, parallel_calls, socket_pair, totals


class Search:
    def __init__(self, fields, bits, fabric_mbps=0):
        self.n,self.bits = len(fields),bits
        self.width = (self.n+7)//8
        self.next = 0
        edges = [socket_pair() for _ in range(3)]
        self.endpoints = []
        try:
            for i in range(3):
                # Edge i carries z_i to party i-1. Each party holds i,i+1.
                self.endpoints.append(Endpoint(MpcRole,
                    (i,edges[(i+1)%3][1],edges[i][0],fabric_mbps),f"mpc-{i}"))
        finally:
            for pair in edges:
                for sock in pair:
                    sock.close()
        try:
            parallel_calls(self.endpoints,"seed",[None]*3)
            parallel_calls(self.endpoints,"publish",[[fields,bits]]*3)
        except Exception:
            totals(self.endpoints)
            raise

    def publish(self, fields):
        parallel_calls(self.endpoints,"publish",[[fields,self.bits]]*3)

    def command(self, command, value):
        return parallel_calls(self.endpoints,command,[value]*3)

    def new(self):
        node = self.next
        self.next += 1
        return node

    def binary(self, op, a, b):
        output = self.new()
        self.command(op,[a,b,output])
        return output

    def public(self, value):
        output = self.new()
        self.command("public",[output,value.to_bytes(self.width,"little")])
        return output

    def equality(self, value, clear=True):
        if clear:
            self.next = 0
        start = self.next
        self.next += self.bits
        shares = []
        for bit in range(self.bits):
            a,b = secrets.randbits(1),secrets.randbits(1)
            shares.append((a,b,a^b^((value>>bit)&1)))
        inputs = [dict(base=start,clear=clear,pairs=[[s[i],s[(i+1)%3]] for s in shares]) for i in range(3)]
        parallel_calls(self.endpoints,"input",inputs)
        layer = list(range(start,start+self.bits))
        while len(layer)>1:
            layer = [self.binary("and",layer[i],layer[i+1]) if i+1<len(layer) else layer[i] for i in range(0,len(layer),2)]
        return layer[0]

    def query(self, low, high=None):
        high = low if high is None else high
        if high < low or high-low>7:
            raise ValueError("private range circuit supports at most eight public slots")
        return self.query_values(list(range(low,high+1)))

    def query_values(self, values):
        if not 1 <= len(values) <= 8:
            raise ValueError("private union has a public bound of eight equality inputs")
        result = self.equality(values[0])
        for value in values[1:]:
            other = self.equality(value,clear=False)
            both = self.binary("and",result,other)
            result = self.binary("xor",self.binary("xor",result,other),both)
        return result

    def reconstruct(self, node, count=None):
        replies = self.command("output",node) if count is None else self.command("output-prefix",[node,count])
        value = 0
        for reply in replies:
            value ^= int.from_bytes(reply,"little")
        return value & ((1<<(self.n if count is None else count))-1)

    def project(self, node, positions):
        result = self.new()
        self.command("project",[node,result,positions])
        return result

    def compact(self, flag, slots):
        if self.n & (self.n-1) or self.n>256 or not 0<slots<=self.n:
            raise ValueError("oblivious compaction prototype requires <=256 power-of-two rows")
        wires = [flag]+[self.public(sum(((i>>b)&1)<<i for i in range(self.n))) for b in range((self.n-1).bit_length())]
        k = 2
        while k<=self.n:
            gap = k//2
            while gap:
                pairs = [(i,i^gap) for i in range(self.n) if (i^gap)>i]
                left = [self.project(w,[a for a,b in pairs]) for w in wires]
                right = [self.project(w,[b for a,b in pairs]) for w in wires]
                ascending = sum(bool(a&k)<<j for j,(a,b) in enumerate(pairs))
                asc = self.public(ascending)
                desc = self.public(((1<<len(pairs))-1)^ascending)
                swap = self.binary("and",self.binary("xor",left[0],desc),self.binary("xor",right[0],asc))
                merged = []
                for a,b in zip(left,right):
                    delta = self.binary("and",swap,self.binary("xor",a,b))
                    a,b = self.binary("xor",a,delta),self.binary("xor",b,delta)
                    destination = self.new()
                    self.command("merge",[a,b,destination,pairs])
                    merged.append(destination)
                wires = merged
                gap //= 2
            k *= 2
        valid = self.reconstruct(wires[0],slots)
        columns = [self.reconstruct(w,slots) for w in wires[1:]]
        return [sum(((col>>slot)&1)<<b for b,col in enumerate(columns)) for slot in range(slots) if (valid>>slot)&1]
