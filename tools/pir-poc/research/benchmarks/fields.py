"""Identical field corpus and public lower-bound index representations."""
from array import array
import hashlib
import random
import struct


def corpus(rows, width, fanout=4, distribution="uniform", order="random", seed=1):
    if rows < 1 or width < 8 or fanout < 1:
        raise ValueError("invalid corpus dimensions")
    rng = random.Random(seed)
    values = [i//fanout for i in range(rows)]
    if distribution == "skewed":
        # Half the corpus is hot; public result budget must include this maximum.
        values = [0 if i < rows//2 else 1+(i-rows//2)//fanout for i in range(rows)]
    elif distribution not in ("uniform","clustered"):
        raise ValueError("unknown distribution")
    permutation = list(range(rows))
    if order == "random" and distribution != "clustered":
        rng.shuffle(permutation)
    elif order not in ("random","sorted"):
        raise ValueError("unknown row ordering")
    records = [i.to_bytes(8,"little")+hashlib.shake_256(struct.pack("<QQ",seed,i)).digest(width-8) for i in permutation]
    field = [values[i] for i in permutation]
    secondary = [(i//max(1,fanout//2)) % 7 for i in permutation]
    return records,field,secondary,permutation


class Index:
    def __init__(self, values, bits, group, representation):
        if not 1 <= bits <= 129 or group not in (1,2,4,8) or bits%group or max(values,default=0) >= 1<<bits:
            raise ValueError("field index dimensions")
        self.n,self.bits,self.group = len(values),bits,group
        self.mode = representation
        self.mask = (1<<self.n)-1
        self.tables = []
        self.bytes = 0
        for offset in range(0,bits,group):
            buckets = [[] for _ in range(1<<group)]
            for i,v in enumerate(values):
                buckets[(v>>offset)&((1<<group)-1)].append(i)
            if representation == "planes":
                if group != 1:
                    raise ValueError("single-plane representation requires g=1")
                table = [sum(1<<i for i in buckets[1])]
                self.bytes += (self.n+7)//8
            elif representation == "bitmap":
                table = [sum(1<<i for i in bucket) for bucket in buckets]
                self.bytes += len(table)*((self.n+7)//8)
            elif representation == "postings":
                table = [array("I",bucket) for bucket in buckets]
                self.bytes += sum(len(a)*a.itemsize for a in table)+8*(len(table)+1)
            elif representation == "runs":
                table = []
                for bucket in buckets:
                    runs = array("I")
                    for i in bucket:
                        if runs and runs[-2]+runs[-1] == i:
                            runs[-1] += 1
                        else:
                            runs.extend([i,1])
                    table.append(runs)
                self.bytes += sum(len(a)*a.itemsize for a in table)+8*(len(table)+1)
            else:
                raise ValueError("unknown index representation")
            self.tables.append(table)

    def selected(self, group, value):
        table = self.tables[group]
        if self.mode == "planes":
            return table[0] if value else self.mask ^ table[0]
        selected = table[value]
        if self.mode == "bitmap":
            return selected
        if self.mode == "postings":
            return sum(1<<int(i) for i in selected)
        result = 0
        for i in range(0,len(selected),2):
            start,length = selected[i:i+2]
            result |= ((1<<length)-1)<<start
        return result

    def equality(self, value):
        result = self.mask
        for group in range(len(self.tables)):
            result &= self.selected(group,(value>>(group*self.group))&((1<<self.group)-1))
        return result

    def bounded_range(self, low, high):
        if low > high or high-low > 1024:
            raise ValueError("range outside public bound")
        result = 0
        for value in range(low,high+1):
            result |= self.equality(value)
        return result


def ids(bitmap):
    while bitmap:
        bit = bitmap & -bitmap
        yield bit.bit_length()-1
        bitmap ^= bit
