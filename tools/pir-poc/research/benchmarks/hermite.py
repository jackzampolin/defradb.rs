"""Executable m=1 specialization of the scalable multivariate PIR construction.

Ghoshal et al., https://eprint.iacr.org/2024/765. The database is interpolated
at public distinct points. Each server stores all Hasse derivatives of order
< t over F_q; S*t > degree. Client queries u+lambda*v and reconstructs at zero
using Hermite interpolation. This concrete prototype makes no asymptotic claim.
Bytes are encoded as two base-16 symbols, so arbitrary payloads are preserved.
"""
import math
import secrets

import numpy as np


def prime_at_least(n):
    while any(n%d == 0 for d in range(2,math.isqrt(n)+1)):
        n += 1
    return n


def inverse(matrix, q):
    n = len(matrix)
    a = np.concatenate((np.array(matrix,dtype=np.int64)%q,np.eye(n,dtype=np.int64)),axis=1)
    for col in range(n):
        pivot = next((row for row in range(col,n) if a[row,col]),None)
        if pivot is None:
            raise ValueError("singular finite-field interpolation matrix")
        a[[col,pivot]] = a[[pivot,col]]
        a[col] = a[col]*pow(int(a[col,col]),-1,q)%q
        for row in range(n):
            if row != col:
                a[row] = (a[row]-a[row,col]*a[col])%q
    return a[:,n:]


def dimensions(n, width, servers):
    if n < 1 or n > 128 or servers not in (4,8,16,32,64,128):
        raise ValueError("Hermite prototype supports 1..128 rows and 4..128 servers")
    q = prime_at_least(max(17,n,servers+1))
    t = (n+servers-1)//servers
    item = 1 if q <= 256 else 2
    return dict(q=q,t=t,m=1,d=n-1,servers=servers,n=n,width=width,symbol_bytes=item,
                storage_per_server=q*t*2*width*item,download_bytes=servers*t*2*width*item,
                storage_amplification=servers*q*t*2*item/n,
                collusion_tolerance=1,encoding="two base-16 symbols per byte")


def encode(records, servers):
    p = dimensions(len(records),len(records[0]),servers)
    q,t,n = p["q"],p["t"],len(records)
    data = np.array([[component for byte in row for component in (byte&15,byte>>4)] for row in records],dtype=np.int64)
    coefficients = inverse([[pow(x,k,q) for k in range(n)] for x in range(n)],q).dot(data)%q
    dtype = np.uint8 if p["symbol_bytes"] == 1 else np.dtype("<u2")
    table = []
    for x in range(q):
        derivatives = []
        for j in range(t):
            basis = np.array([0 if k < j else math.comb(k,j)*pow(x,k-j,q)%q for k in range(n)],dtype=np.int64)
            derivatives.append(basis.dot(coefficients)%q)
        table.append(np.array(derivatives,dtype=dtype).tobytes())
    return p,table


class Client:
    def __init__(self, parameters):
        self.p = parameters
        q,n,t = (parameters[k] for k in ("q","n","t"))
        self.rows = [(s,j) for s in range(1,parameters["servers"]+1) for j in range(t)][:n]
        matrix = [[0 if k<j else math.comb(k,j)*pow(s,k-j,q)%q for k in range(n)] for s,j in self.rows]
        # Only coefficient zero is required to recover F(u).
        self.weights = inverse(matrix,q)[0]

    def query(self, target):
        if not 0 <= target < self.p["n"]:
            raise ValueError("target outside polynomial database")
        v = secrets.randbelow(self.p["q"])
        return v,[(target+s*v)%self.p["q"] for s in range(1,self.p["servers"]+1)]

    def recover(self, v, replies):
        q,t = self.p["q"],self.p["t"]
        dtype = np.uint8 if self.p["symbol_bytes"] == 1 else np.dtype("<u2")
        arrays = [np.frombuffer(row,dtype=dtype).astype(np.int64).reshape(t,-1) for row in replies]
        answer = np.zeros(2*self.p["width"],dtype=np.int64)
        for weight,(server,j) in zip(self.weights,self.rows):
            answer = (answer+weight*pow(v,j,q)*arrays[server-1][j])%q
        if any(int(v)>15 for v in answer):
            raise ValueError("recovered symbol outside nibble alphabet")
        return bytes(int(answer[i]) | int(answer[i+1])<<4 for i in range(0,len(answer),2))
