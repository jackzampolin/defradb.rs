"""Bounded JSON RPC over real loopback TCP, with per-process CPU accounting.

Loopback is an isolated benchmark transport, not a public service. Independent
operator deployments must put the endpoints behind authenticated SSH tunnels.
Binary values use base64; framing/serialization bytes and CPU are included.
"""
import base64
from concurrent.futures import ThreadPoolExecutor
import json
import multiprocessing as mp
import os
import resource
import socket
import struct
import time

MAX_FRAME = 256 << 20


def encode(value):
    if isinstance(value, bytes):
        return {"$bytes": base64.b64encode(value).decode("ascii")}
    raise TypeError(type(value).__name__)


def decode(value):
    if set(value) == {"$bytes"}:
        return base64.b64decode(value["$bytes"], validate=True)
    return value


def receive_exact(sock, count):
    chunks = bytearray()
    while len(chunks) < count:
        chunk = sock.recv(count-len(chunks))
        if not chunk:
            raise EOFError("peer disconnected mid-frame")
        chunks.extend(chunk)
    return bytes(chunks)


def send(sock, value):
    blob = json.dumps(value, default=encode, separators=(",", ":")).encode()
    if len(blob) > MAX_FRAME:
        raise ValueError("RPC frame exceeds bound")
    sock.sendall(struct.pack("!I", len(blob))+blob)
    return len(blob)+4


def receive(sock):
    size = struct.unpack("!I", receive_exact(sock,4))[0]
    if size > MAX_FRAME:
        raise ValueError("RPC frame exceeds bound")
    return json.loads(receive_exact(sock,size), object_hook=decode), size+4


def socket_pair():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1",0))
        listener.listen(1)
        one = socket.create_connection(listener.getsockname(),timeout=120)
        two, _ = listener.accept()
    for sock in (one,two):
        sock.settimeout(120)
        sock.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
    return one,two


def process_stats():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return dict(cpu_ms=1000*(usage.ru_utime+usage.ru_stime),
                user_ms=1000*usage.ru_utime, kernel_ms=1000*usage.ru_stime,
                peak_rss_bytes=usage.ru_maxrss*1024)


def worker(sock, factory, args, role):
    sock.settimeout(120)
    start = process_stats()
    state = factory(*args)
    phases = []
    wire_in = wire_out = 0
    while True:
        cpu = time.process_time_ns()
        wall = time.perf_counter_ns()
        request, size = receive(sock)
        wire_in += size
        command = request["command"]
        if command == "close":
            stats = process_stats()
            report = dict(role=role, pid=os.getpid(), phases=phases,
                          process_cpu_ms=stats["cpu_ms"],
                          peak_rss_bytes=stats["peak_rss_bytes"],
                          received_bytes=wire_in, sent_bytes=wire_out,
                          peer_sent_bytes=getattr(state,"peer_sent_bytes",0))
            send(sock,report)
            sock.close()
            return
        try:
            result = state.handle(command,request.get("value"))
            response = {"ok":True,"value":result}
        except Exception as exc:
            response = {"ok":False,"error":f"{type(exc).__name__}: {exc}"}
        wire_out += send(sock,response)
        phases.append(dict(id=len(phases),phase=command,cpu_ms=(time.process_time_ns()-cpu)/1e6,
                           wall_ms=(time.perf_counter_ns()-wall)/1e6,success=response["ok"]))


class Endpoint:
    def __init__(self, factory, args=(), role="server"):
        parent, child = socket_pair()
        self.sock = parent
        # Spawn isolates client memory and ensures RSS/CPU are attributable.
        self.process = mp.get_context("spawn").Process(target=worker,args=(child,factory,args,role))
        self.process.start()
        child.close()
        self.sent = self.received = 0
        self.calls = 0
        self.report = None

    def call(self, command, value=None):
        self.calls += 1
        self.sent += send(self.sock,dict(command=command,value=value))
        response, size = receive(self.sock)
        self.received += size
        if not response["ok"]:
            raise RuntimeError(response["error"])
        return response["value"]

    def close(self):
        if self.report is not None:
            return self.report
        try:
            self.sent += send(self.sock,dict(command="close"))
            self.report, size = receive(self.sock)
            self.received += size
            self.process.join(10)
            if self.process.exitcode != 0:
                raise RuntimeError("worker did not finish cleanly")
            return self.report
        finally:
            self.sock.close()
            if self.process.is_alive():
                self.process.terminate()
                self.process.join()


def parallel_calls(endpoints, command, values):
    with ThreadPoolExecutor(max_workers=len(endpoints)) as pool:
        futures = [pool.submit(e.call,command,v) for e,v in zip(endpoints,values)]
        return [f.result() for f in futures]


def totals(endpoints):
    roles,errors = [],[]
    for endpoint in endpoints:
        try:
            roles.append(endpoint.close())
        except Exception as exc:
            errors.append(str(exc))
    return dict(roles=roles,server_cpu_ms=sum(r["process_cpu_ms"] for r in roles),
                role_errors=errors,
                client_to_server_bytes=sum(e.sent for e in endpoints),
                server_to_client_bytes=sum(e.received for e in endpoints),
                inter_server_bytes=sum(r["peer_sent_bytes"] for r in roles),
                aggregate_peak_role_rss_bytes=sum(r["peak_rss_bytes"] for r in roles),
                rss_note="Sum of role high-water marks, not simultaneous fleet peak; client RSS separately reported.")
