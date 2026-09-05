"""Pinned official Zelda adapter with separated roles and full-process meters."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import time

REVISION = "11b8e70ffcb3ee8d2ea72824c04ed8faa1fa558a"
SOURCE = "https://github.com/p-b-p-b/Zelda.git"

HELPER = r'''package util
import (
 cr "crypto/rand"
 "encoding/binary"
 "encoding/json"
 "math/big"
 "net"
 "os"
 "sync/atomic"
 "syscall"
 "time"
)
type SecureRandom struct{}
func (SecureRandom) Uint64() uint64 { var b [8]byte; if _,e:=cr.Read(b[:]); e!=nil {panic(e)}; return binary.LittleEndian.Uint64(b[:]) }
func (r SecureRandom) Uint32() uint32 { return uint32(r.Uint64()) }
func (SecureRandom) Shuffle(n int, swap func(int,int)) { for i:=n-1;i>0;i-- { j,e:=cr.Int(cr.Reader,big.NewInt(int64(i+1)));if e!=nil {panic(e)};swap(i,int(j.Int64())) } }
var ReadBytes atomic.Uint64
var WrittenBytes atomic.Uint64
var Samples []map[string]interface{}
type Mark struct { Cpu float64; Read,Write uint64; Wall time.Time }
func CpuMS() float64 {var r syscall.Rusage; if e:=syscall.Getrusage(syscall.RUSAGE_SELF,&r);e!=nil {panic(e)};return float64(r.Utime.Sec+r.Stime.Sec)*1000+float64(r.Utime.Usec+r.Stime.Usec)/1000}
func Begin() Mark {return Mark{CpuMS(),ReadBytes.Load(),WrittenBytes.Load(),time.Now()}}
func Finish(m Mark,phase string) {Samples=append(Samples,map[string]interface{}{"phase":phase,"cpu_ms":CpuMS()-m.Cpu,"read_bytes":ReadBytes.Load()-m.Read,"write_bytes":WrittenBytes.Load()-m.Write,"wall_ms":float64(time.Since(m.Wall).Nanoseconds())/1e6})}
type MeteredConn struct { net.Conn }
func (c MeteredConn) Read(b []byte)(int,error) {n,e:=c.Conn.Read(b);ReadBytes.Add(uint64(n));return n,e}
func (c MeteredConn) Write(b []byte)(int,error) {n,e:=c.Conn.Write(b);WrittenBytes.Add(uint64(n));return n,e}
type MeteredListener struct { net.Listener }
func (l MeteredListener) Accept()(net.Conn,error) {c,e:=l.Listener.Accept();if e!=nil{return nil,e};return MeteredConn{c},nil}
func WriteMetrics(path,role string) {
 var r syscall.Rusage;if e:=syscall.Getrusage(syscall.RUSAGE_SELF,&r);e!=nil {panic(e)}
 cpu:=float64(r.Utime.Sec+r.Stime.Sec)*1000+float64(r.Utime.Usec+r.Stime.Usec)/1000
 result:=map[string]interface{}{"role":role,"cpu_ms":cpu,"peak_rss_bytes":r.Maxrss*1024,"read_bytes":ReadBytes.Load(),"write_bytes":WrittenBytes.Load(),"samples":Samples}
 b,e:=json.MarshalIndent(result,"","  ");if e!=nil{panic(e)};if e=os.WriteFile(path,b,0600);e!=nil{panic(e)}
}
'''


def replace(text, old, new, count=1):
    if text.count(old) != count:
        raise ValueError(f"pinned Zelda patch context changed: {old[:70]!r}")
    return text.replace(old,new)


def build(source, output, rows=32768, width=32, queries=100):
    if rows < 32768 or rows & (rows-1) or width%8 or not 8 <= width <= 1024:
        raise ValueError("Zelda requires bounded power-of-two rows >=32768 and 8-byte-aligned rows")
    if subprocess.check_output(["git","-C",str(source),"rev-parse","HEAD"],text=True).strip() != REVISION:
        raise ValueError("Zelda source revision mismatch")
    if subprocess.check_output(["git","-C",str(source),"status","--porcelain"],text=True).strip():
        raise ValueError("Zelda source must be pristine")
    root = output/"source"
    shutil.copytree(source,root,ignore=shutil.ignore_patterns(".git","output.txt"))
    util = root/"util/util.go"
    import re
    code = util.read_text()
    code,n = re.subn(r"DBSize\s*= 1 << 32",f"DBSize = {rows}",code)
    if n != 1:
        raise ValueError("Zelda DBSize patch")
    code,n = re.subn(r"DBEntrySize\s*= 32",f"DBEntrySize = {width}",code)
    if n != 1:
        raise ValueError("Zelda width patch")
    util.write_text(code)
    (root/"util/benchmark.go").write_text(HELPER)
    path = root/"client/client.go"
    code = path.read_text()
    code = replace(code,'"math/rand"','"net"')
    code = replace(code,'var rng *rand.Rand','var rng util.SecureRandom\nvar Prep pb.QueryServiceClient')
    code = replace(code,'\tseed := time.Now().UnixNano()\n\trng = rand.New(rand.NewSource(seed))','')
    code = replace(code,'totalQueryNum := uint32(1000)',f'totalQueryNum := uint32({queries})')
    code = replace(code,'\tsetParameter()', '\tsetupMark := util.Begin()\n\tsetParameter()')
    code = replace(code,'\tI := uint32(0)','\tutil.Finish(setupMark,"setup")\n\tif os.Getenv("PIR_DISCARD_SETUP")=="1" { util.WriteMetrics(os.Getenv("PIR_DISCARD_METRICS"),"discarded-client");os.Exit(75) }\n\tI := uint32(0)')
    code = replace(code,'\tfor q := uint32(0); q < totalQueryNum; q++ {','\tfor q := uint32(0); q < totalQueryNum; q++ {\n\t\tqueryMark := util.Begin()')
    code = replace(code,'\t\ttotalOfflineClientComputeTime += uint64(time.Since(offlineClientStart).Nanoseconds())','\t\ttotalOfflineClientComputeTime += uint64(time.Since(offlineClientStart).Nanoseconds())\n\t\tutil.Finish(queryMark,"complete-query-and-refresh")')
    for method in ("RandomHintQuery","ReplacementEntryQuery","HintComputeTimeQuery"):
        code = code.replace("Server."+method,"Prep."+method)
    code = replace(code,'flag.Parse()', 'prepPtr := flag.String("prep", "", "independent preprocessing endpoint")\n\tmetricsPtr := flag.String("metrics", "", "process metrics output")\n\tflag.Parse()\n\tif *prepPtr == "" || *prepPtr == *addrPtr || *ignorePreprocessingPtr { log.Fatal("distinct preprocessing role and correctness required") }\n\tdefer util.WriteMetrics(*metricsPtr,"client")')
    code = replace(code,'grpc.WithInsecure(),','grpc.WithInsecure(),\n\t\tgrpc.WithContextDialer(func(ctx context.Context,addr string)(net.Conn,error) { c,e:=(&net.Dialer{}).DialContext(ctx,"tcp",addr);if e!=nil{return nil,e};return util.MeteredConn{Conn:c},nil }),')
    code = replace(code,'Server := pb.NewQueryServiceClient(Conn)', '''Server := pb.NewQueryServiceClient(Conn)
 PrepConn, err := grpc.Dial(*prepPtr, grpc.WithInsecure(), grpc.WithBlock(),
 grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize),grpc.MaxCallSendMsgSize(maxMsgSize)),
 grpc.WithContextDialer(func(ctx context.Context,addr string)(net.Conn,error) { c,e:=(&net.Dialer{}).DialContext(ctx,"tcp",addr);if e!=nil{return nil,e};return util.MeteredConn{Conn:c},nil }))
 if err!=nil {log.Fatal(err)};defer PrepConn.Close();Prep=pb.NewQueryServiceClient(PrepConn)''')
    path.write_text(code)
    path = root/"server/server.go"
    code = path.read_text()
    code = replace(code,'"golang.org/x/exp/rand"','"os"\n "os/signal"\n "syscall"\n "strings"\n "fmt"')
    code = replace(code,'var rng *rand.Rand','var rng util.SecureRandom')
    code = replace(code,'\trng = rand.New(rand.NewSource(uint64(time.Now().UnixNano())))','')
    code = replace(code,'flag.Parse()', 'rolePtr := flag.String("role", "", "prep or online")\n\tmetricsPtr := flag.String("metrics", "", "process metrics output")\n\tflag.Parse()\n\tif (*rolePtr != "prep" && *rolePtr != "online") || *ignorePreprocessingPtr {log.Fatal("role and real preprocessing required")}')
    code = replace(code,'port = ":" + port','port = "127.0.0.1:" + port')
    code = replace(code,'s := grpc.NewServer(','''s := grpc.NewServer(
 grpc.UnaryInterceptor(func(ctx context.Context,req interface{},info *grpc.UnaryServerInfo,handler grpc.UnaryHandler)(interface{},error) {
 online:=strings.HasSuffix(info.FullMethod,"/SetParityQuery")
 if strings.HasSuffix(info.FullMethod,"/PlaintextQuery") || (online != (*rolePtr=="online")) {return nil,fmt.Errorf("method forbidden for role")};return handler(ctx,req) }),
 grpc.StreamInterceptor(func(srv interface{},stream grpc.ServerStream,info *grpc.StreamServerInfo,handler grpc.StreamHandler)error {if *rolePtr!="prep" {return fmt.Errorf("hints forbidden for online role")};return handler(srv,stream)}),''')
    code = replace(code,'if err := s.Serve(lis); err != nil {','''signals:=make(chan os.Signal,1);signal.Notify(signals,os.Interrupt,syscall.SIGTERM)
 go func(){<-signals;s.GracefulStop()}()
 defer util.WriteMetrics(*metricsPtr,*rolePtr)
 if err := s.Serve(util.MeteredListener{Listener:lis}); err != nil {''')
    path.write_text(code)
    for component in ("server","client"):
        subprocess.run(["go","build","-o",str((output/component).resolve()),f"{component}/{component}.go"],cwd=root,check=True)
    return dict(revision=REVISION,patch_sha256=hashlib.sha256(HELPER.encode()+code.encode()+(root/"client/client.go").read_bytes()).hexdigest(),
                source="https://eprint.iacr.org/2025/1340",rows=rows,row_bytes=width,queries=queries,
                security="two noncolluding semi-honest roles; non-adaptive query correctness; OS cryptographic randomness; unaudited prototype")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1",0))
        return sock.getsockname()[1]


def run(source, output, rows=32768, width=32, queries=100, timeout=600, clients=1, discard_setup=False):
    if not 1<=clients<=10000 or not 1<=queries<=10000:raise ValueError("Zelda client/query bound")
    output.mkdir(parents=True,exist_ok=False)
    manifest = build(source,output,rows,width,queries)
    processes = []
    handles = []
    ports = [free_port(),free_port()]
    while ports[0] == ports[1]:
        ports[1] = free_port()
    try:
        for role,port in zip(("prep","online"),ports):
            handle = (output/f"{role}.log").open("w")
            handles.append(handle)
            proc = subprocess.Popen([str((output/"server").resolve()),"-port",str(port),"-role",role,
                "-metrics",str((output/f"{role}.json").resolve()),"-numThreads","2"],stdout=handle,stderr=handle)
            processes.append(proc)
        deadline = time.monotonic()+60
        for port,proc in zip(ports,processes):
            while True:
                if proc.poll() is not None or time.monotonic()>deadline:
                    raise RuntimeError("Zelda server startup failed")
                try:
                    with socket.create_connection(("127.0.0.1",port),timeout=.2):
                        break
                except OSError:
                    time.sleep(.05)
        for client_number in range(-int(discard_setup),clients):
            with (output/f"client-{client_number}.log").open("w") as log:
                path=(output/f"client-{client_number}.json").resolve()
                env=dict(os.environ,PIR_DISCARD_SETUP="1" if client_number<0 else "0",PIR_DISCARD_METRICS=str(path))
                result=subprocess.run([str((output/"client").resolve()),"-ip",f"127.0.0.1:{ports[1]}",
                    "-prep",f"127.0.0.1:{ports[0]}","-metrics",str(path)],
                    cwd=output,stdout=log,stderr=log,timeout=timeout,env=env)
                if result.returncode!=(75 if client_number<0 else 0):raise RuntimeError(f"Zelda client {client_number} exited {result.returncode}")
    finally:
        for proc in processes:
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(15)
            except subprocess.TimeoutExpired:
                proc.kill();proc.wait()
        for handle in handles:
            handle.close()
    roles = [json.loads((output/f"{role}.json").read_text()) for role in ("prep","online")]
    client_reports = [json.loads((output/f"client-{i}.json").read_text()) for i in range(-int(discard_setup),clients)]
    client = dict(cpu_ms=sum(c["cpu_ms"] for c in client_reports),read_bytes=sum(c["read_bytes"] for c in client_reports),write_bytes=sum(c["write_bytes"] for c in client_reports),peak_rss_bytes=max(c["peak_rss_bytes"] for c in client_reports))
    result = dict(schema="pir-protocol-work-v1",family="B5",candidate="zelda",manifest=manifest,
        completed_logical_queries=queries*clients,roles=roles,client=client,clients=client_reports,failed_attempts=int(discard_setup),
        server_cpu_ms=sum(r["cpu_ms"] for r in roles),
        server_cpu_ms_per_query=sum(r["cpu_ms"] for r in roles)/(queries*clients),
        aggregate_server_storage_bytes=2*rows*width,
        client_to_server_bytes=client["write_bytes"],server_to_client_bytes=client["read_bytes"],
        correctness="every upstream answer verified; preprocessing bypass rejected",
        maintenance="all replacement entries, new and discarded hints, and unused generated hint batches counted",
        transport="two loopback gRPC endpoints; actual TCP bytes including HTTP/2 framing; no TLS",
        recovery="fresh client process/preprocessing per run; interrupted client state is discarded, never rolled back")
    (output/"result.json").write_text(json.dumps(result,indent=2))
    return result
