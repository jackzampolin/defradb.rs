// Copy into the pinned official finite-differences pir package. This diagnostic
// meters both answers and actual encodings; it is not a multi-host deployment.
package pir

import (
    "crypto/rand"
    "crypto/sha256"
    "encoding/binary"
    "encoding/json"
    "fmt"
    "math/bits"
    "os"
    "syscall"
    "testing"
)

func coldCPU() float64 {
    var r syscall.Rusage
    if err:=syscall.Getrusage(syscall.RUSAGE_SELF,&r);err!=nil { panic(err) }
    return float64(r.Utime.Sec+r.Stime.Sec)*1000+float64(r.Utime.Usec+r.Stime.Usec)/1000
}
func coldTag(i int) uint64 {
    h:=sha256.Sum256([]byte(fmt.Sprintf("cold-tag-%d",i)))
    return binary.LittleEndian.Uint64(h[:8])
}
func coldBucket(key uint64,n int) int {
    var b [8]byte;binary.LittleEndian.PutUint64(b[:],key)
    h:=sha256.Sum256(b[:]);return int(binary.LittleEndian.Uint64(h[:8])%uint64(n))
}

func TestColdCompleteTagSearch(t *testing.T) {
    type sample struct { ServerCPU float64; ClientCPU float64; Download int; Matches int }
    results:=[]map[string]interface{}{}
    for _,n:=range []int{256,1024,4096,16384} {
      for _,payload:=range []int{32,96} {
        slots:=32;stride:=13+payload;width:=slots*stride
        buckets:=1<<bits.Len(uint(n/4-1));p:=PickParams(buckets,width,0.5)
        encodedBytes:=2*(int64(1)<<p.M)*int64(width)
        row:=map[string]interface{}{"source_rows":n,"payload_bytes":payload,"page_bytes":width,"buckets":buckets,"params":p,"aggregate_encoded_bytes":encodedBytes}
        if encodedBytes>512<<20 {row["status"]="memory-preflight";results=append(results,row);continue}
        db:=&Database{Num_records:buckets,Record_len:width,Data:make([]byte,buckets*width)}
        used:=make([]int,buckets)
        for i:=0;i<n;i++ {
            key:=coldTag(i/2);bucket:=coldBucket(key,buckets);at:=used[bucket]
            if at>=slots {t.Fatalf("public bucket overflow n=%d",n)}
            used[bucket]++;offset:=bucket*width+at*stride
            db.Data[offset]=1;binary.LittleEndian.PutUint64(db.Data[offset+1:],key)
            binary.LittleEndian.PutUint32(db.Data[offset+9:],uint32(i))
            for j:=0;j<payload;j++ {db.Data[offset+13+j]=byte(i+j)}
        }
        start:=coldCPU();enc0:=EncodeDatabase(db,p);enc1:=EncodeDatabase(db,p)
        row["two_server_build_cpu_ms"]=coldCPU()-start
        cloud:=fetchCloud(p);samples:=[]sample{}
        for q:=0;q<16;q++ {
            key:=coldTag(q*7%(n/2));if q%4==0 {key=coldTag(n+q)}
            start=coldCPU();at:=coldBucket(key,buckets);state:=EncodingToIndex(EncodeIndex(at,p))
            var randomness [8]byte;if _,err:=rand.Read(randomness[:]);err!=nil {panic(err)}
            r:=int(binary.LittleEndian.Uint64(randomness[:])&((uint64(1)<<p.M)-1))
            clientCPU:=coldCPU()-start
            start=coldCPU();a:=Answer(enc0,cloud,r);b:=Answer(enc1,cloud,state^r);serverCPU:=coldCPU()-start
            start=coldCPU();page:=Recover(p,cloud,state,a,b);found:=0
            for j:=0;j<slots;j++ {
                off:=j*stride
                if page[off]==1 && binary.LittleEndian.Uint64(page[off+1:])==key {
                    id:=int(binary.LittleEndian.Uint32(page[off+9:]));found++
                    for k:=0;k<payload;k++ {if page[off+13+k]!=byte(id+k) {t.Fatal("payload mismatch")}}
                }
            }
            clientCPU+=coldCPU()-start
            expected:=2;if q%4==0 {expected=0};if found!=expected {t.Fatalf("incomplete answer: %d != %d",found,expected)}
            samples=append(samples,sample{serverCPU,clientCPU,len(a)+len(b),found})
        }
        row["status"]="verified";row["samples"]=samples
        row["qualification"]="actual two encodings and CSPRNG queries; same-process diagnostic; routing tag predicate with collision-checked full payloads; no canonical provenance"
        results=append(results,row)
      }
    }
    if path:=os.Getenv("PIR_COLD_FINITE_OUTPUT");path!="" {b,_:=json.MarshalIndent(results,"","  ");if err:=os.WriteFile(path,b,0644);err!=nil {t.Fatal(err)}}
}
