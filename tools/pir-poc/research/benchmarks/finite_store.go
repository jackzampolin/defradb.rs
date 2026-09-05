// Copy into the pinned author's pir package; call ColdServe from cmd/cold-store.
package pir

import (
 "bufio"
 "encoding/hex"
 "encoding/json"
 "fmt"
 "os"
 "syscall"
)

func storeCPU() float64 {var r syscall.Rusage;syscall.Getrusage(syscall.RUSAGE_SELF,&r);return float64(r.Utime.Sec+r.Stime.Sec)*1000+float64(r.Utime.Usec+r.Stime.Usec)/1000}
func ColdServe() {
 var enc *EncodedDatabase;var cloud []int;var p *Params
 var configured []int
 phases:=[]map[string]interface{}{};readBytes:=0
 scanner:=bufio.NewScanner(os.Stdin);scanner.Buffer(make([]byte,4096),256<<20)
 writer:=bufio.NewWriter(os.Stdout)
 for scanner.Scan() {
  start:=storeCPU();var req struct{Command string `json:"command"`;Value json.RawMessage `json:"value"`}
  if err:=json.Unmarshal(scanner.Bytes(),&req);err!=nil {panic(err)}
  var value interface{}
  switch req.Command {
  case "configure":
   if enc!=nil {panic("already published")};if err:=json.Unmarshal(req.Value,&configured);err!=nil {panic(err)}
   if len(configured)!=2 || configured[0]<2 || configured[0]>27 || configured[1]<1 || configured[1]%2!=1 || configured[1]>configured[0] {panic("invalid M,D")};value=true
  case "publish":
   var rows []struct{Bytes string `json:"bytes"`};if err:=json.Unmarshal(req.Value,&rows);err!=nil {panic(err)}
   width:=len(rows[0].Bytes)/2;p=PickParams(len(rows),width,0.5)
   limit:=int64(256<<20)
   if len(configured)>0 {p.M=configured[0];p.D=configured[1];limit=2500<<20;if Binomial(p.M,p.D)<p.N {panic("insufficient monomials")}}
   if p.M>=30 || (int64(1)<<p.M)*int64(width)>limit {panic("finite encoded-state preflight")}
   db:=&Database{Num_records:len(rows),Record_len:width,Data:make([]byte,0,len(rows)*width)}
   for _,row:=range rows {b,err:=hex.DecodeString(row.Bytes);if err!=nil || len(b)!=width {panic("record width")};db.Data=append(db.Data,b...)}
   if len(configured)>0 {enc=ColdEncodeFast(db,p)} else {enc=EncodeDatabase(db,p)};cloud=fetchCloud(p);value=p
  case "parameters":value=p
  case "finite":
   var q int;if err:=json.Unmarshal(req.Value,&q);err!=nil {panic(err)}
   if q<0 || q>=1<<p.M {panic("query domain")}
   answer:=Answer(enc,cloud,q);readBytes+=len(answer);value=map[string]string{"bytes":hex.EncodeToString(answer)}
  case "stats":value=map[string]int{"stored_bytes":len(enc.Data),"logical_read_bytes":readBytes}
  case "close":value=phases
  default:panic("unsupported finite store command")
  }
  var usage syscall.Rusage;syscall.Getrusage(syscall.RUSAGE_SELF,&usage)
  response:=map[string]interface{}{"value":value,"cpu_ms":storeCPU()-start,"process_cpu_ms":storeCPU(),"peak_rss_bytes":usage.Maxrss*1024}
  b,err:=json.Marshal(response);if err!=nil {panic(err)};fmt.Fprintln(writer,string(b));writer.Flush()
  phases=append(phases,map[string]interface{}{"phase":req.Command,"cpu_ms":storeCPU()-start})
  if req.Command=="close" {return}
 }
 if err:=scanner.Err();err!=nil {panic(err)}
}

// Exact Boolean zeta transform of the same coefficient polynomial. Unlike the
// reference's bytewise recursion plus interleave, this needs one encoded buffer.
func ColdEncodeFast(db *Database,p *Params) *EncodedDatabase {
 data:=make([]byte,(1<<p.M)*p.Record_len)
 for row:=0;row<p.N;row++ {
  bits:=EncodeIndex(row,p);mask:=0;for bit,on:=range bits {if on {mask|=1<<bit}}
  copy(data[mask*p.Record_len:(mask+1)*p.Record_len],db.Read(row))
 }
 for bit:=0;bit<p.M;bit++ {
  half:=(1<<bit)*p.Record_len
  for base:=0;base<len(data);base+=2*half {
   left,right:=data[base:base+half],data[base+half:base+2*half]
   for i:=range left {right[i]^=left[i]}
  }
 }
 return &EncodedDatabase{Poly:p,Data:data}
}
