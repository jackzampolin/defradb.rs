//! Fixed-width store control: scalar set-bit Dense kernel, optimized by LLVM.
//! JSON-lines pipe framing is fully metered by the Python endpoint adapter.
use serde_json::{json,Value};
use std::io::{self,BufRead,Write};
#[repr(C)] struct Timespec{sec:i64,nano:i64}
extern "C"{fn clock_gettime(id:i32,t:*mut Timespec)->i32;}
fn cpu()->f64{let mut t=Timespec{sec:0,nano:0};assert_eq!(unsafe{clock_gettime(2,&mut t)},0);t.sec as f64*1000.0+t.nano as f64/1e6}
fn decode(v:&Value)->Vec<u8>{let s=v["bytes"].as_str().unwrap();s.as_bytes().chunks_exact(2).map(|c|u8::from_str_radix(std::str::from_utf8(c).unwrap(),16).unwrap()).collect()}
fn blob(v:&[u8])->Value{json!({"bytes":v.iter().map(|b|format!("{b:02x}")).collect::<String>()})}
fn main(){
    let mut rows=Vec::<Vec<u8>>::new();let mut reads=0usize;let mut writes=0usize;let mut phases=Vec::<Value>::new();
    for line in io::stdin().lock().lines(){
        let start=cpu();let req:Value=serde_json::from_str(&line.unwrap()).unwrap();let v=&req["value"];
        let command=req["command"].as_str().unwrap();
        let result=match command{
            "publish"=>{rows=v.as_array().unwrap().iter().map(decode).collect();assert!(!rows.is_empty());assert!(rows.iter().all(|r|r.len()==rows[0].len()));writes+=rows.len()*rows[0].len();json!(rows.len())},
            "dense"=>{let selector=decode(v);assert_eq!(selector.len(),(rows.len()+7)/8);let mut answer=vec![0u8;rows[0].len()];
                for (byte,q) in selector.into_iter().enumerate(){let mut bits=q;while bits!=0{let at=byte*8+bits.trailing_zeros() as usize;if at<rows.len(){for (a,b) in answer.iter_mut().zip(&rows[at]){*a^=*b;}reads+=rows[at].len();}bits&=bits-1;}}
                blob(&answer)},
            "read"=>{let answer:Vec<_>=v.as_array().unwrap().iter().map(|i|{let r=&rows[i.as_u64().unwrap() as usize];reads+=r.len();blob(r)}).collect();json!(answer)},
            "partition-read"=>{let length=v[0].as_u64().unwrap() as usize;let answer:Vec<_>=v[1].as_array().unwrap().iter().enumerate().map(|(p,i)|{let i=i.as_u64().unwrap() as usize;assert!(i<length);let at=p*length+i;reads+=rows[0].len();if at<rows.len(){blob(&rows[at])}else{blob(&vec![0;rows[0].len()])}}).collect();json!(answer)},
            "write"=>{for pair in v.as_array().unwrap(){let at=pair[0].as_u64().unwrap() as usize;let r=decode(&pair[1]);assert_eq!(r.len(),rows[at].len());writes+=r.len();rows[at]=r;}json!(true)},
            "stats"=>json!({"stored_bytes":rows.iter().map(Vec::len).sum::<usize>(),"logical_read_bytes":reads,"logical_write_bytes":writes}),
            "close"=>json!(phases),_=>panic!("unknown command")};
        let status=std::fs::read_to_string("/proc/self/status").unwrap();let rss=status.lines().find(|s|s.starts_with("VmHWM:")).unwrap().split_whitespace().nth(1).unwrap().parse::<usize>().unwrap()*1024;
        println!("{}",json!({"value":result,"cpu_ms":cpu()-start,"process_cpu_ms":cpu(),"peak_rss_bytes":rss}));io::stdout().flush().unwrap();phases.push(json!({"phase":command,"cpu_ms":cpu()-start}));if command=="close"{break;}
    }
}
