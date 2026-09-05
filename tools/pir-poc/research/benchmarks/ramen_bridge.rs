//! JSON-lines client bridge for the pinned Ramen artifact; three independent processes.
//! Instructions/database are fresh additive shares, never a public address trace.
use communicator::tcp::{make_tcp_communicator, NetworkOptions, NetworkPartyInfo};
use communicator::AbstractCommunicator;
use dpf::{mpdpf::SmartMpDpf, spdpf::HalfTreeSpDpf};
use ff::PrimeField;
use oram::{common::InstructionShare, oram::{DistributedOram, DistributedOramProtocol}};
use utils::{field::Fp, hash::AesHashFunction};
use serde_json::{json, Value};
use std::io::{self, BufRead, Write};
type D = DistributedOramProtocol<Fp, SmartMpDpf<Fp, HalfTreeSpDpf<Fp>, AesHashFunction<u16>>, HalfTreeSpDpf<Fp>>;
#[repr(C)] struct Timespec { sec: i64, nano: i64 }
extern "C" { fn clock_gettime(id: i32, t: *mut Timespec) -> i32; }
fn cpu() -> f64 { let mut t=Timespec{sec:0,nano:0}; assert_eq!(unsafe{clock_gettime(2,&mut t)},0); t.sec as f64*1000.0+t.nano as f64/1e6 }
fn field(v: &Value) -> Fp { Fp::from_u128(v.as_str().unwrap().parse().unwrap()) }
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let party: usize=args[1].parse().unwrap();
    let ports: Vec<u16>=args[2].split(',').map(|s|s.parse().unwrap()).collect();
    rayon::ThreadPoolBuilder::new().num_threads(1).build_global().unwrap();
    let opts=NetworkOptions{listen_host:"127.0.0.1".into(),listen_port:ports[party],
        connect_info:(0..3).map(|i|if i<party {NetworkPartyInfo::Connect("127.0.0.1".into(),ports[i])}else{NetworkPartyInfo::Listen}).collect(),connect_timeout_seconds:30};
    let mut comm=make_tcp_communicator(3,party,&opts).unwrap();
    let mut db: Option<D>=None;
    let mut phases=Vec::<Value>::new();
    for line in io::stdin().lock().lines() {
        let request: Value=serde_json::from_str(&line.unwrap()).unwrap();
        let start=cpu();
        let result=match request["command"].as_str().unwrap() {
            "init" => { let shares: Vec<_>=request["values"].as_array().unwrap().iter().map(field).collect();
                let mut d=D::new(party,shares.len()); d.init(&mut comm,&shares).unwrap(); db=Some(d); json!(true) },
            "access" => { let mut result=Vec::new();
                for inst in request["values"].as_array().unwrap() {
                    let answer=db.as_mut().unwrap().access(&mut comm,InstructionShare{operation:field(&inst[0]),address:field(&inst[1]),value:field(&inst[2])}).unwrap();
                    result.push(u128::from_le_bytes(answer.to_le_bytes()).to_string());
                } json!(result) },
            "close" => { comm.shutdown(); json!(phases) },
            _ => panic!("unknown command"),
        };
        let peer_bytes: usize=comm.get_stats().values().map(|s|s.num_bytes_sent).sum();
        let status=std::fs::read_to_string("/proc/self/status").unwrap();
        let rss: usize=status.lines().find(|s|s.starts_with("VmHWM:")).unwrap().split_whitespace().nth(1).unwrap().parse::<usize>().unwrap()*1024;
        println!("{}",json!({"value":result,"cpu_ms":cpu()-start,"process_cpu_ms":cpu(),"peer_sent_bytes":peer_bytes,"peak_rss_bytes":rss}));
        io::stdout().flush().unwrap();
        phases.push(json!({"phase":request["command"],"cpu_ms":cpu()-start}));
        if request["command"]=="close" {break;}
    }
}
