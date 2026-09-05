"""Complete-answer protocol cases, with explicit failure and lifecycle costs."""
from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import secrets
import time

from .fields import corpus, ids
from .hermite import Client as HermiteClient, dimensions, encode as hermite_encode
from .mpc import Search
from .oram import PathOram
from .servers import FieldStore, IndexStore, Store, private_row
from .transport import Endpoint, parallel_calls, process_stats, totals


@dataclass
class Case:
    candidate: str = "served-dense"
    rows: int = 256
    row_bytes: int = 32
    queries: int = 10
    clients: int = 1
    seed: int = 1
    bits: int = 16
    group: int = 1
    index_workers: int = 1
    format: str = "planes"
    fanout: int = 4
    slots: int = 4
    distribution: str = "uniform"
    order: str = "random"
    predicate: str = "equality"
    range_width: int = 2
    servers: int = 4
    update_every: int = 0
    update_batch: int = 1
    mutation: str = "value"
    compact_every: int = 4
    recovery_every: int = 0
    client_mbps: float = 0
    fabric_mbps: float = 0
    max_resident_bytes: int = 512<<20
    max_client_bytes: int = 64<<20
    online_budget_bytes: int = 1<<20

    def validate(self):
        if self.candidate not in ("served-dense","served-public","served-decoy","public-index","field-index","mpc-dense","mpc-oram","mpc-compact-dense","mpc-compact-oram","path-oram","hermite","base-delta","registered"):
            raise ValueError("unknown protocol candidate")
        if not 1<=self.rows<=1_000_000 or not 8<=self.row_bytes<=2008 or not 1<=self.queries<=10000 or not 1<=self.clients<=10000:
            raise ValueError("protocol dimensions outside execution bounds")
        if self.bits not in (16,32,64) or not 1<=self.slots<=self.rows or self.update_batch>self.rows or self.fanout<1:
            raise ValueError("invalid field/result/update dimensions")
        if self.predicate not in ("equality","conjunction","range") or not 1<=self.range_width<=8:
            raise ValueError("unsupported predicate")
        if self.mutation not in ("value","insert","delete") or self.client_mbps<0 or self.fabric_mbps<0:
            raise ValueError("invalid lifecycle/transport parameter")
        if self.compact_every<1 or self.update_every<0 or self.recovery_every<0 or self.update_batch<1:
            raise ValueError("invalid maintenance schedule")
        if self.group not in (1,2,4,8) or self.format not in ("planes","bitmap","runs","postings") or (self.format=="planes" and self.group!=1):
            raise ValueError("invalid index representation")
        groups=self.bits*(2 if self.predicate=="conjunction" else 1)//self.group
        if self.index_workers not in (1,2,4,8,16,32,64) or self.index_workers>groups:
            raise ValueError("index workers exceed available field groups")
        roles = 5 if self.candidate.startswith("mpc") else (self.servers if self.candidate=="hermite" else 4)
        if self.candidate=="field-index":roles=2*self.index_workers+2
        estimate = roles*(24<<20)+self.rows*(self.row_bytes+64)*8
        if estimate>self.max_resident_bytes:
            raise ValueError(f"preflight resident estimate {estimate} exceeds budget")
        if self.candidate=="hermite":
            p=dimensions(self.rows,self.row_bytes,self.servers)
            if p["storage_amplification"]>512 or p["storage_per_server"]*self.servers>self.max_resident_bytes:
                raise ValueError("Hermite storage frontier exceeded")
        if "compact" in self.candidate and (self.rows>256 or self.rows&(self.rows-1)):
            raise ValueError("compaction circuit bounded to <=256 power-of-two rows")


def query_spec(case, fields, secondary, query):
    target = int.from_bytes(hashlib.sha256(f"{case.seed}:{query//2}".encode()).digest()[:8],"little")%len(fields)
    value = max(fields)+16 if query%4==3 else fields[target]
    high = value+case.range_width-1 if case.predicate=="range" else value
    return target,value,high,secondary[target]


def run(case, output):
    c=case if isinstance(case,Case) else Case(**case)
    c.validate()
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    records,fields,secondary,permutation=corpus(c.rows,c.row_bytes,c.fanout,c.distribution,c.order,c.seed)
    if max(fields)+32 >= 1<<c.bits:
        raise ValueError("field domain cannot represent an absent query")
    active=[True]*len(records)
    if c.mutation=="insert":
        active[-max(1,c.rows//4):]=[False]*max(1,c.rows//4)
    all_endpoints=[]
    clients=[]
    samples=[]
    maintenance=[]
    exporters=[]
    setup_bytes=[]
    stats=[]
    search=None
    oram=None
    payload=[]
    index=None
    private_index=[]
    hermite=None
    delta=[bytes(9+c.row_bytes) for _ in records]
    base=[bytes([live])+row for live,row in zip(active,records)]
    delta_endpoints=[]
    registered=None
    failed_attempts=0
    actual_updates=0

    def endpoint(factory,role):
        result=Endpoint(factory,role=role);all_endpoints.append(result);return result

    def encoded_fields():
        if c.predicate=="conjunction":
            return [((a|(b<<c.bits))<<1)|int(live) for a,b,live in zip(fields,secondary,active)],2*c.bits+1
        return [(v<<1)|int(live) for v,live in zip(fields,active)],c.bits+1

    def publish_payload():
        for e in payload:
            e.call("publish",records)

    def publish_index():
        cpu=time.process_time_ns()
        if index:
            index.call("publish",[records,fields,secondary,active,asdict(c)])
        if search:
            values,_=encoded_fields();search.publish(values)
        if private_index:
            values=[a|(b<<c.bits) for a,b in zip(fields,secondary)] if c.predicate=="conjunction" else fields
            groups=c.bits*(2 if c.predicate=="conjunction" else 1)//c.group
            for worker in range(c.index_workers):
                assigned=list(range(worker,groups,c.index_workers))
                projected=[sum(((v>>(g*c.group))&((1<<c.group)-1))<<(i*c.group) for i,g in enumerate(assigned)) for v in values]
                for e in private_index[2*worker:2*worker+2]:e.call("publish",[projected,len(assigned)*c.group,c.group,c.format])
        exporters.append(dict(role="field-generation-publisher",cpu_ms=(time.process_time_ns()-cpu)/1e6))

    started=time.perf_counter()
    global_setup_cpu=time.process_time_ns()
    try:
        if c.candidate=="public-index":
            index=endpoint(IndexStore,"public-index");publish_index()
        elif c.candidate=="hermite":
            # Shared immutable exporter runs once, not once per client/replica.
            cpu=time.process_time_ns();parameters,table=hermite_encode(records,c.servers)
            exporters.append(dict(role="shared-polynomial-exporter",cpu_ms=(time.process_time_ns()-cpu)/1e6))
            payload=[endpoint(Store,f"hermite-{i}") for i in range(c.servers)]
            for e in payload:e.call("publish",table)
        elif c.candidate=="path-oram" or c.candidate.endswith("oram"):
            payload=[endpoint(Store,"oram-store")]
        else:
            count=1 if c.candidate in ("served-public","served-decoy") else 2
            payload=[endpoint(Store,f"payload-{i}") for i in range(count)]
            publish_payload()
        if c.candidate.startswith("mpc"):
            values,bits=encoded_fields();search=Search(values,bits,c.fabric_mbps)
            all_endpoints.extend(search.endpoints)
        if c.candidate=="field-index":
            private_index=[endpoint(FieldStore,f"field-worker-{worker}-operator-{operator}") for worker in range(c.index_workers) for operator in range(2)]
            publish_index()
        if c.candidate=="base-delta":
            for e in payload:e.call("publish",base)
            delta_endpoints=[endpoint(Store,f"delta-{i}") for i in range(2)]
            for e in delta_endpoints:e.call("publish",delta)
        exporters.append(dict(role="shared-publication-and-role-setup",cpu_ms=max(0,(time.process_time_ns()-global_setup_cpu)/1e6-sum(p["cpu_ms"] for p in exporters))))
        for client_number in range(c.clients):
            setup_cpu=time.process_time_ns();upload_before=sum(e.sent for e in all_endpoints);download_before=sum(e.received for e in all_endpoints)
            if c.candidate=="path-oram" or c.candidate.endswith("oram"):
                oram=PathOram(payload[0],records,c.max_client_bytes)
            if c.candidate=="hermite":hermite=HermiteClient(parameters)
            if c.candidate=="registered":
                target=query_spec(c,fields,secondary,0)[0]
                width=(len(records)+7)//8
                one=os.urandom(width);two=(int.from_bytes(one,"little")^(1<<target)).to_bytes(width,"little")
                registered=(target,one,two)
                for e,q in zip(payload,(one,two)):e.call("register",q)
            clients.append(dict(client=client_number,setup_cpu_ms=(time.process_time_ns()-setup_cpu)/1e6))
            setup_bytes.append(dict(upload=sum(e.sent for e in all_endpoints)-upload_before,download=sum(e.received for e in all_endpoints)-download_before))
            for q in range(c.queries):
                logical=client_number*c.queries+q
                if c.update_every and logical and logical%c.update_every==0:
                    cpu=time.process_time_ns()
                    changed=[]
                    for j in range(c.update_batch):
                        at=(logical+j)%len(records)
                        if c.mutation=="insert":
                            at=next((i for i,live in enumerate(active) if not live),None)
                            if at is None:
                                raise ValueError("insert reserve exhausted; increase declared physical capacity")
                            active[at]=True;fields[at]=max(fields)+1
                        elif c.mutation=="delete":
                            active[at]=False
                        else:
                            fields[at]=(fields[at]+1)%(max(fields)+1)
                            records[at]=bytes([records[at][0]^1])+records[at][1:]
                        changed.append(at)
                        actual_updates+=1
                    if c.order=="sorted" and c.candidate=="public-index":
                        order=sorted(range(len(fields)),key=fields.__getitem__)
                        records,fields,secondary,active,permutation=([a[i] for i in order] for a in (records,fields,secondary,active,permutation))
                    if c.candidate=="base-delta":
                        for at in changed:delta[at]=logical.to_bytes(8,"little")+bytes([active[at]])+records[at]
                        for e in delta_endpoints:e.call("write",[(i,delta[i]) for i in changed])
                        if actual_updates%c.compact_every==0:
                            merge_cpu=time.process_time_ns()
                            for at,d in enumerate(delta):
                                if int.from_bytes(d[:8],"little"):base[at]=d[8:]
                            delta=[bytes(9+c.row_bytes) for _ in records]
                            for e in payload:e.call("publish",base)
                            for e in delta_endpoints:e.call("publish",delta)
                            exporters.append(dict(role="base-delta-compaction",cpu_ms=(time.process_time_ns()-merge_cpu)/1e6))
                    elif c.candidate=="hermite":
                        start=time.process_time_ns();parameters,table=hermite_encode(records,c.servers)
                        exporters.append(dict(role="polynomial-generation-rebuild",cpu_ms=(time.process_time_ns()-start)/1e6))
                        for e in payload:e.call("publish",table)
                    elif oram:
                        for at in changed:oram.access(at,records[at])
                    else:
                        for e in payload:e.call("write",[(i,records[i]) for i in changed])
                    publish_index()
                    maintenance.append(dict(after_query=logical,changed_records=len(changed),owner_cpu_ms=(time.process_time_ns()-cpu)/1e6))
                cpu=time.process_time_ns();wall=time.perf_counter_ns()
                up=sum(e.sent for e in all_endpoints);down=sum(e.received for e in all_endpoints)
                target,value,high,other=query_spec(c,fields,secondary,q)
                expected=[i for i,v in enumerate(fields) if active[i] and value<=v<=high and (c.predicate!="conjunction" or secondary[i]==other)]
                selected=[]
                # Query-corpus selection and a full-scan correctness oracle are
                # benchmark fixtures, not application-client algorithm work.
                cpu=time.process_time_ns();wall=time.perf_counter_ns()
                if c.candidate=="public-index":
                    reply=index.call("query",[c.predicate,value,high,other])
                    selected=[i for i,row in reply if i>=0]
                    if selected!=expected or any(row!=records[i] for i,row in reply if i>=0):raise AssertionError("public index complete result")
                elif private_index:
                    bitmap=0
                    for v in range(value,high+1):
                        encoded=v|(other<<c.bits) if c.predicate=="conjunction" else v
                        intersection=(1<<len(records))-1
                        groups=c.bits*(2 if c.predicate=="conjunction" else 1)//c.group
                        for group in range(groups):
                            bucket=(encoded>>(group*c.group))&((1<<c.group)-1)
                            width=((1<<c.group)+7)//8
                            one=os.urandom(width);two=(int.from_bytes(one,"little")^(1<<bucket)).to_bytes(width,"little")
                            worker=group%c.index_workers;local_group=group//c.index_workers
                            replies=[e.call("select",[local_group,share]) for e,share in zip(private_index[2*worker:2*worker+2],(one,two))]
                            intersection&=int.from_bytes(replies[0],"little")^int.from_bytes(replies[1],"little")
                        bitmap|=intersection
                    selected=[i for i in ids(bitmap) if active[i]]
                    if selected!=expected or len(selected)>c.slots:raise AssertionError("private compressed index complete result")
                    for slot in range(c.slots):
                        at=selected[slot] if slot<len(selected) else 0
                        if private_row(payload,at,len(records))!=records[at]:raise AssertionError("compressed index payload")
                elif search:
                    values=[(((v|(other<<c.bits)) if c.predicate=="conjunction" else v)<<1)|1 for v in range(value,high+1)]
                    node=search.query_values(values)
                    selected=sorted(search.compact(node,c.slots)) if "compact" in c.candidate else list(ids(search.reconstruct(node)))
                    if selected!=expected or len(selected)>c.slots:raise AssertionError("private complete intersection/compaction")
                    for slot in range(c.slots):
                        at=selected[slot] if slot<len(selected) else 0
                        row=oram.access(at) if oram else private_row(payload,at,len(records))
                        if row!=records[at]:raise AssertionError("private indexed payload")
                elif c.candidate=="hermite":
                    direction,points=hermite.query(target)
                    replies=[e.call("read",[point])[0] for e,point in zip(payload,points)]
                    if hermite.recover(direction,replies)!=records[target]:raise AssertionError("Hermite recovery")
                elif c.candidate=="base-delta":
                    b=private_row(payload,target,len(records));d=private_row(delta_endpoints,target,len(records))
                    answer=d[8:] if int.from_bytes(d[:8],"little") else b
                    if answer!=bytes([active[target]])+records[target]:raise AssertionError("base/delta current answer")
                elif oram:
                    if c.recovery_every and q and q%c.recovery_every==0:
                        try:oram.access(target,interrupt=True)
                        except ConnectionError:failed_attempts+=1
                        else:raise AssertionError("failure injection did not fail")
                        # Charge complete recovery from the authoritative source.
                        oram=PathOram(payload[0],records,c.max_client_bytes)
                    if oram.access(target)!=records[target]:raise AssertionError("ORAM row")
                    checkpoint=output/f"owner-{client_number}-{q}.state"
                    _,digest=oram.persist(checkpoint);oram.restore(checkpoint,digest,oram.epoch)
                elif c.candidate=="served-public":
                    if payload[0].call("read",[target])[0]!=records[target]:raise AssertionError("public row")
                elif c.candidate=="served-decoy":
                    candidates=[secrets.randbelow(len(records)) for _ in range(99)]+[target]
                    secrets.SystemRandom().shuffle(candidates)
                    replies=payload[0].call("read",candidates)
                    if replies[candidates.index(target)]!=records[target]:raise AssertionError("decoy row")
                elif c.candidate=="registered":
                    target=registered[0]
                    replies=[e.call("registered",0) for e in payload]
                    if (int.from_bytes(replies[0],"little")^int.from_bytes(replies[1],"little")).to_bytes(c.row_bytes,"little")!=records[target]:raise AssertionError("registered row")
                elif private_row(payload,target,len(records))!=records[target]:raise AssertionError("Dense row")
                upload=sum(e.sent for e in all_endpoints)-up;download=sum(e.received for e in all_endpoints)-down
                compute_cpu=(time.process_time_ns()-cpu)/1e6
                # Application-level link pacing: does not alter host routing or
                # pretend to emulate packet loss/congestion of a shaped NIC.
                if c.client_mbps:
                    desired=(upload+download)*8/(c.client_mbps*1e6)
                    elapsed=(time.perf_counter_ns()-wall)/1e9
                    if desired>elapsed:time.sleep(desired-elapsed)
                samples.append(dict(client=client_number,query=q,client_cpu_ms=compute_cpu,
                    verified_latency_ms=(time.perf_counter_ns()-wall)/1e6,upload_bytes=upload,download_bytes=download,
                    complete_result_records=len(selected) if search or index or private_index else 1,
                    padded_slots=c.slots if search or index or private_index else 1))
            if oram:stats.append(oram.stats())
        if c.candidate=="base-delta" and any(int.from_bytes(d[:8],"little") for d in delta):
            # Close the maintenance horizon: the last short delta cannot be
            # left as an unpaid liability merely because the run ended.
            merge_cpu=time.process_time_ns()
            for at,d in enumerate(delta):
                if int.from_bytes(d[:8],"little"):base[at]=d[8:]
            if base!=[bytes([live])+row for live,row in zip(active,records)]:raise AssertionError("final current-state compaction")
            delta=[bytes(9+c.row_bytes) for _ in records]
            for e in payload:e.call("publish",base)
            for e in delta_endpoints:e.call("publish",delta)
            exporters.append(dict(role="closing-base-delta-compaction",cpu_ms=(time.process_time_ns()-merge_cpu)/1e6))
        for e in all_endpoints:stats.append(e.call("stats"))
    finally:
        report=totals(all_endpoints)
        (output/"work-ledger.json").write_text(json.dumps(dict(report,completed_logical_queries=len(samples),exporter_phases=exporters),indent=2))
    if report["role_errors"]:raise RuntimeError(report["role_errors"])
    report.update(schema="pir-protocol-work-v1",config=asdict(c),completed_logical_queries=len(samples),
        samples=samples,client_setup=clients,client_setup_bytes=setup_bytes,maintenance=maintenance,
        exporter_phases=exporters,store_stats=stats,failed_attempts=failed_attempts,actual_updates=actual_updates,
        client_peak_rss_bytes=process_stats()["peak_rss_bytes"],elapsed_seconds=time.perf_counter()-started,
        lane="Python protocol implementation; compare with matching served Python baseline, not Rust kernel timings",
        private=c.candidate not in ("served-public","served-decoy","public-index"),
        collusion_tolerance=1 if c.candidate.startswith("mpc") or c.candidate=="hermite" else (0 if c.candidate.startswith("served-p") or c.candidate=="served-decoy" or index else 1),
        leakage="registered query identifier is linkable" if c.candidate=="registered" else "fixed public query/result schedule",
        transport="actual loopback TCP JSON/base64 and framed peer exchanges; local-only, no TLS",
        client_rss_scope="includes the benchmark coordinator and ground-truth corpus; conservative, not isolated application-client RSS",
        client_cpu_scope="request construction, response combination, verification and state management; synthetic query selection and full-scan oracle excluded",
        pacing="application-level client link pacing" if c.client_mbps else "unshaped",
        physical_dram_bytes=None,energy_joules=None,gpu_active_ms=None)
    report["server_cpu_ms"]+=sum(p["cpu_ms"] for p in exporters)
    report["server_cpu_ms_per_query"]=report["server_cpu_ms"]/len(samples)
    report["conservative_resident_budget_pass"]=report["aggregate_peak_role_rss_bytes"]+report["client_peak_rss_bytes"]<=c.max_resident_bytes
    report["client_caps_pass"]=report["client_peak_rss_bytes"]<=128<<20 and all(s["setup_cpu_ms"]<=10000 for s in clients) and all(s["client_cpu_ms"]<=1000 and s["upload_bytes"]<=c.online_budget_bytes and s["download_bytes"]<=c.online_budget_bytes for s in samples) and all(s["download"]<=64<<20 for s in setup_bytes)
    return report
