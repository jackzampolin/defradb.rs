"""Staged experiments: vary one factor, then scale survivors. No Cartesian explosion."""
from dataclasses import asdict
from .cases import Case


def matrix(profile="smoke"):
    smoke=profile=="smoke"
    n=32 if smoke else 4096
    q=4 if smoke else 100
    result=[]
    def add(family,engine,name,**config):
        result.append(dict(family=family,engine=engine,name=name,config=config))
    def protocol(family,name,candidate,**config):
        add(family,"protocol",name,**asdict(Case(candidate=candidate,rows=config.pop("rows",n),queries=config.pop("queries",q),**config)))
    def native(family,name,candidate,**config):
        add(family,"native",name,candidate=candidate,rows=config.pop("rows",256 if smoke else 262144),row_bytes=config.pop("row_bytes",96),queries=config.pop("queries",q),field_bits=config.pop("field_bits",32),**config)
    for mode in ("dense","public","decoy"):
        native("B0","cpu-"+mode,mode)
        protocol("B0","tcp-"+mode,"served-"+mode)
    for workers in (1,2,4,8):native("B0",f"shards-{workers}","sharded",workers=workers)
    for format in ("planes","bitmap","runs","postings"):
        for group in ((1,) if format=="planes" else (1,2,4,8)):
            protocol("B1",f"index-{format}-{group}","public-index",format=format,group=group)
            protocol("B1",f"private-index-{format}-{group}","field-index",format=format,group=group)
    for bits in (16,32,64):
        for group in (1,2,4,8):native("B1",f"private-bitmap-{bits}-{group}","field-bitmap",field_bits=bits,group_bits=group,fanout=8 if bits==16 and not smoke else 4,payload_slots=8 if bits==16 and not smoke else 4)
    for workers in (1,2,4,8,16,32,64):
        protocol("B1",f"bit-owners-{workers}","field-index",index_workers=workers,bits=64 if workers>16 else 16)
    for distribution,order in (("uniform","random"),("uniform","sorted"),("skewed","random"),("clustered","sorted")):
        for predicate in ("equality","range","conjunction"):
            protocol("B1",f"workload-{distribution}-{order}-{predicate}","public-index",distribution=distribution,order=order,predicate=predicate,slots=n)
    for fanout in ((1,4,16) if smoke else (1,4,16,256,1024)):
        protocol("B1",f"fanout-{fanout}","public-index",fanout=fanout,slots=min(n,fanout))
    for mode in ("mpc-dense","mpc-oram","mpc-compact-dense","mpc-compact-oram"):
        protocol("B2",mode,mode,rows=16 if smoke else 128)
    for bits in (16,32,64):
        for predicate in ("equality","range","conjunction"):
            protocol("B2",f"intersection-{bits}-{predicate}","mpc-dense",bits=bits,predicate=predicate,slots=8)
    for g in (2,4,6,8,10):
        for cold in (0,1<<20 if smoke else 64<<20):
            native("B3",f"subset-{g}-cold-{cold}","subset",group_bits=g,cold_cache_bytes=cold)
    for k in ((1,8) if smoke else (1,8,32,128,512)):
        for kernel in ("independent","shared","blocked","transposed","four-russians"):
            native("B4",f"batch-{kernel}-{k}","batch",kernel=kernel,batch_size=k,queries=2 if smoke else 10)
    for dwell in (0,5,20,100):
        native("B4",f"arrival-{dwell}","batch",batch_size=8 if smoke else 128,queries=2 if smoke else 4,arrival_interval_ms=1,max_queue_dwell_ms=dwell)
    protocol("B4","registered-linkable","registered")
    for partitions in (2,4,8,16,32):
        native("B5",f"single-pass-{partitions}","single-pass",partitions=partitions)
    for clients in ((1,2) if smoke else (1,10,100)):
        # Each native invocation creates a fresh client and its complete setup.
        add("B5","native-clients",f"single-pass-clients-{clients}",candidate="single-pass",rows=256 if smoke else 262144,row_bytes=96,queries=q,clients=clients,field_bits=32)
    for width in ((32,) if smoke else (32,96,256,1024)):
        add("B5","zelda",f"zelda-{width}",rows=32768 if smoke else 262144,width=width,queries=12 if smoke else 100)
    add("B5","zelda","zelda-multiple-clients",rows=32768,width=32,queries=12 if smoke else 100,clients=2 if smoke else 10)
    add("B5","zelda","zelda-recovery",rows=32768,width=32,queries=12 if smoke else 100,discard_setup=True)
    native("B6","finite-differences","finite-differences")
    for servers in (4,8,16,32,64,128):
        protocol("B6",f"hermite-{servers}","hermite",rows=16 if smoke else 128,servers=servers)
    for clients in ((1,2) if smoke else (1,10,100)):
        protocol("B7",f"oram-clients-{clients}","path-oram",clients=clients)
    protocol("B7","oram-recovery","path-oram",recovery_every=2)
    for candidate in ("path-oram","base-delta","mpc-dense","public-index"):
        for mutation in ("insert","delete","value"):
            protocol("B8",f"lifecycle-{candidate}-{mutation}",candidate,update_every=2 if smoke else 10,mutation=mutation,slots=n)
    for every in ((2,) if smoke else (1,10,100)):
        for candidate in ("dense","subset","single-pass","field-bitmap"):
            native("B8",f"rebuild-{candidate}-{every}",candidate,rebuild_every=every,update_batch=1 if smoke else 100)
        protocol("B8",f"base-delta-compact-{every}","base-delta",queries=max(q,2*every+1),update_every=every,compact_every=2)
    native("B8","canonical-witness","witness",rows=4 if smoke else 256,row_bytes=2008,queries=6 if smoke else 100,rebuild_every=3 if smoke else 10)
    for mbps in (25,100,1000):
        protocol("B0",f"client-link-{mbps}","served-dense",client_mbps=mbps)
    for mbps in (100,1000,10000):
        protocol("B2",f"fabric-link-{mbps}","mpc-dense",fabric_mbps=mbps,client_mbps=100)
    for width in ((32,) if smoke else (32,96,256,1024)):
        for candidate in ("dense","dpf","public","decoy"):
            add("B0","gpu",f"gpu-{candidate}-{width}",candidate=candidate,rows=1024 if smoke else 262144,row_bytes=width,queries=4 if smoke else 100,batch=1)
    if not smoke:
        for k in (8,32,128,512):
            for candidate in ("dense","dpf"):
                add("B4","gpu",f"gpu-batch-{candidate}-{k}",candidate=candidate,rows=262144,row_bytes=96,queries=5,batch=k)
    if profile=="scale":
        for rows in (1_048_576,10_000_000,100_000_000,1_000_000_000):
            for width in (32,96,256,1024):
                for candidate in ("dense","single-pass","subset","field-bitmap"):
                    native("B0" if candidate=="dense" else "B5" if candidate=="single-pass" else "B3" if candidate=="subset" else "B1",f"scale-{candidate}-{rows}-{width}",candidate,rows=rows,row_bytes=width,queries=100)
    return result
