"""Summarize backend diagnostics separately from complete-search comparisons."""
from collections import defaultdict
import json
from pathlib import Path
import re
import statistics as st

target=Path('/mnt/c/src/defradb.rs/target');groups=defaultdict(list)
for path in target.glob('pir-cold-gpu-v2-*/result.json'):
    r=json.loads(path.read_text());c=r['config'];key=(c['clients'],c['window'],c['spacing']);groups[key].append((path,r))
gpu=[]
for (clients,window,spacing),runs in sorted(groups.items()):
    samples=[s for _,r in runs for s in r['samples']]
    batch_sizes=[int(n) for path,_ in runs for n in re.findall(r'Cold batch size (\d+)',(path.parent/'server.log').read_text())]
    gpu.append(dict(clients=clients,window_ms=window,spacing_ms=spacing,repeats=len(runs),answers=len(samples),
        server_cpu_per_answer_ms=st.median(r['batch_server_cpu_ms']/clients for _,r in runs),
        server_cpu_counter_uncertainty_ms=10/clients,
        generation_server_cpu_ms=st.median(r['publication_server_cpu_ms'] for _,r in runs),
        median_batch_wall_ms=st.median(r['batch_wall_ms'] for _,r in runs),
        max_client_cpu_upper_bound_ms=max(s['client_cpu_upper_bound_ms'] for s in samples),
        max_client_rss_bytes=max(s['client_peak_rss_bytes'] for s in samples),
        cap_failures=sum(not s['caps_pass'] for s in samples),
        actual_gpu_batch_min=min(batch_sizes),actual_gpu_batch_max=max(batch_sizes),
        qualification='isolated complete clients on same host; aggregate CPU only, GPU active timers separate in raw logs; colocated client startup dominates group wall time'))
groups=defaultdict(list)
for path in (target/'pir-cold-dense-batch-v1').glob('*.json'):
    r=json.loads(path.read_text());groups[(r['rows'],r['batch'],r['padded_2048'])].append(r)
dense=[dict(rows=n,batch=b,padded_2048=p,repeats=len(rs),server_cpu_per_answer_ms=st.median(r['server_cpu_per_answer_ms'] for r in rs)) for (n,b,p),rs in sorted(groups.items())]
(target/'pir-cold-artifact-summary.json').write_text(json.dumps(dict(gpu=gpu,dense_batches=dense),indent=2))
print(json.dumps(dict(gpu=gpu,dense_batches=dense),indent=2))
