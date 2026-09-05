"""Aggregate repeated cases without treating failures or old RSS as zero work."""
import argparse
import csv
from collections import defaultdict
import json
from pathlib import Path
import statistics as st


def main():
    p=argparse.ArgumentParser();p.add_argument('campaigns',nargs='+',type=Path);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    rows=[]
    for campaign in a.campaigns:
        groups=defaultdict(list)
        for result in campaign.glob('*-r*/result.json'):
            r=json.loads(result.read_text())
            if not r.get('correct') or len(r['clients'])!=r['config']['clients']:
                raise ValueError(f'incomplete or incorrect benchmark: {result}')
            if any(len(client['samples'])!=r['config']['queries_per_client'] or not all(s['correct'] for s in client['samples']) for client in r['clients']):
                raise ValueError(f'incomplete answers: {result}')
            groups[json.dumps(r['config'],sort_keys=True)].append(r)
        for config,runs in groups.items():
            c=json.loads(config);clients=[r for run in runs for r in run['clients']]
            external_fixture_cpu=0
            if c.get('canonical_file'):
                external_fixture_cpu=json.loads(Path(c['canonical_file']).read_text()).get('build_cpu_ms',0)
            samples=[s for client in clients for s in client['samples']]
            fresh_rss=all('rss_method' in r['client_process'] for r in clients)
            failures=sorted({f for client in clients for f in client['budget_failures'] if fresh_rss or f!='client-rss'})
            service=[r['cold_service_cpu_per_answer_ms'] for r in runs]
            native_unattributed=[run['all_server_process_cpu_ms']-sum(p['cpu_ms'] for role in run['roles'] for p in role['phases']) for run in runs]
            full_cpu=[run['build_cpu_ms']+run['publication_gateway_cpu_ms']+run['all_server_process_cpu_ms']+
                      sum(sum(client['gateway_cpu_ms'].values()) for client in run['clients'])+external_fixture_cpu for run in runs]
            global_cpu=[r['build_cpu_ms']+r['publication_gateway_cpu_ms']+r['all_server_process_cpu_ms']-
                        sum(sum(client['server_phase_cpu_ms'].values()) for client in r['clients'])+external_fixture_cpu for r in runs]
            setup=[sum(r['server_phase_cpu_ms'].get('setup',0)+r['gateway_cpu_ms'].get('setup',0) for r in run['clients'])/len(run['clients']) for run in runs]
            wall=sorted(r['spawned_client_wall_ms'] for r in clients)
            label=f'{campaign.parent.name}/{campaign.name}' if campaign.name=='campaign' else campaign.name
            row=dict(campaign=label,**c,repeats=len(runs),verified_answers=len(samples),
                service_cpu_ms=st.median(service),service_min_ms=min(service),service_max_ms=max(service),
                full_campaign_server_cpu_ms=st.median(full_cpu),
                full_campaign_server_cpu_per_answer_ms=st.median(cpu/sum(len(client['samples']) for client in run['clients']) for cpu,run in zip(full_cpu,runs)),
                native_cpu_outside_request_phases_ms=st.median(native_unattributed),
                server_client_setup_ms=st.median(setup),global_publish_build_cpu_ms=st.median(global_cpu),
                server_online_cpu_ms=st.median(sum(client['server_phase_cpu_ms'].get('online',0)+client['gateway_cpu_ms'].get('online',0)
                    for client in run['clients'])/sum(len(client['samples']) for client in run['clients']) for run in runs),
                external_canonical_fixture_cpu_ms=external_fixture_cpu,
                logical_index_bytes=runs[0]['logical_index_bytes'],metadata_state_bytes=max(r['client_state_bytes'] for r in clients),
                client_setup_cpu_ms=st.median(r['client_setup_cpu_ms'] for r in clients),
                client_online_cpu_ms=st.median(s['client_online_cpu_ms'] for s in samples),
                complete_client_process_cpu_ms=st.median(r['client_process']['cpu_ms']+sum(s.get('canonical_verifier_cpu_ms',0) for s in r['samples']) for r in clients),
                setup_download_bytes=max(r['setup_wire'][1] for r in clients),
                query_upload_bytes=max(s['wire'][0] for s in samples),query_download_bytes=max(s['wire'][1] for s in samples),
                p50_first_answer_wall_ms=st.median(wall),p95_first_answer_wall_ms=wall[min(len(wall)-1,int(.95*len(wall)))],
                max_client_rss_bytes=max(r['client_process']['peak_rss_bytes'] for r in clients) if fresh_rss else None,
                rss_qualified=fresh_rss,budget_failures=';'.join(failures),
                median_private_reads=st.median(s['private_reads'] for s in samples))
            for generation in (1,16,256,4096):row[f'projected_G{generation}_cpu_ms']=row['service_cpu_ms']+row['global_publish_build_cpu_ms']/generation
            rows.append(row)
    a.output.parent.mkdir(parents=True,exist_ok=True)
    a.output.with_suffix('.json').write_text(json.dumps(rows,indent=2))
    keys=list(dict.fromkeys(k for row in rows for k in row))
    with a.output.with_suffix('.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=keys);writer.writeheader();writer.writerows(rows)
    print(f'{len(rows)} configurations, {sum(r["verified_answers"] for r in rows)} verified answers')


if __name__=='__main__':main()
