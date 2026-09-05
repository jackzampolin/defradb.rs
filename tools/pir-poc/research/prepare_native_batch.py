from pathlib import Path
import sys
source=Path('benchmarks/native_store.rs').read_text()
needle='            "read"=>'
addition='''            "batch-dense"=>{
                let selectors:Vec<_>=v.as_array().unwrap().iter().map(decode).collect();
                assert!(selectors.iter().all(|q|q.len()==(rows.len()+7)/8));
                let mut answers=vec![vec![0u8;rows[0].len()];selectors.len()];
                for (at,row) in rows.iter().enumerate(){
                    for (q,answer) in selectors.iter().zip(answers.iter_mut()){
                        if (q[at/8]>>(at%8))&1!=0 {for (a,b) in answer.iter_mut().zip(row){*a^=*b;}reads+=row.len();}
                    }
                }
                json!(answers.iter().map(|a|blob(a)).collect::<Vec<_>>())},
'''
if needle not in source:raise ValueError('source changed')
Path(sys.argv[1]).write_text(source.replace(needle,addition+needle))
