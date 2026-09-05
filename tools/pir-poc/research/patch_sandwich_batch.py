"""Add a bounded public batch window to the pinned research HTTP wrapper."""
from pathlib import Path
import sys
p=Path(sys.argv[1])/'pir_server/src/bin/serve.rs';s=p.read_text()
if 'COLD_BATCH_WINDOW_MS' in s:raise ValueError('already patched')
s=s.replace('    for stream in listener.incoming() {','''    let window: u64 = std::env::var("COLD_BATCH_WINDOW_MS").unwrap_or("0".into()).parse().unwrap();
    let (batch_tx, batch_rx) = std::sync::mpsc::channel::<(Vec<u8>, std::sync::mpsc::Sender<Result<Vec<u8>,String>>) >();
    let batch_server=Arc::clone(&server);
    std::thread::spawn(move || {
        while let Ok(first)=batch_rx.recv() {
            let deadline=std::time::Instant::now()+std::time::Duration::from_millis(window);
            let mut jobs=vec![first];
            while jobs.len()<64 {
                let now=std::time::Instant::now();
                if now>=deadline { break; }
                match batch_rx.recv_timeout(deadline-now) { Ok(job)=>jobs.push(job), Err(_)=>break }
            }
            let mut parsed=Vec::new();let mut replies=Vec::new();
            for (body,tx) in jobs {
                match batch_server.parse_query(&body) { Ok(q)=>{parsed.push(q);replies.push(tx);}, Err(e)=>{let _=tx.send(Err(e));} }
            }
            if !parsed.is_empty() {
                let refs:Vec<_>=parsed.iter().collect();
                let result=batch_server.answer_batch(&refs);
                info!("Cold batch size {} window {} ms",result.len(),window);
                for (tx,answer) in replies.into_iter().zip(result) {let _=tx.send(Ok(answer));}
            }
        }
    });
    for stream in listener.incoming() {''')
s=s.replace('                let web_dir = args.web_dir.clone();','                let web_dir = args.web_dir.clone();\n                let tx=batch_tx.clone();')
s=s.replace('handle(stream, &server, web_dir.as_deref())','handle(stream, &server, web_dir.as_deref(), &tx)')
s=s.replace('    web_dir: Option<&str>,','    web_dir: Option<&str>,\n    batch: &std::sync::mpsc::Sender<(Vec<u8>, std::sync::mpsc::Sender<Result<Vec<u8>,String>>)>,')
s=s.replace('            match server.answer(&body) {','''            let (tx,rx)=std::sync::mpsc::channel();
            batch.send((body,tx)).map_err(|e|e.to_string())?;
            match rx.recv().map_err(|e|e.to_string())? {''')
p.write_text(s)
