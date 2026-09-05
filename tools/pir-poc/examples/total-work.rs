//! Small research entry point: avoids linking unrelated service/benchmark paths.
use anyhow::{bail, Result};
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let path = match args.as_slice() {
        [path] => path,
        [research, mode, path] if research == "research" && mode == "total-work" => path,
        _ => bail!("usage: total-work CONFIG.json"),
    };
    let report = pir_poc::benchmark::total_work::run_file(std::path::Path::new(path))?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
