//! Canonical witness research bridge. Uses the same verifier as the serving POC.
//! Fixture positions are sorted-value order, not a live Shieldd corpus.

#[cfg(target_os = "linux")]
fn main() -> anyhow::Result<()> {
    linux::run()
}

#[cfg(not(target_os = "linux"))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("cold-canonical requires Linux process CPU accounting (use WSL on Windows)")
}

#[cfg(target_os = "linux")]
mod linux {
    use anyhow::{bail, ensure, Context, Result};
    use pir_poc::verification;
    use poseidon377::Fq;
    use std::time::Instant;

    fn cpu_ms() -> Result<f64> {
        let mut time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: time is a valid writable timespec; this clock has no other preconditions.
        ensure!(
            unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut time) } == 0,
            "process CPU clock failed"
        );
        Ok(time.tv_sec as f64 * 1000.0 + time.tv_nsec as f64 / 1e6)
    }

    pub fn run() -> Result<()> {
        let args: Vec<String> = std::env::args().skip(1).collect();
        match args.as_slice() {
            [mode, count, output] if mode == "build" => {
                let n: u64 = count.parse().context("invalid fixture size")?;
                ensure!(
                    (1..=262_144).contains(&n),
                    "fixture size must be 1..=262144"
                );
                let values: Vec<_> = (0..n).map(|i| Fq::from(i * 1009 + 17).to_bytes()).collect();
                let start = Instant::now();
                let cpu_start = cpu_ms()?;
                let (root, witnesses) = verification::build_benchmark_witnesses(&values, true)?;
                let data: Vec<_> = witnesses
                    .into_iter()
                    .map(|(position, witness)| {
                        let key = u64::from_le_bytes(
                            witness[8..16].try_into().expect("fixed witness field"),
                        );
                        serde_json::json!([key, position, position % 4, hex::encode(witness)])
                    })
                    .collect();
                std::fs::write(
                    output,
                    serde_json::to_vec(&serde_json::json!({
                        "root": hex::encode(root),
                        "data": data,
                        "build_wall_ms": start.elapsed().as_secs_f64() * 1000.0,
                        "build_cpu_ms": cpu_ms()? - cpu_start,
                        "qualification": "Poseidon depth-20 quaternary fixtures; sorted physical positions; not a production corpus"
                    }))?,
                )?;
            }
            [mode, key, root, witness] if mode == "verify" => {
                let key: u64 = key.parse().context("invalid query value")?;
                let root: [u8; 32] = hex::decode(root)?
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("root must contain 32 bytes"))?;
                verification::verify_nullifier_witness(
                    &Fq::from(key).to_bytes(),
                    &hex::decode(witness)?,
                    &root,
                )?;
                println!("{{\"correct\":true}}");
            }
            _ => bail!("usage: cold-canonical build N OUTPUT | verify KEY ROOT WITNESS_HEX"),
        }
        Ok(())
    }
}
