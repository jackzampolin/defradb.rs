use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use pir_poc::benchmark::Profile;
use pir_poc::snapshot::{records_from_json, Snapshot, SnapshotCatalog, SnapshotConfig};

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.first().map(String::as_str) {
        Some("demo") => print_json(&pir_poc::demo::run().await?),
        Some("singlepass-demo") => print_json(&pir_poc::demo::run_single_pass().await?),
        Some("bench") => {
            let profile = profile(args.get(1))?;
            print_json(&pir_poc::benchmark::run(profile)?)
        }
        Some("bench-opt") => {
            let profile = profile(args.get(1))?;
            print_json(&pir_poc::benchmark::run_optimizations(profile)?)
        }
        Some("bench-singlepass") => {
            let profile = profile(args.get(1))?;
            print_json(&pir_poc::benchmark::run_single_pass(profile)?)
        }
        Some("bench-warm-stateful") => {
            let profile = profile(args.get(1))?;
            print_json(&pir_poc::benchmark::run_warm_stateful(profile)?)
        }
        Some("bench-cold") => {
            let profile = profile(args.get(1))?;
            print_json(&pir_poc::benchmark::run_cold(profile)?)
        }
        Some("bench-dense-batch") => {
            let profile = profile(args.get(1))?;
            print_json(&pir_poc::benchmark::run_dense_batch(profile)?)
        }
        Some("bench-endpoints") => {
            let profile = profile(args.get(1))?;
            print_json(&pir_poc::benchmark::run_endpoints(profile).await?)
        }
        Some("bench-end-to-end") => {
            let profile = profile(args.get(1))?;
            print_json(&pir_poc::benchmark::run_end_to_end(profile)?)
        }
        Some("bench-fuse") => {
            let profile = profile(args.get(1))?;
            print_json(&pir_poc::benchmark::run_fuse(profile)?)
        }
        Some("bench-mphf") => {
            let profile = profile(args.get(1))?;
            print_json(&pir_poc::benchmark::run_mphf(profile)?)
        }
        Some("bench-mphf-subset-xor") => {
            let profile = profile(args.get(1))?;
            print_json(&pir_poc::benchmark::run_mphf_subset_xor(profile)?)
        }
        Some("bench-production-scale") => {
            let profile = profile(args.get(1))?;
            print_json(&pir_poc::benchmark::run_production_scale(
                profile,
                &args[2..],
            )?)
        }
        Some("bench-ribbon") => {
            let profile = profile(args.get(1))?;
            print_json(&pir_poc::benchmark::run_ribbon(profile)?)
        }
        Some("bench-subset-xor") => {
            let profile = profile(args.get(1))?;
            print_json(&pir_poc::benchmark::run_subset_xor(profile)?)
        }
        Some("subscription-demo") => print_json(&pir_poc::subscription::demo().await?),
        Some("bench-subscriptions") => {
            let profile = profile(args.get(1))?;
            print_json(&pir_poc::subscription::benchmark(profile)?)
        }
        Some("bench-subscription-batches") => {
            let profile = profile(args.get(1))?;
            print_json(&pir_poc::subscription::benchmark_batches(profile)?)
        }
        Some("build") => build(&args[1..]),
        Some("serve") => serve(&args[1..]).await,
        Some("query") => query(&args[1..]).await,
        Some("query-window") => query_window(&args[1..]).await,
        _ => {
            usage();
            Ok(())
        }
    }
}

fn build(args: &[String]) -> Result<()> {
    if args.len() != 5 {
        bail!("build requires INPUT OUTPUT COLLECTION KEY_FIELD VALUE_FIELD");
    }
    let input: serde_json::Value = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    let root = input.get("data").unwrap_or(&input);
    let records = records_from_json(root, Some(&args[2]), &args[3], &args[4])?;
    let bucket_count = (records.len().saturating_mul(2))
        .next_power_of_two()
        .max(16);
    let snapshot = Snapshot::build_paged(
        records,
        SnapshotConfig {
            bucket_count,
            bucket_capacity: 8,
            values_per_page: 4,
            max_key_bytes: 128,
            max_value_bytes: 1024,
            source: format!("{}.{}->{}", args[2], args[3], args[4]),
            source_cutoff: "manual-export".into(),
        },
    )?;
    snapshot.save(Path::new(&args[1]))?;
    print_json(&snapshot.manifest)
}

async fn serve(args: &[String]) -> Result<()> {
    if args.len() != 2 {
        bail!("serve requires SNAPSHOT_OR_CATALOG_DIR BIND_ADDRESS");
    }
    let catalog = Arc::new(SnapshotCatalog::load(&PathBuf::from(&args[0]))?);
    pir_poc::http::serve_catalog(catalog, &args[1]).await
}

async fn query_window(args: &[String]) -> Result<()> {
    if args.len() < 4 {
        bail!("query-window requires KEY WINDOW[,WINDOW...] SERVER [SERVER ...]");
    }
    let windows = args[1]
        .split(',')
        .filter(|window| !window.is_empty())
        .collect::<Vec<_>>();
    if windows.is_empty() {
        bail!("query-window requires at least one public window");
    }
    let client = pir_poc::http::PirClient::connect(&args[2..]).await?;
    let results = client
        .private_lookup_windows(args[0].as_bytes(), &windows)
        .await?;
    let rendered = results
        .into_iter()
        .map(|result| {
            let values = result
                .values
                .into_iter()
                .map(|value| {
                    String::from_utf8(value.clone()).unwrap_or_else(|_| hex::encode(value))
                })
                .collect::<Vec<_>>();
            (result.window_id, values)
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    print_json(&rendered)
}

async fn query(args: &[String]) -> Result<()> {
    if args.len() < 3 {
        bail!("query requires KEY SERVER [SERVER ...]");
    }
    let client = pir_poc::http::PirClient::connect(&args[1..]).await?;
    let values = client.private_lookup(args[0].as_bytes()).await?;
    let rendered = values
        .into_iter()
        .map(|value| String::from_utf8(value.clone()).unwrap_or_else(|_| hex::encode(value)))
        .collect::<Vec<_>>();
    print_json(&rendered)
}

fn profile(value: Option<&String>) -> Result<Profile> {
    match value.map(String::as_str) {
        None | Some("quick") => Ok(Profile::Quick),
        Some("full") => Ok(Profile::Full),
        Some(value) => bail!("unknown benchmark profile {value:?}; expected quick or full"),
    }
}

fn print_json(value: &impl serde::Serialize) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).context("serialize report")?
    );
    Ok(())
}

fn usage() {
    eprintln!(
        "pir-poc commands:\n  demo\n  singlepass-demo\n  subscription-demo\n  bench [quick|full]\n  bench-opt [quick|full]\n  bench-cold [quick|full]\n  bench-dense-batch [quick|full]\n  bench-endpoints [quick|full]\n  bench-end-to-end [quick|full]\n  bench-fuse [quick|full]\n  bench-mphf [quick|full]\n  bench-mphf-subset-xor [quick|full]\n  bench-production-scale [quick|full] [preflight|execute] [PAGES] [ROW_BYTES_CSV] [MAX_BUILD_MIB] [MAX_TABLE_MIB] [MAX_TIMED_WORK_MIB]\n  bench-ribbon [quick|full]\n  bench-subset-xor [quick|full]\n  bench-singlepass [quick|full]\n  bench-warm-stateful [quick|full]\n  bench-subscriptions [quick|full]\n  bench-subscription-batches [quick|full]\n  build INPUT OUTPUT COLLECTION KEY_FIELD VALUE_FIELD\n  serve SNAPSHOT_OR_CATALOG_DIR BIND_ADDRESS\n  query KEY SERVER [SERVER ...]\n  query-window KEY WINDOW[,WINDOW...] SERVER [SERVER ...]"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_profile_rejects_unknown_values() {
        let quick = "quick".to_owned();
        let full = "full".to_owned();
        let typo = "typo".to_owned();
        assert!(matches!(profile(None), Ok(Profile::Quick)));
        assert!(matches!(profile(Some(&quick)), Ok(Profile::Quick)));
        assert!(matches!(profile(Some(&full)), Ok(Profile::Full)));
        assert!(profile(Some(&typo)).is_err());
    }
}
