use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use pir_poc::selected::{TableUseCase, UseCaseBuildInput, UseCaseStore};
use pir_poc::selected_http::{serve_selected, ShinzoSubscription, UseCaseClient};
use pir_poc::subscription::SubscriptionId;
use pir_poc::verification::{decrypt_projection_values, verify_nullifier_witness};
use pir_poc::Profile;
use serde::Serialize;

const OPERATOR_KEY_ENV: &str = "PIR_POC_OPERATOR_KEY_HEX";
const PROJECTION_KEY_ENV: &str = "PIR_POC_PROJECTION_KEY_HEX";

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.first().map(String::as_str) {
        Some("demo") => print_json(&pir_poc::selected_demo::run().await?),
        Some("build") => build(&args[1..]),
        Some("serve") => serve(&args[1..]).await,
        Some("query") => query(&args[1..]).await,
        Some("bucket") => bucket(&args[1..]),
        Some("use-cases") => print_json(&pir_poc::use_case_gallery::run(
            args.get(1).map(String::as_str),
        )?),
        Some("encrypted-search") => {
            let rows = args
                .get(1)
                .map(|value| value.parse())
                .transpose()?
                .unwrap_or(1_000);
            print_json(&pir_poc::encrypted_search::benchmark(rows)?)
        }
        Some("benchmark") => {
            let profile = profile(args.get(1))?;
            print_json(&pir_poc::benchmark::run(profile)?)
        }
        #[cfg(feature = "research")]
        Some("research") => research(&args[1..]).await,
        None => {
            usage();
            Ok(())
        }
        Some(command) => {
            usage();
            bail!("unknown pir-poc command {command:?}")
        }
    }
}

fn bucket(args: &[String]) -> Result<()> {
    if args.len() < 3 || args.len() > 4 || args[0] != "shinzo" {
        bail!("bucket requires shinzo FIELD HEX_VALUE [BUCKET_COUNT]");
    }
    let field = match args[1].as_str() {
        "address" => pir_poc::shinzo::LOG_ADDRESS_FIELD,
        "topic0" => pir_poc::shinzo::LOG_TOPIC0_FIELD,
        other => bail!("unsupported Shinzo selector {other:?}; expected address or topic0"),
    };
    let bucket_count = args
        .get(3)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(pir_poc::shinzo::DEFAULT_BUCKET_COUNT);
    print_json(&BucketOutput {
        field,
        bucket_count,
        bucket: pir_poc::shinzo::ethereum_log_selector_bucket(field, &args[2], bucket_count)?,
    })
}

fn build(args: &[String]) -> Result<()> {
    if args.len() != 2 {
        bail!("build requires INPUT_JSON OUTPUT_ROOT");
    }
    let operator_key = operator_key()?;
    let input: UseCaseBuildInput = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    let output = PathBuf::from(&args[1]);
    if output.exists() {
        bail!("build output already exists; selected POC generations are immutable");
    }
    std::fs::create_dir_all(&output)?;
    let left = UseCaseStore::build(input.clone(), &operator_key, 0)?;
    let right = UseCaseStore::build(input, &operator_key, 1)?;
    left.save_immutable(&output.join("replica-0"), &operator_key, 0)?;
    right.save_immutable(&output.join("replica-1"), &operator_key, 1)?;
    print_json(&BuildOutput {
        body_digest_hex: hex::encode(left.manifest.manifest.body_digest),
        replica_directories: vec![output.join("replica-0"), output.join("replica-1")],
        manifest: left.manifest,
    })
}

async fn serve(args: &[String]) -> Result<()> {
    if args.len() != 2 {
        bail!("serve requires REPLICA_STORE BIND_ADDRESS");
    }
    let operator_key = operator_key()?;
    let store = Arc::new(UseCaseStore::load(Path::new(&args[0]), &operator_key)?);
    serve_selected(store, &args[1]).await
}

async fn query(args: &[String]) -> Result<()> {
    match args.first().map(String::as_str) {
        Some("strict") => strict_query(&args[1..]).await,
        Some("decoy") => decoy_query(&args[1..]).await,
        Some("shinzo") => shinzo_query(&args[1..]).await,
        Some("shinzo-register") => shinzo_register(&args[1..]).await,
        Some("shinzo-poll") => shinzo_poll(&args[1..]).await,
        _ => bail!("query requires strict, decoy, shinzo, shinzo-register, or shinzo-poll mode"),
    }
}

async fn strict_query(args: &[String]) -> Result<()> {
    if args.len() < 4 {
        bail!("query strict requires USE_CASE KEY SERVER SERVER [SERVER ...]");
    }
    let use_case = table_use_case(&args[0])?;
    let key = lookup_key(use_case, &args[1])?;
    let operator_key = operator_key()?;
    let client = UseCaseClient::connect(&args[2..], &operator_key).await?;
    let values = match use_case {
        TableUseCase::Nullifier => {
            let nullifier: [u8; 32] = key.as_slice().try_into().expect("validated nullifier");
            client
                .verified_nullifier_lookup(&nullifier)
                .await?
                .map(|witness| vec![witness])
        }
        TableUseCase::EncryptedTag => client.verified_tag_lookup(&key, &projection_key()?).await?,
    };
    print_json(&LookupOutput {
        mode: "strict",
        present: values.is_some(),
        values_base64: values
            .unwrap_or_default()
            .into_iter()
            .map(|value| STANDARD.encode(value))
            .collect(),
        returned_rows: 1,
        processed_rows: 1,
        ignored_without_decoding: 0,
    })
}

async fn decoy_query(args: &[String]) -> Result<()> {
    if args.len() != 4 {
        bail!("query decoy requires USE_CASE TARGET_KEY CANDIDATE_JSON SERVER");
    }
    let use_case = table_use_case(&args[0])?;
    let target = lookup_key(use_case, &args[1])?;
    let encoded_candidates: Vec<String> = serde_json::from_slice(&std::fs::read(&args[2])?)?;
    let candidates = encoded_candidates
        .iter()
        .map(|candidate| lookup_key(use_case, candidate))
        .collect::<Result<Vec<_>>>()?;
    let operator_key = operator_key()?;
    let client = UseCaseClient::connect_decoy(&args[3], &operator_key).await?;
    let result = client.decoy_lookup(use_case, &target, &candidates).await?;
    let verified_values = match (use_case, result.values) {
        (TableUseCase::Nullifier, Some(values)) => {
            if values.len() != 1 {
                bail!("nullifier decoy query returned a non-canonical result count");
            }
            let nullifier: [u8; 32] = target.as_slice().try_into().expect("validated nullifier");
            verify_nullifier_witness(
                &nullifier,
                &values[0],
                &client
                    .metadata
                    .manifest
                    .manifest
                    .active_generation
                    .manifest
                    .root,
            )?;
            Some(values)
        }
        (TableUseCase::EncryptedTag, Some(values)) => {
            let generation = &client.metadata.manifest.manifest.active_generation.manifest;
            Some(decrypt_projection_values(
                &projection_key()?,
                generation.height,
                &generation.root,
                &target,
                &values,
            )?)
        }
        (_, None) => None,
    };
    print_json(&LookupOutput {
        mode: "decoy",
        present: verified_values.is_some(),
        values_base64: verified_values
            .unwrap_or_default()
            .into_iter()
            .map(|value| STANDARD.encode(value))
            .collect(),
        returned_rows: result.returned_rows,
        processed_rows: result.processed_rows,
        ignored_without_decoding: result.ignored_without_decoding,
    })
}

async fn shinzo_query(args: &[String]) -> Result<()> {
    if args.len() != 4 {
        bail!("query shinzo requires TARGET_BUCKET EVENT_BUCKET SERVER_0 SERVER_1");
    }
    let target_bucket = args[0].parse::<usize>()?;
    let event_bucket = args[1].parse::<usize>()?;
    let operator_key = operator_key()?;
    let client = UseCaseClient::connect(&args[2..], &operator_key).await?;
    print_json(&ShinzoOutput {
        matched: client
            .subscribe_and_evaluate(target_bucket, event_bucket)
            .await?,
    })
}

async fn shinzo_register(args: &[String]) -> Result<()> {
    if args.len() != 3 {
        bail!("query shinzo-register requires TARGET_BUCKET SERVER_0 SERVER_1");
    }
    let target_bucket = args[0].parse::<usize>()?;
    let client = UseCaseClient::connect(&args[1..], &operator_key()?).await?;
    let subscription = client.register_shinzo_subscription(target_bucket).await?;
    print_json(&ShinzoRegistrationOutput {
        subscription_id_hex: subscription.id.to_string(),
        cursor: subscription.cursor,
    })
}

async fn shinzo_poll(args: &[String]) -> Result<()> {
    if args.len() != 4 {
        bail!("query shinzo-poll requires SUBSCRIPTION_ID AFTER_CURSOR SERVER_0 SERVER_1");
    }
    let id: [u8; 16] = hex::decode(&args[0])?
        .try_into()
        .map_err(|_| anyhow::anyhow!("subscription ID must be exactly 16 bytes"))?;
    let mut subscription = ShinzoSubscription {
        id: SubscriptionId::from_bytes(id),
        cursor: args[1].parse::<u64>()?,
    };
    let client = UseCaseClient::connect(&args[2..], &operator_key()?).await?;
    let notifications = client
        .poll_shinzo_subscription(&mut subscription, 256)
        .await?;
    print_json(&ShinzoPollOutput {
        subscription_id_hex: subscription.id.to_string(),
        cursor: subscription.cursor,
        notifications,
    })
}

fn table_use_case(value: &str) -> Result<TableUseCase> {
    match value {
        "nullifier" => Ok(TableUseCase::Nullifier),
        "tag" => Ok(TableUseCase::EncryptedTag),
        _ => bail!("unknown table use case {value:?}; expected nullifier or tag"),
    }
}

fn lookup_key(use_case: TableUseCase, encoded: &str) -> Result<Vec<u8>> {
    match use_case {
        TableUseCase::Nullifier => {
            let value = hex::decode(encoded).context("decode nullifier hex")?;
            if value.len() != 32 {
                bail!("nullifier must be exactly 32 bytes");
            }
            Ok(value)
        }
        TableUseCase::EncryptedTag => STANDARD.decode(encoded).context("decode tag Base64"),
    }
}

fn operator_key() -> Result<[u8; 32]> {
    let value = std::env::var(OPERATOR_KEY_ENV)
        .with_context(|| format!("{OPERATOR_KEY_ENV} must contain a 32-byte hex key"))?;
    hex::decode(value)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("{OPERATOR_KEY_ENV} must contain exactly 32 bytes"))
}

fn projection_key() -> Result<[u8; 32]> {
    let value = std::env::var(PROJECTION_KEY_ENV)
        .with_context(|| format!("{PROJECTION_KEY_ENV} must contain a 32-byte hex key"))?;
    hex::decode(value)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("{PROJECTION_KEY_ENV} must contain exactly 32 bytes"))
}

fn profile(value: Option<&String>) -> Result<Profile> {
    match value.map(String::as_str) {
        None | Some("quick") => Ok(Profile::Quick),
        Some("full") => Ok(Profile::Full),
        Some(value) => bail!("unknown benchmark profile {value:?}; expected quick or full"),
    }
}

#[cfg(feature = "research")]
async fn research(args: &[String]) -> Result<()> {
    let profile = profile(args.get(1))?;
    match args.first().map(String::as_str) {
        Some("active-nullifier") => print_json(&pir_poc::benchmark::run_active_nullifier(profile)?),
        Some("billion-tag") => print_json(&pir_poc::benchmark::run_billion_tag(profile)?),
        Some("cold") => print_json(&pir_poc::benchmark::run_cold(profile)?),
        Some("cpu-snapshot") => print_json(&pir_poc::benchmark::run_cpu_snapshot(profile)?),
        Some("dense-batch") => print_json(&pir_poc::benchmark::run_dense_batch(profile)?),
        Some("defra-events") => print_json(&pir_poc::subscription::demo().await?),
        Some("end-to-end") => print_json(&pir_poc::benchmark::run_end_to_end(profile)?),
        Some("endpoints") => print_json(&pir_poc::benchmark::run_endpoints(profile).await?),
        Some("fuse") => print_json(&pir_poc::benchmark::run_fuse(profile)?),
        Some("gpu-reference-decoy") => {
            print_json(&pir_poc::benchmark::run_gpu_reference_decoy(profile)?)
        }
        Some("mphf") => print_json(&pir_poc::benchmark::run_mphf(profile)?),
        Some("mphf-subset-xor") => print_json(&pir_poc::benchmark::run_mphf_subset_xor(profile)?),
        Some("optimization") => print_json(&pir_poc::benchmark::run_optimizations(profile)?),
        Some("ribbon") => print_json(&pir_poc::benchmark::run_ribbon(profile)?),
        Some("single-pass") => print_json(&pir_poc::benchmark::run_single_pass(profile)?),
        Some("subset-xor") => print_json(&pir_poc::benchmark::run_subset_xor(profile)?),
        Some("warm-stateful") => print_json(&pir_poc::benchmark::run_warm_stateful(profile)?),
        _ => bail!("unknown research benchmark"),
    }
}

fn print_json(value: &impl Serialize) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[derive(Serialize)]
struct BuildOutput {
    body_digest_hex: String,
    replica_directories: Vec<PathBuf>,
    manifest: pir_poc::selected::AuthenticatedUseCaseManifest,
}

#[derive(Serialize)]
struct LookupOutput {
    mode: &'static str,
    present: bool,
    values_base64: Vec<String>,
    returned_rows: usize,
    processed_rows: usize,
    ignored_without_decoding: usize,
}

#[derive(Serialize)]
struct ShinzoOutput {
    matched: bool,
}

#[derive(Serialize)]
struct ShinzoRegistrationOutput {
    subscription_id_hex: String,
    cursor: u64,
}

#[derive(Serialize)]
struct ShinzoPollOutput {
    subscription_id_hex: String,
    cursor: u64,
    notifications: Vec<pir_poc::selected_http::ShinzoNotification>,
}

#[derive(Serialize)]
struct BucketOutput {
    field: &'static str,
    bucket_count: usize,
    bucket: usize,
}

fn usage() {
    eprintln!(
        "pir-poc commands:\n  demo\n  use-cases [mizu|shinzo|defra]\n  encrypted-search [ROWS<=1000000]\n  build INPUT_JSON OUTPUT_ROOT\n  serve REPLICA_STORE BIND_ADDRESS\n  bucket shinzo address|topic0 HEX_VALUE [BUCKET_COUNT]\n  query strict nullifier NULLIFIER_HEX SERVER SERVER [SERVER ...]\n  query strict tag TAG_BASE64 SERVER SERVER [SERVER ...]\n  query decoy nullifier NULLIFIER_HEX CANDIDATE_JSON SERVER\n  query decoy tag TAG_BASE64 CANDIDATE_JSON SERVER\n  query shinzo TARGET_BUCKET EVENT_BUCKET SERVER_0 SERVER_1\n  query shinzo-register TARGET_BUCKET SERVER_0 SERVER_1\n  query shinzo-poll SUBSCRIPTION_ID AFTER_CURSOR SERVER_0 SERVER_1\n  benchmark [quick|full]\n\nBuild, serve, and query require {OPERATOR_KEY_ENV}=64_HEX_CHARS. Tag queries additionally require {PROJECTION_KEY_ENV}=64_HEX_CHARS. Historical experiments require --features research."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_and_key_parsing_reject_ambiguous_input() {
        let typo = "typo".to_owned();
        assert!(profile(Some(&typo)).is_err());
        assert!(lookup_key(TableUseCase::Nullifier, "00").is_err());
        assert!(table_use_case("other").is_err());
    }
}
