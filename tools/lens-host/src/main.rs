use std::error::Error;
use std::io::{Error as IoError, ErrorKind};
use std::path::PathBuf;

use lens::{LensConfig, TransformStore, WasmTransformStore};
use tokio::io::AsyncReadExt;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let config_path = config_path()?;
    let config_bytes = tokio::fs::read(config_path).await?;
    let config: LensConfig = serde_json::from_slice(&config_bytes)?;

    let input = read_stdin_json_array().await?;
    validate_input_values(&input)?;

    if config.lenses.is_empty() {
        write_json_array(input)?;
        return Ok(());
    }

    let store = WasmTransformStore::new()?;
    let transform_id = store.add(config).await?;
    let output = store.transform_json(&transform_id, input)?;

    write_json_array(output)?;
    Ok(())
}

fn config_path() -> Result<PathBuf> {
    let mut args = std::env::args_os();
    let _program = args.next();
    let path = args.next().ok_or_else(|| {
        IoError::new(ErrorKind::InvalidInput, "missing lens config path argument")
    })?;

    Ok(path.into())
}

async fn read_stdin_json_array() -> Result<Vec<serde_json::Value>> {
    let mut input_bytes = Vec::new();
    tokio::io::stdin().read_to_end(&mut input_bytes).await?;

    match serde_json::from_slice::<serde_json::Value>(&input_bytes)? {
        serde_json::Value::Array(values) => Ok(values),
        _ => Err(IoError::new(ErrorKind::InvalidInput, "stdin must be a JSON array").into()),
    }
}

fn validate_input_values(values: &[serde_json::Value]) -> Result<()> {
    for value in values {
        if !matches!(
            value,
            serde_json::Value::Object(_) | serde_json::Value::Null
        ) {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "input array elements must be JSON objects",
            )
            .into());
        }
    }
    Ok(())
}

fn write_json_array(output: Vec<serde_json::Value>) -> Result<()> {
    let stdout = std::io::stdout();
    serde_json::to_writer(stdout.lock(), &output)?;
    Ok(())
}
