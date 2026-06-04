//! Auto-generated Lean conformance vectors.
//!
//! `proofs/lean/Conformance.lean` emits a JSON contract (vocabularies derived
//! from the Lean models) between two sentinel lines. We run it via
//! `lake env lean --run`, slice out the JSON, and deserialize it. Tests then
//! assert the live Rust types still match — so a rename in the code breaks the
//! proof's binding loudly. Mirrors defra-agent's `lean_vocab_test` pattern.

use anyhow::{bail, Context, Result};
use serde::de::DeserializeOwned;
use std::path::PathBuf;
use std::process::Command;

pub const BEGIN: &str = "---BEGIN DEFRA LEAN CONTRACT JSON---";
pub const END: &str = "---END DEFRA LEAN CONTRACT JSON---";

pub fn lean_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lean")
}

/// Run the Lean contract generator and return its full stdout.
pub fn run_generator() -> Result<String> {
    let dir = lean_dir();
    let out = Command::new("lake")
        .args(["env", "lean", "--run", "Conformance.lean"])
        .current_dir(&dir)
        .output()
        .with_context(|| format!("failed to spawn `lake` in {}", dir.display()))?;
    if !out.status.success() {
        bail!(
            "`lake env lean --run Conformance.lean` failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Slice the contract JSON out of the generator's stdout.
pub fn extract_json(stdout: &str) -> Result<&str> {
    let begin = stdout
        .find(BEGIN)
        .context("contract BEGIN marker missing")?
        + BEGIN.len();
    let end = stdout.find(END).context("contract END marker missing")?;
    if end < begin {
        bail!("contract END marker precedes BEGIN marker");
    }
    Ok(stdout[begin..end].trim())
}

/// Build, run, extract, and deserialize the Lean contract.
pub fn load_contract<T: DeserializeOwned>() -> Result<T> {
    let stdout = run_generator()?;
    let json = extract_json(&stdout)?;
    serde_json::from_str(json).with_context(|| format!("parsing contract json:\n{json}"))
}

#[derive(Debug, serde::Deserialize)]
pub struct ContractSnapshot {
    pub generated_by: String,
    pub vocabularies: Vec<Vocabulary>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Vocabulary {
    pub domain: String,
    pub values: Vec<String>,
}

impl ContractSnapshot {
    pub fn vocab(&self, domain: &str) -> Option<&Vocabulary> {
        self.vocabularies.iter().find(|v| v.domain == domain)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_json_slices_between_markers() {
        let stdout = format!("noise\n{BEGIN}\n{{\"k\":1}}\n{END}\ntrailing");
        assert_eq!(extract_json(&stdout).unwrap(), "{\"k\":1}");
    }

    #[test]
    fn extract_json_errors_without_markers() {
        assert!(extract_json("no markers here").is_err());
    }
}
