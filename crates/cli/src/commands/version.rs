use clap::Args;
use defra_version::VersionInfo;

use crate::error::Result;

/// Arguments for the version command.
#[derive(Args, Debug)]
pub struct VersionArgs {
    /// Version output format. Options are text, json
    #[arg(short, long, default_value = "text")]
    pub format: String,

    /// Display the full version information
    #[arg(long)]
    pub full: bool,
}

impl VersionArgs {
    pub fn execute(&self) -> Result<()> {
        let info = VersionInfo::new();

        if self.format.to_lowercase() == "json" {
            let json = serde_json::to_string_pretty(&info)?;
            println!("{json}");
        } else if self.full {
            println!("{}", info.full());
        } else {
            println!("{}", info.short());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_full() {
        let info = VersionInfo::new();
        let full = info.full();
        assert!(full.contains("defradb"));
        assert!(full.contains("Rust:"));
        assert!(full.contains("Go compat:"));
    }
}
