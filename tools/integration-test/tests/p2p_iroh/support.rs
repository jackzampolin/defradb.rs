use std::path::{Path, PathBuf};
use std::process::Command;

#[ctor::ctor]
fn prepare_iroh_binary() {
    if std::env::var_os("DEFRA_IROH_BINARY")
        .zip(std::env::var_os("DEFRA_RUST_BINARY"))
        .is_some_and(|(iroh, rust)| PathBuf::from(iroh).is_file() && PathBuf::from(rust).is_file())
    {
        return;
    }

    let workspace = workspace_root();
    let status = Command::new("cargo")
        .args(["build", "-p", "cli", "--features", "iroh"])
        .current_dir(&workspace)
        .status()
        .expect("failed to run cargo build for iroh defra binary");
    assert!(
        status.success(),
        "cargo build -p cli --features iroh failed"
    );

    let src = workspace
        .join("target/debug")
        .join(format!("defra{}", std::env::consts::EXE_SUFFIX));
    let dst = workspace
        .join("target/debug")
        .join(format!("defra-iroh{}", std::env::consts::EXE_SUFFIX));
    let tmp = workspace.join("target/debug").join(format!(
        "defra-iroh-{}{}",
        std::process::id(),
        std::env::consts::EXE_SUFFIX
    ));
    std::fs::copy(&src, &tmp).expect("failed to copy iroh defra binary");
    std::fs::rename(&tmp, &dst).unwrap_or_else(|_| {
        let _ = std::fs::remove_file(&dst);
        std::fs::rename(&tmp, &dst).expect("failed to replace iroh defra binary");
    });

    std::env::set_var("DEFRA_IROH_BINARY", &dst);
    std::env::set_var("DEFRA_RUST_BINARY", &dst);
}

pub fn sourcehub_binary_available() -> bool {
    std::env::var_os("SOURCEHUB_BINARY").is_some()
        || std::env::var_os("SOURCEHUB_WORKSPACE").is_some()
        || path_contains_binary("sourcehubd")
}

fn path_contains_binary(name: &str) -> bool {
    let binary = format!("{}{}", name, std::env::consts::EXE_SUFFIX);
    std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|path| path.join(&binary).is_file()))
        .unwrap_or(false)
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}
