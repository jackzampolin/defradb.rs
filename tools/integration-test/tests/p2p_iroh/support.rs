use std::path::PathBuf;

use integration_test::{build_cli_variant, workspace_root};

#[ctor::ctor]
fn prepare_iroh_binary() {
    if std::env::var_os("DEFRA_IROH_BINARY")
        .zip(std::env::var_os("DEFRA_RUST_BINARY"))
        .is_some_and(|(iroh, rust)| PathBuf::from(iroh).is_file() && PathBuf::from(rust).is_file())
    {
        return;
    }

    let binary = build_cli_variant(&workspace_root(), &["iroh"], "defra-iroh");

    std::env::set_var("DEFRA_IROH_BINARY", &binary);
    std::env::set_var("DEFRA_RUST_BINARY", &binary);
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
