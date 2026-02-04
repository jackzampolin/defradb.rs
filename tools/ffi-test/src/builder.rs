use std::path::Path;
use tokio::process::Command;

use crate::config::{CBINDGEN_CONFIG, FFI_LIB_NAME, HEADER_DESTINATION};
use crate::error::{FfiTestError, Result};
use crate::worktree::WorktreeContext;

/// Build the FFI library and generate the C header
pub async fn build_ffi(ctx: &WorktreeContext, verbose: bool) -> Result<()> {
    // Build the FFI crate
    build_ffi_crate(&ctx.rust_path, verbose).await?;

    // Generate and copy the header
    generate_header(ctx, verbose).await?;

    Ok(())
}

/// Build the FFI crate in release mode
async fn build_ffi_crate(rust_path: &Path, verbose: bool) -> Result<()> {
    if verbose {
        println!("Building FFI crate...");
    }

    let mut cmd = Command::new("cargo");
    cmd.args(["build", "--release", "-p", FFI_LIB_NAME])
        .current_dir(rust_path);

    let output = cmd.output().await?;

    if !output.status.success() {
        return Err(FfiTestError::FfiBuild(
            String::from_utf8_lossy(&output.stderr).to_string(),
        ));
    }

    if verbose {
        println!("FFI crate built successfully");
    }

    Ok(())
}

/// Generate the C header using cbindgen and copy to Go worktree
async fn generate_header(ctx: &WorktreeContext, verbose: bool) -> Result<()> {
    // Check if cbindgen is available
    check_cbindgen().await?;

    if verbose {
        println!("Generating C header...");
    }

    let config_path = ctx.rust_path.join(CBINDGEN_CONFIG);
    let header_dest = ctx.go_path.join(HEADER_DESTINATION);

    // Ensure destination directory exists
    if let Some(parent) = header_dest.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let output = Command::new("cbindgen")
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "--crate",
            FFI_LIB_NAME,
            "--output",
            header_dest.to_str().unwrap(),
        ])
        .current_dir(&ctx.rust_path)
        .output()
        .await?;

    if !output.status.success() {
        return Err(FfiTestError::HeaderGeneration(
            String::from_utf8_lossy(&output.stderr).to_string(),
        ));
    }

    if verbose {
        println!("Header generated at: {}", header_dest.display());
    }

    Ok(())
}

/// Check if cbindgen is installed
async fn check_cbindgen() -> Result<()> {
    let output = Command::new("cbindgen").arg("--version").output().await;

    match output {
        Ok(o) if o.status.success() => Ok(()),
        _ => Err(FfiTestError::CbindgenNotFound),
    }
}
