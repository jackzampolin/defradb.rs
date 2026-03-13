use std::fs;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=DEFRA_NODE_GIT_HASH_OVERRIDE");
    println!("cargo:rerun-if-env-changed=DEFRA_NODE_GIT_DIRTY_OVERRIDE");
    println!("cargo:rerun-if-env-changed=DEFRA_NODE_RELEASE_TAG_OVERRIDE");

    // Git commit hash
    let git_hash = std::env::var("DEFRA_NODE_GIT_HASH_OVERRIDE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            Command::new("git")
                .args(["rev-parse", "--short=12", "HEAD"])
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
        })
        .unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=DEFRA_NODE_GIT_HASH={}", git_hash.trim());

    // Git dirty flag
    let dirty = std::env::var("DEFRA_NODE_GIT_DIRTY_OVERRIDE")
        .ok()
        .unwrap_or_else(|| {
            let dirty = Command::new("git")
                .args(["status", "--porcelain", "--untracked-files=no"])
                .output()
                .ok()
                .map(|o| !o.stdout.is_empty())
                .unwrap_or(false);
            if dirty {
                "-dirty".into()
            } else {
                String::new()
            }
        });
    println!("cargo:rustc-env=DEFRA_NODE_GIT_DIRTY={}", dirty.trim());

    let release_tag = std::env::var("DEFRA_NODE_RELEASE_TAG_OVERRIDE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            Command::new("git")
                .args(["describe", "--tags", "--exact-match", "HEAD"])
                .output()
                .ok()
                .filter(|o| o.status.success())
                .and_then(|o| String::from_utf8(o.stdout).ok())
        })
        .unwrap_or_default();
    println!(
        "cargo:rustc-env=DEFRA_NODE_RELEASE_TAG={}",
        release_tag.trim()
    );

    // Build timestamp (UTC)
    let now = Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=DEFRA_NODE_BUILD_TIME={}", now.trim());

    // Target triple
    println!(
        "cargo:rustc-env=DEFRA_NODE_TARGET={}",
        std::env::var("TARGET").unwrap_or_else(|_| "unknown".into())
    );

    // Rustc version
    let rustc = Command::new("rustc")
        .args(["--version"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=DEFRA_NODE_RUSTC={}", rustc.trim());

    // Rebuild on git branch / index changes so version metadata tracks commits.
    let git_head = "../../.git/HEAD";
    println!("cargo:rerun-if-changed={git_head}");
    if let Ok(head_contents) = fs::read_to_string(git_head) {
        if let Some(reference) = head_contents.strip_prefix("ref: ") {
            println!("cargo:rerun-if-changed=../../.git/{}", reference.trim());
        }
    }
    println!("cargo:rerun-if-changed=../../.git/index");
}
