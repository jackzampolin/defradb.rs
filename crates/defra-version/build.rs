use std::path::Path;
use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
}

/// Emitting a path that does not exist would leave the build script permanently dirty,
/// rebuilding every dependent crate on every build.
fn rerun_if_exists(path: &str) {
    if Path::new(path).exists() {
        println!("cargo:rerun-if-changed={path}");
    }
}

fn main() {
    // Without these the build script is cached and the constants below drift from the commit
    // actually built. `--git-path` is what resolves each file in a linked worktree, where HEAD
    // lives in the per-worktree git dir but refs live in the common one.
    //
    // refs/heads is watched as a directory: a ref that is packed at build time has no loose
    // file to watch, and a later commit writes it back out without touching packed-refs or the
    // contents of HEAD. It is shared, so a commit in a sibling worktree also rebuilds this one;
    // that is the price of staying correct across packed refs and branch switches.
    //
    // The reftable backend keeps refs outside refs/heads entirely, leaving HEAD a fixed stub.
    for entry in ["HEAD", "refs/heads", "packed-refs", "reftable"] {
        if let Some(path) = git(&["rev-parse", "--git-path", entry]) {
            rerun_if_exists(&path);
        }
    }

    // --git-path resolves reftable to the per-worktree stack, but a linked worktree writes its
    // branch refs to the common one, and only reflogs happen to touch the former.
    if let Some(dir) = git(&["rev-parse", "--git-common-dir"]) {
        rerun_if_exists(&format!("{dir}/reftable"));
    }

    let commit = git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    let date = git(&["show", "-s", "--date=short", "--format=%cd", "HEAD"])
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=GIT_COMMIT={commit}");
    println!("cargo:rustc-env=BUILD_DATE={date}");
}
