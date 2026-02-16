use std::path::PathBuf;
use std::process::Command;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("failed to canonicalize workspace root")
}

fn defra_binary() -> PathBuf {
    workspace_root().join("target/debug/defra")
}

#[test]
#[ignore] // Run with: cargo test -p integration-test --test sdl_generate -- --ignored
fn sdl_generate_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let input_path = tmp.path().join("input.graphql");
    let output_path = tmp.path().join("output.graphql");

    // Write input SDL
    std::fs::write(&input_path, "type User { name: String  age: Int }").unwrap();

    // Run sdl generate
    let output = Command::new(defra_binary())
        .args([
            "sdl",
            "generate",
            input_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
        ])
        .output()
        .expect("failed to run defra binary");

    assert!(
        output.status.success(),
        "sdl generate failed: stderr={}, stdout={}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout),
    );

    // Verify output file exists and contains expected types
    let generated = std::fs::read_to_string(&output_path).unwrap();
    assert!(generated.contains("type User"), "should contain User type");
    assert!(
        generated.contains("type Query"),
        "should contain Query type"
    );
    assert!(
        generated.contains("type Mutation"),
        "should contain Mutation type"
    );
}

#[test]
#[ignore]
fn sdl_generate_stdout() {
    let tmp = tempfile::tempdir().unwrap();
    let input_path = tmp.path().join("input.graphql");

    std::fs::write(&input_path, "type Book { title: String  author: String }").unwrap();

    // Output to stdout with -o -
    let output = Command::new(defra_binary())
        .args(["sdl", "generate", input_path.to_str().unwrap(), "-o", "-"])
        .output()
        .expect("failed to run defra binary");

    assert!(
        output.status.success(),
        "sdl generate stdout failed: stderr={}",
        String::from_utf8_lossy(&output.stderr),
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("type Book"),
        "stdout should contain Book type"
    );
}

#[test]
#[ignore]
fn sdl_generate_no_overwrite() {
    let tmp = tempfile::tempdir().unwrap();
    let input_path = tmp.path().join("input.graphql");
    let output_path = tmp.path().join("existing.graphql");

    std::fs::write(&input_path, "type Foo { bar: String }").unwrap();
    std::fs::write(&output_path, "existing content").unwrap();

    // Should fail without --overwrite
    let output = Command::new(defra_binary())
        .args([
            "sdl",
            "generate",
            input_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
        ])
        .output()
        .expect("failed to run defra binary");

    assert!(
        !output.status.success(),
        "should fail when output file exists without --overwrite"
    );

    // Verify original file unchanged
    let content = std::fs::read_to_string(&output_path).unwrap();
    assert_eq!(content, "existing content");
}
