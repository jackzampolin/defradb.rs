use anyhow::{bail, Result};
use pir_poc::benchmark::cross_language;
use pir_poc::Profile;

fn main() -> Result<()> {
    let profile = match std::env::args().nth(1).as_deref() {
        None | Some("quick") => Profile::Quick,
        Some("full") => Profile::Full,
        Some(other) => bail!("unknown profile {other:?}; expected quick or full"),
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&cross_language::run(profile)?)?
    );
    Ok(())
}
