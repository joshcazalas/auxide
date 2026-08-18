//! Standard output belongs to whichever subcommand is running.
//!
//! Three subcommands print tab-separated records for a script to read, and
//! `scripts/source-check.sh` reads exactly that every morning to notice when
//! `YouTube` changes the shape of its answers. When the log shared that stream,
//! a line of JSON arrived where the first track should have been and the probe
//! reported that extraction had drifted. It had not. Working out that the
//! canary was wrong rather than `YouTube` took a while, and the daily run would
//! have gone on saying it.
//!
//! That is a whole-binary property, so this is a whole-binary test: the unit
//! tests never run `main`, and the probe that would have caught it needs the
//! network and only runs once a day.

use std::process::Command;

/// Runs the built binary against a valid configuration and returns its streams.
fn run(subcommand: &str) -> (String, String, bool) {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let token = directory.path().join("token");
    std::fs::write(&token, "not-a-real-token").expect("a token file");
    let config = directory.path().join("config.toml");
    std::fs::write(
        &config,
        format!(
            "[discord]\ntoken_file = {:?}\n\n[observability]\nlisten_address = \"127.0.0.1:0\"\n",
            token.display().to_string()
        ),
    )
    .expect("a configuration file");

    let output = Command::new(env!("CARGO_BIN_EXE_auxide"))
        .args(["--config", &config.display().to_string(), subcommand])
        .output()
        .expect("the binary runs");
    (
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
        output.status.success(),
    )
}

#[test]
fn the_log_stays_off_standard_output() {
    // `check-config` answers by exiting zero and has nothing of its own to
    // print, so anything on standard output here came from the log — and
    // whatever the log would put here it would also put in front of a track.
    let (stdout, stderr, succeeded) = run("check-config");
    assert!(succeeded, "check-config failed: {stderr}");
    assert!(
        stdout.is_empty(),
        "the log reached standard output, where a subcommand's answer belongs:\n{stdout}"
    );
    // And it is still saying what it loaded, somewhere. A log that went quiet
    // would pass the assertion above for the wrong reason.
    assert!(
        stderr.contains("loaded configuration"),
        "the configuration was never logged at all:\n{stderr}"
    );
}
