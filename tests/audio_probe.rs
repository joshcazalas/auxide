use std::process::Command;

#[test]
fn the_probe_reaches_source_validation_without_a_discord_token() {
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("config.toml");
    let absent_token = directory.path().join("absent-token");
    std::fs::write(
        &config,
        format!(
            "[discord]\ntoken_file = {:?}\n",
            absent_token.display().to_string()
        ),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_auxide"))
        .arg("--config")
        .arg(config)
        .args(["youtube-probe", "https://example.com/not-youtube"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unsupported YouTube host"));
    assert!(!absent_token.exists());
}

#[test]
fn an_empty_probe_is_refused_before_loading_configuration() {
    let output = Command::new(env!("CARGO_BIN_EXE_auxide"))
        .args([
            "youtube-probe",
            "https://www.youtube.com/watch?v=hLOheGDwD_0",
            "--packets",
            "0",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid value '0'"));
}
