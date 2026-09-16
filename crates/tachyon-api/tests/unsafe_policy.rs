#![forbid(unsafe_code)]

#[test]
fn workspace_target_roots_forbid_unsafe_code() {
    let output = std::process::Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--offline",
            "--locked",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("run cargo metadata");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let members = metadata["workspace_members"].as_array().unwrap();
    let mut checked = 0;
    for package in metadata["packages"].as_array().unwrap() {
        if !members.contains(&package["id"]) {
            continue;
        }
        for target in package["targets"].as_array().unwrap() {
            let path = target["src_path"].as_str().unwrap();
            let source = std::fs::read_to_string(path).expect("read workspace target root");
            // Require a real leading attribute, not a match in a comment/string.
            // Cargo discovers custom roots, bins, tests, examples and build scripts.
            assert_eq!(
                source.lines().next(),
                Some("#![forbid(unsafe_code)]"),
                "{path} must start with #![forbid(unsafe_code)]"
            );
            checked += 1;
        }
    }
    assert!(checked > 0, "no workspace targets checked");
}
