#![forbid(unsafe_code)]
#![cfg(target_os = "linux")]
use std::process::Stdio;
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn actual_binary_rejects_malformed_and_missing_bootstrap() {
    for mode in ["oversized", "missing", "invalid-json", "unknown-field"] {
        let root = tempfile::tempdir().unwrap();
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_ghost"))
            .args(["--broker", "--agent-id", "fixture", "--cwd"])
            .arg(root.path())
            .env_clear()
            .env("HOME", root.path())
            .env("PATH", "/usr/bin:/bin")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        match mode {
            "oversized" => stdin.write_u32(u32::MAX).await.unwrap(),
            "invalid-json" | "unknown-field" => {
                let bytes = if mode == "invalid-json" {
                    b"bootstrap-secret-sentinel".as_slice()
                } else {
                    br#"{"path":"/no-such-broker","capability":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"credential":"bootstrap-secret-sentinel"}"#
                };
                stdin.write_u32(bytes.len() as u32).await.unwrap();
                stdin.write_all(bytes).await.unwrap();
            }
            _ => {}
        }
        let output =
            tokio::time::timeout(std::time::Duration::from_secs(7), child.wait_with_output())
                .await
                .unwrap()
                .unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("invalid private bootstrap"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("bootstrap-secret-sentinel"));
        assert!(output.stdout.is_empty());
    }
}
