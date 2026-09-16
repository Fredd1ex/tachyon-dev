#![forbid(unsafe_code)]

use tachyon_api::campaign::{AllocationMode, CampaignAllocation};

#[test]
fn strict_host_reallocation_manifest_fragment() {
    let value = serde_json::json!({
        "mode": "deterministic", "max_running": 2,
        "allowed_actions": ["reallocate"],
        "signals": [{"command_id": "move-1", "group_id": "children", "expected_revision": 1,
            "action": {"kind": "reallocate", "source_work_id": "a", "source_generation": 1,
                "target_work_id": "b", "target_generation": 1, "tokens": 10,
                "cost_micro_usd": 0, "expected_ledger_revision": 5}}]
    });
    let parsed: CampaignAllocation = serde_json::from_value(value.clone()).unwrap();
    parsed.validate().unwrap();
    assert_eq!(serde_json::to_value(&parsed).unwrap(), value);
    for mode in [AllocationMode::Fixed, AllocationMode::ModelProposed] {
        let mut invalid = parsed.clone();
        invalid.mode = mode;
        assert!(invalid.validate().is_err());
    }
    let mut invalid = parsed.clone();
    invalid.allowed_actions.clear();
    assert!(invalid.validate().is_err());
    for (key, replacement) in [
        ("tokens", serde_json::json!(0)),
        ("source_generation", serde_json::json!(0)),
        ("target_work_id", serde_json::json!("a")),
    ] {
        let mut invalid = value.clone();
        invalid["signals"][0]["action"][key] = replacement;
        assert!(serde_json::from_value::<CampaignAllocation>(invalid)
            .unwrap()
            .validate()
            .is_err());
    }
    for (key, replacement) in [
        ("tokens", serde_json::json!(-1)),
        ("tokens", serde_json::json!(1.5)),
        ("grant", serde_json::json!(100)),
    ] {
        let mut invalid = value.clone();
        invalid["signals"][0]["action"][key] = replacement;
        assert!(serde_json::from_value::<CampaignAllocation>(invalid).is_err());
    }
    let mut invalid = value;
    invalid["signals"][0]["action"]
        .as_object_mut()
        .unwrap()
        .remove("expected_ledger_revision");
    assert!(serde_json::from_value::<CampaignAllocation>(invalid).is_err());
}
