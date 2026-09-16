//! Operator inventory, NOT an admission ledger or an atomic cross-store snapshot.
use super::*;
use serde_json::json;

impl CampaignService {
    pub(super) fn storage_inventory(&self, id: &str) -> Result<serde_json::Value, String> {
        let tx = self.store.database.begin_read().map_err(err)?;
        let mut ready = 0u64;
        let mut reserved = 0u64;
        let mut uncertain = 0u64;
        let mut references = 0u64;
        let mut records = 0u64;
        for row in tx
            .open_table(super::super::research_context::traces::TRACES)
            .map_err(err)?
            .range((id, "")..)
            .map_err(err)?
        {
            let (key, value) = row.map_err(err)?;
            if key.value().0 != id {
                break;
            }
            records += 1;
            if records > 20_000 {
                return Err("storage inventory record bound exceeded".into());
            }
            let resource: tachyon_api::context::Resource =
                serde_json::from_slice(value.value()).map_err(err)?;
            let bytes = resource.data["size_bytes"]
                .as_u64()
                .ok_or("invalid trace size")?;
            let total = if !resource.data["artifact"].is_null() {
                // A document descriptor references ArtifactStore, not another copy.
                &mut references
            } else if resource.data["retention_state"] == "staging" {
                &mut reserved
            } else if resource.data["retention_state"] == "gap" {
                &mut uncertain
            } else {
                &mut ready
            };
            *total = total
                .checked_add(bytes)
                .ok_or("storage inventory overflow")?;
        }
        drop(tx);
        // Count path lengths, including staging and orphan entries, without opening
        // content or the independently locked artifact database. Hardlinks charge
        // once per path, matching ArtifactStore's conservative admission policy.
        let root = self.root.join("campaigns").join(id).join("artifacts");
        let artifact = (|| -> Result<serde_json::Value, String> {
            match root.canonicalize() {
                Ok(canonical) if canonical != root => {
                    return Err("artifact root is not canonical".into())
                }
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(err(e)),
                _ => (),
            }
            let mut entries = 0u64;
            let mut objects = 0u64;
            let mut staging = 0u64;
            for (name, bytes) in [("objects", &mut objects), ("staging", &mut staging)] {
                let path = root.join(name);
                match std::fs::symlink_metadata(&path) {
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(err(e)),
                    Ok(meta) if !meta.is_dir() => return Err("artifact directory replaced".into()),
                    Ok(_) => (),
                }
                for entry in std::fs::read_dir(path).map_err(err)? {
                    entries += 1;
                    if entries > 20_000 {
                        return Err("artifact inventory entry bound exceeded".into());
                    }
                    let meta =
                        std::fs::symlink_metadata(entry.map_err(err)?.path()).map_err(err)?;
                    if !meta.is_file() {
                        return Err("non-file artifact entry; size unknown".into());
                    }
                    *bytes = bytes
                        .checked_add(meta.len())
                        .ok_or("storage inventory overflow")?;
                }
            }
            Ok(
                json!({"entries":entries,"object_path_bytes":objects,"staging_path_bytes":staging,
                "total_path_bytes":objects.checked_add(staging).ok_or("storage inventory overflow")?}),
            )
        })();
        let trace_charge = ready
            .checked_add(reserved)
            .and_then(|v| v.checked_add(uncertain))
            .ok_or("storage inventory overflow")?;
        let observed = artifact
            .as_ref()
            .ok()
            .and_then(|v| v["total_path_bytes"].as_u64())
            .and_then(|v| v.checked_add(trace_charge));
        Ok(
            json!({"schema_version":1,"shared_quota_enforced":true,"complete":false,
            "retained_admission":self.store.retained.summary(id)?,
            "measurement":"bounded metadata observation; not atomic; not allocated disk blocks",
            "artifact":artifact.unwrap_or_else(|error| json!({"unavailable":error})),
            "trace":{"records":records,"ready_recorded_bytes":ready,"reserved_upper_bytes":reserved,
                "uncertain_recorded_bytes":uncertain,"artifact_reference_bytes_excluded":references},
            "observed_artifact_plus_trace_charge_bytes":observed,
            "unmeasured":["managed_snapshots_and_quarantine","verification_copies","live_anonymous_output",
                "unindexed_trace_orphans","native_workspace_and_home","database_and_filesystem_overhead"],
            "purge_supported":false}),
        )
    }
}
