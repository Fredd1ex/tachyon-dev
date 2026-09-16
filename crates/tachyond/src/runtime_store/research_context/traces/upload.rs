//! Blocking storage actor state. A staged object is charged in the existing trace
//! table and physical quota, but cannot resolve as a readable resource.
use super::*;
use std::sync::Arc;
use tachyon_model::broker::{ResourceUpload, UploadReply, UPLOAD_CHUNK};

pub(crate) struct Upload {
    store: Arc<RuntimeStore>,
    campaign: String,
    resource: Resource,
    file: std::fs::File,
    hash: Sha256,
    offset: u64,
    committed: bool,
}

impl Upload {
    #[cfg(test)]
    pub(crate) fn begin(
        store: Arc<RuntimeStore>,
        campaign: &str,
        work: &str,
        attempt: &str,
        generation: u64,
        request: ResourceUpload,
    ) -> Result<Self, String> {
        Self::begin_checked(store, campaign, work, attempt, generation, request, || true)
    }

    pub(crate) fn begin_checked(
        store: Arc<RuntimeStore>,
        campaign: &str,
        work: &str,
        attempt: &str,
        generation: u64,
        request: ResourceUpload,
        permitted: impl Fn() -> bool,
    ) -> Result<Self, String> {
        let ResourceUpload::Begin {
            handle,
            retained,
            total,
            storage_failed,
            sha256,
        } = request
        else {
            return Err(err("expected upload begin"));
        };
        if !handle.starts_with("output:")
            || uuid::Uuid::parse_str(&handle[7..]).is_err()
            || handle.len() > 64
            || retained > store.trace_limits.operation as u64
            || total < retained
            || sha256.len() != 64
            || !sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(err("invalid output declaration"));
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(err)?
            .as_millis() as u64;
        let id = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    campaign,
                    work,
                    attempt,
                    generation,
                    &handle,
                    "retained_output"
                ))
                .map_err(err)?
            )
        );
        let resource = Resource {
            reference: ResourceRef {
                kind: ResourceKind::Trace,
                work_id: work.into(),
                id: id.clone(),
                version: sha256,
            },
            occurred_at_ms: Some(now),
            data: json!({"schema_version":1,"phase":"retained_output","attempt_id":attempt,
                "generation":generation,"live_handle_id":handle,"size_bytes":retained,
                "retained_bytes":retained,"total_bytes":total,"discarded_bytes":total-retained,
                "storage_failed":storage_failed,"retention_state":"staging",
                "lease_expires_ms":now.saturating_add(300_000),
                "full_output":false,"worker_metadata_informational_only":true}),
        };
        let tx = store.database.begin_write().map_err(err)?;
        // Expiry is not proof of absence or cleanup. Retain the object and charge.
        {
            let mut table = tx.open_table(TRACES).map_err(err)?;
            let mut expired = Vec::new();
            for row in table.iter().map_err(err)? {
                let (key, value) = row.map_err(err)?;
                let r: Resource = serde_json::from_slice(value.value()).map_err(err)?;
                if r.data["retention_state"] == "staging"
                    && r.data["lease_expires_ms"]
                        .as_u64()
                        .is_some_and(|t| t <= now)
                {
                    expired.push((key.value().0.to_owned(), key.value().1.to_owned()));
                }
            }
            for (campaign, id) in expired {
                let mut resource: Resource = serde_json::from_slice(
                    table
                        .get((campaign.as_str(), id.as_str()))
                        .map_err(err)?
                        .ok_or("missing upload lease")?
                        .value(),
                )
                .map_err(err)?;
                resource.data["retention_state"] = json!("gap");
                resource.data["failure"] =
                    json!("upload lease expired; retained bytes unavailable");
                // Keep the identity tombstone: an old actor cannot overwrite a retry.
                table
                    .insert(
                        (campaign.as_str(), id.as_str()),
                        serde_json::to_vec(&resource).map_err(err)?.as_slice(),
                    )
                    .map_err(err)?;
            }
        }
        tx.commit().map_err(err)?;
        let tx = store.database.begin_write().map_err(err)?;
        if tx
            .open_table(TRACES)
            .map_err(err)?
            .get((campaign, id.as_str()))
            .map_err(err)?
            .is_some()
        {
            return Err(err("duplicate output upload"));
        }
        store.store_trace_object_checked(tx, campaign, resource.clone(), &[], &permitted)?;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .custom_flags(nix::libc::O_NOFOLLOW)
            .open(store.trace_root.join(&id))
            .map_err(err)?;
        Ok(Self {
            store,
            campaign: campaign.into(),
            resource,
            file,
            hash: Sha256::new(),
            offset: 0,
            committed: false,
        })
    }

    #[cfg(test)]
    pub(crate) fn apply(&mut self, request: ResourceUpload) -> Result<UploadReply, String> {
        self.apply_checked(request, || true, |_| Ok(()))
    }

    pub(crate) fn apply_checked(
        &mut self,
        request: ResourceUpload,
        permitted: impl Fn() -> bool,
        validate_commit: impl Fn(&WriteTransaction) -> Result<(), String>,
    ) -> Result<UploadReply, String> {
        if !permitted() {
            return Err(err("upload expired"));
        }
        let size = self.resource.data["size_bytes"]
            .as_u64()
            .ok_or("invalid upload size")?;
        match request {
            ResourceUpload::Chunk { offset, bytes } => {
                if offset != self.offset
                    || bytes.is_empty()
                    || bytes.len() > UPLOAD_CHUNK
                    || offset
                        .checked_add(bytes.len() as u64)
                        .is_none_or(|n| n > size)
                {
                    return Err(err("invalid upload chunk"));
                }
                self.file.write_all(&bytes).map_err(err)?;
                self.hash.update(&bytes);
                self.offset += bytes.len() as u64;
                Ok(UploadReply::Accepted {
                    offset: self.offset,
                })
            }
            ResourceUpload::Finish { sha256 } => {
                let digest = format!("{:x}", self.hash.clone().finalize());
                if self.offset != size
                    || digest != self.resource.reference.version
                    || sha256.is_some_and(|d| d != digest)
                {
                    return Err(err("upload length/digest mismatch"));
                }
                self.file.flush().map_err(err)?;
                self.file.sync_all().map_err(err)?;
                // Rehash the on-disk object, not merely the received buffers.
                self.store.trace_read(&self.resource, 0, 1)?;
                self.file
                    .set_permissions(std::fs::Permissions::from_mode(0o400))
                    .map_err(err)?;
                self.file.sync_all().map_err(err)?;
                self.resource.data["retention_state"] = json!("ready");
                self.resource.data["full_output"] = json!(
                    self.resource.data["discarded_bytes"] == 0
                        && self.resource.data["storage_failed"] == false
                );
                let tx = self.store.database.begin_write().map_err(err)?;
                let mut table = tx.open_table(TRACES).map_err(err)?;
                let previous: Resource = serde_json::from_slice(
                    table
                        .get((self.campaign.as_str(), self.resource.reference.id.as_str()))
                        .map_err(err)?
                        .ok_or("upload lease lost")?
                        .value(),
                )
                .map_err(err)?;
                if previous.data["retention_state"] != "staging" {
                    return Err(err("upload lease lost"));
                }
                validate_commit(&tx)?;
                if !permitted() {
                    return Err(err("upload expired before commit"));
                }
                table
                    .insert(
                        (self.campaign.as_str(), self.resource.reference.id.as_str()),
                        serde_json::to_vec(&self.resource).map_err(err)?.as_slice(),
                    )
                    .map_err(err)?;
                drop(table);
                let receipt = self.store.retained.reserve_in(
                    &tx,
                    &self.campaign,
                    "trace",
                    &self.resource.reference.id,
                    size,
                    &self.resource.reference.version,
                    false,
                )?;
                self.store.retained.ready_in(&tx, &receipt, self.offset)?;
                tx.commit().map_err(err)?;
                self.committed = true;
                Ok(UploadReply::Ready {
                    resource: self.resource.reference.clone(),
                })
            }
            _ => Err(err("unexpected upload begin")),
        }
    }
}

impl Drop for Upload {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        // Runs on the blocking actor. Retain partial evidence and its upper charge;
        // disconnect/cancellation is not authority to delete local campaign data.
        self.resource.data["retention_state"] = json!("gap");
        self.resource.data["full_output"] = json!(false);
        self.resource.data["failure"] =
            json!("upload incomplete or rejected; retained bytes unavailable");
        let _ = (|| -> Result<(), String> {
            let tx = self.store.database.begin_write().map_err(err)?;
            tx.open_table(TRACES)
                .map_err(err)?
                .insert(
                    (self.campaign.as_str(), self.resource.reference.id.as_str()),
                    serde_json::to_vec(&self.resource).map_err(err)?.as_slice(),
                )
                .map_err(err)?;
            tx.commit().map_err(err)
        })();
    }
}
