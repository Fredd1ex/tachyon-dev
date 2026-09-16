use super::{ToolContext, ToolError, ToolErrorCode};
use std::{sync::Arc, time::Duration};
use tachyon_model::broker::{BrokerClient, CpuJobReply, CpuJobRequest, JobWorkload};

pub(crate) struct JobLease {
    client: Arc<BrokerClient>,
    id: uuid::Uuid,
    pub cleanup_confirmed: bool,
}

impl JobLease {
    #[cfg(test)]
    pub async fn acquire(
        context: &ToolContext,
        deadline: tokio::time::Instant,
    ) -> Result<Option<Self>, ToolError> {
        Self::acquire_request(context, deadline, CpuJobRequest::TryAcquire)
            .await
            .map(|(lease, _)| lease)
    }

    pub async fn acquire_job(
        context: &ToolContext,
        deadline: tokio::time::Instant,
        workload: JobWorkload,
        max_duration_ms: u64,
    ) -> Result<(Option<Self>, Vec<String>), ToolError> {
        if context.host_service.is_none() && workload == (JobWorkload::Gpu {}) {
            return Err(ToolError::new(
                ToolErrorCode::PermissionDenied,
                "GPU jobs require an explicit host grant",
                false,
            ));
        }
        Self::acquire_request(
            context,
            deadline,
            CpuJobRequest::Acquire {
                workload,
                max_duration_ms,
            },
        )
        .await
    }

    async fn acquire_request(
        context: &ToolContext,
        deadline: tokio::time::Instant,
        request: CpuJobRequest,
    ) -> Result<(Option<Self>, Vec<String>), ToolError> {
        let Some(client) = &context.host_service else {
            return Ok((None, Vec::new()));
        };
        let acquire = async {
            loop {
                let client = client.clone();
                let request = request.clone();
                // Finish the frame even if the caller stops waiting. A late grant
                // is dropped (and released) without ever spawning a process.
                let reply = tokio::spawn(async move {
                    match client.cpu_job(request).await {
                        Ok(CpuJobReply::Granted { permit, device_ids }) => Ok(Some((
                            Self {
                                client,
                                id: permit,
                                cleanup_confirmed: true,
                            },
                            device_ids,
                        ))),
                        Ok(CpuJobReply::Acquired { permit }) => Ok(Some((
                            Self {
                                client,
                                id: permit,
                                cleanup_confirmed: true,
                            },
                            Vec::new(),
                        ))),
                        Ok(CpuJobReply::Busy) => Ok(None),
                        _ => Err(()),
                    }
                })
                .await;
                match reply {
                    Ok(Ok(Some((permit, devices)))) => return Ok((Some(permit), devices)),
                    Ok(Ok(None)) => {
                        // cpu_job returned the stream and dropped its mutex guard.
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    _ => {
                        return Err(ToolError::new(
                            ToolErrorCode::PermissionDenied,
                            "host native job admission unavailable",
                            false,
                        ));
                    }
                }
            }
        };
        tokio::select! {
            biased;
            _ = context.cancellation.cancelled() => Err(ToolError::new(
                ToolErrorCode::Cancelled, "exec cancelled waiting for host native capacity", true)),
            _ = tokio::time::sleep_until(deadline) => Err(ToolError::new(
                ToolErrorCode::Timeout, "exec deadline elapsed waiting for host native capacity", true)),
            result = acquire => result,
        }
    }

    pub async fn release(mut self) -> Result<(), ToolError> {
        self.cleanup_confirmed = false;
        let client = self.client.clone();
        let id = self.id;
        // Supervisor cancellation must not interrupt a confirmed cleanup RPC.
        tokio::spawn(async move { release(&client, id).await })
            .await
            .map_err(|_| {
                ToolError::new(
                    ToolErrorCode::Internal,
                    "native lease release task failed",
                    false,
                )
            })?
    }
}

async fn release(client: &BrokerClient, permit: uuid::Uuid) -> Result<(), ToolError> {
    // Do not abandon confirmed cleanup just because another bounded broker RPC
    // currently owns the stream. The host session supplies the outer deadline.
    match client.cpu_job(CpuJobRequest::Release { permit }).await {
        Ok(CpuJobReply::Released) => Ok(()),
        _ => Err(ToolError::new(
            ToolErrorCode::ProcessFailed,
            "native cleanup completed but host lease release was not confirmed",
            false,
        )),
    }
}

impl Drop for JobLease {
    fn drop(&mut self) {
        if self.cleanup_confirmed {
            let client = self.client.clone();
            let id = self.id;
            // Spawn failure / pre-spawn cancellation has no native resources.
            tokio::spawn(async move {
                let _ = client
                    .cpu_job(CpuJobRequest::Unspawned { permit: id })
                    .await;
            });
        }
        // Dropping a running supervisor only requests kill; it is not proof of
        // cleanup. The host retains that permit rather than oversubscribing.
    }
}

#[cfg(test)]
mod tests {
    use super::JobLease as CpuJobPermit;
    use super::*;
    use crate::harness::runtime::*;
    use tachyon_model::broker::{private_pair, read_frame, write_frame, FrameReply, FrameRequest};

    #[tokio::test]
    async fn cancelled_cpu_acquire_drains_late_grant_and_preserves_shared_stream() {
        let root = tempfile::tempdir().unwrap();
        let (host, client) = private_pair().unwrap();
        let context = ToolContext {
            workspace_root: root.path().into(),
            cwd: root.path().into(),
            identity: ToolIdentity::default(),
            deadline: std::time::Instant::now() + Duration::from_secs(5),
            cancellation: tokio_util::sync::CancellationToken::new(),
            policy: Arc::new(ToolPolicy::worker_default(root.path().into())),
            event_sink: Arc::new(NoopEventSink),
            output_store: Arc::new(NoopOutputStore),
            host_service: Some(Arc::new(client)),
        };
        let (seen, arrived) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        let (released, release_seen) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut stream = host.authenticate().await.unwrap();
            assert!(matches!(
                read_frame(&mut stream).await.unwrap(),
                FrameRequest::CpuJob(CpuJobRequest::TryAcquire)
            ));
            seen.send(()).unwrap();
            resumed.await.unwrap();
            let id = uuid::Uuid::new_v4();
            write_frame(
                &mut stream,
                &FrameReply::CpuJob(CpuJobReply::Acquired { permit: id }),
            )
            .await
            .unwrap();
            assert!(matches!(read_frame(&mut stream).await.unwrap(),
                FrameRequest::CpuJob(CpuJobRequest::Unspawned { permit }) if permit == id));
            write_frame(&mut stream, &FrameReply::CpuJob(CpuJobReply::Released))
                .await
                .unwrap();
            released.send(()).unwrap();
            assert!(matches!(
                read_frame(&mut stream).await.unwrap(),
                FrameRequest::CpuJob(CpuJobRequest::Profile)
            ));
            write_frame(
                &mut stream,
                &FrameReply::CpuJob(CpuJobReply::Profile {
                    max_cpu_jobs: 1,
                    max_gpu_jobs: 0,
                }),
            )
            .await
            .unwrap();
        });
        let worker = async {
            let acquire = CpuJobPermit::acquire(
                &context,
                tokio::time::Instant::now() + Duration::from_secs(5),
            );
            tokio::pin!(acquire);
            tokio::select! {
                _ = arrived => {},
                _ = &mut acquire => panic!("acquire completed before grant"),
            }
            context.cancellation.cancel();
            assert!(matches!(
                acquire.await,
                Err(ToolError {
                    code: ToolErrorCode::Cancelled,
                    ..
                })
            ));
            resume.send(()).unwrap();
            release_seen.await.unwrap();
            assert!(matches!(
                context
                    .host_service
                    .as_ref()
                    .unwrap()
                    .cpu_job(CpuJobRequest::Profile)
                    .await
                    .unwrap(),
                CpuJobReply::Profile { .. }
            ));
            server.await.unwrap();
        };
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cancelled_cpu_release_finishes_but_unconfirmed_drop_never_releases() {
        let (host, client) = private_pair().unwrap();
        let client = Arc::new(client);
        let id = uuid::Uuid::new_v4();
        drop(CpuJobPermit {
            client: client.clone(),
            id,
            cleanup_confirmed: false,
        });
        let (seen, arrived) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut stream = host.authenticate().await.unwrap();
            assert!(matches!(
                read_frame(&mut stream).await.unwrap(),
                FrameRequest::CpuJob(CpuJobRequest::Profile)
            ));
            write_frame(
                &mut stream,
                &FrameReply::CpuJob(CpuJobReply::Profile {
                    max_cpu_jobs: 1,
                    max_gpu_jobs: 0,
                }),
            )
            .await
            .unwrap();
            assert!(
                matches!(read_frame(&mut stream).await.unwrap(), FrameRequest::CpuJob(CpuJobRequest::Release { permit }) if permit == id)
            );
            seen.send(()).unwrap();
            resumed.await.unwrap();
            write_frame(&mut stream, &FrameReply::CpuJob(CpuJobReply::Released))
                .await
                .unwrap();
            assert!(matches!(
                read_frame(&mut stream).await.unwrap(),
                FrameRequest::CpuJob(CpuJobRequest::Profile)
            ));
            write_frame(
                &mut stream,
                &FrameReply::CpuJob(CpuJobReply::Profile {
                    max_cpu_jobs: 1,
                    max_gpu_jobs: 0,
                }),
            )
            .await
            .unwrap();
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            client.cpu_job(CpuJobRequest::Profile).await.unwrap();
            let mut release = Box::pin(
                CpuJobPermit {
                    client: client.clone(),
                    id,
                    cleanup_confirmed: true,
                }
                .release(),
            );
            tokio::select! {
                _ = arrived => {},
                _ = &mut release => panic!("release completed before reply"),
            }
            drop(release);
            resume.send(()).unwrap();
            client.cpu_job(CpuJobRequest::Profile).await.unwrap();
            server.await.unwrap();
        })
        .await
        .unwrap();
    }
}
