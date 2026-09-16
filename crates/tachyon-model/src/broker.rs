//! Private model transport. Possession is scoped authority, not a sandbox.
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    sync::Mutex,
};

use crate::{ChatMessage, Completion, ModelError, Result, ToolSpec};

pub const MAX_FRAME: usize = 1024 * 1024;
pub const MAX_MESSAGES: usize = 256;
pub const MAX_TOOLS: usize = 64;
pub const MAX_REQUESTS: usize = 128;
pub const UPLOAD_CHUNK: usize = 32 * 1024;

/// Native job admission only, not an OS sandbox or a CPU utilization limit.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum CpuJobRequest {
    Profile,
    TryAcquire,
    Acquire {
        workload: JobWorkload,
        max_duration_ms: u64,
    },
    Unspawned {
        permit: uuid::Uuid,
    },
    Release {
        permit: uuid::Uuid,
    },
}

/// Logical request class only. Device selection belongs exclusively to the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "class", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobWorkload {
    Cpu {},
    Gpu {},
}

impl Default for JobWorkload {
    fn default() -> Self {
        Self::Cpu {}
    }
}

/// Round up before narrowing; never truncate a u128 duration into a budget.
pub fn job_duration_ms(duration: std::time::Duration) -> Option<u64> {
    u64::try_from(duration.as_nanos().div_ceil(1_000_000)).ok()
}

#[cfg(test)]
mod native_compute_tests {
    use super::*;

    #[test]
    fn duration_rounds_before_narrowing_and_workload_cannot_select_devices() {
        use std::time::Duration;
        assert_eq!(job_duration_ms(Duration::ZERO), Some(0));
        assert_eq!(job_duration_ms(Duration::from_nanos(1)), Some(1));
        assert_eq!(job_duration_ms(Duration::from_micros(1001)), Some(2));
        assert_eq!(
            job_duration_ms(Duration::from_millis(u64::MAX)),
            Some(u64::MAX)
        );
        assert_eq!(job_duration_ms(Duration::MAX), None);
        assert!(
            serde_json::from_str::<JobWorkload>(r#"{"class":"gpu","device_ids":["0"]}"#).is_err()
        );
        assert!(serde_json::from_str::<JobWorkload>(r#"{"class":"gpu","count":0}"#).is_err());
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum CpuJobReply {
    Profile {
        max_cpu_jobs: usize,
        max_gpu_jobs: usize,
    },
    Acquired {
        permit: uuid::Uuid,
    },
    Granted {
        permit: uuid::Uuid,
        device_ids: Vec<String>,
    },
    Busy,
    Released,
    Denied,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceUpload {
    Begin {
        handle: String,
        retained: u64,
        total: u64,
        storage_failed: bool,
        sha256: String,
    },
    Chunk {
        offset: u64,
        bytes: Vec<u8>,
    },
    Finish {
        sha256: Option<String>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum UploadReply {
    Accepted {
        offset: u64,
    },
    Ready {
        resource: tachyon_api::context::ResourceRef,
    },
    Denied,
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum FrameRequest {
    CpuJob(CpuJobRequest),
    ResourceUpload(ResourceUpload),
    Work(tachyon_api::work::Request),
    Model(Request),
    Control(tachyon_api::agents::Request),
    BoundaryAck { id: String, cursor: u64 },
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum FrameReply {
    CpuJob(CpuJobReply),
    ResourceUpload(UploadReply),
    Work(tachyon_api::work::Reply),
    Model(Reply),
    Control(tachyon_api::agents::Reply),
    Boundary(Boundary),
}

/// Canonical host context, never accepted from model/tool arguments.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Boundary {
    pub id: String,
    pub objective: String,
    pub cursor: u64,
    pub instruction_revision: u64,
    pub instructions: Option<String>,
    pub messages: Vec<ParentMessage>,
    /// Bounded durable context window, including already delivered data.
    pub context_messages: Vec<ParentMessage>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParentMessage {
    pub sequence: u64,
    pub sender: String,
    pub text: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub id: String,
    pub messages: Vec<ChatMessage>,
    pub tools: Vec<ToolSpec>,
    #[serde(default)]
    pub context: Option<tachyon_api::context::WorkerContextMetadata>,
}

impl Request {
    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty()
            || self.id.len() > 256
            || self.messages.len() > MAX_MESSAGES
            || self.tools.len() > MAX_TOOLS
            || self.context.as_ref().is_some_and(|c| !c.valid())
        {
            return Err(protocol_error());
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reply {
    pub id: String,
    // Never forward provider/storage error strings across this boundary.
    pub completion: Option<Completion>,
}

pub fn protocol_error() -> ModelError {
    ModelError::Api("private model channel failed; outcome may be unknown".into())
}

pub async fn read_frame<T: DeserializeOwned>(
    stream: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<T> {
    let size = stream.read_u32().await? as usize;
    if size == 0 || size > MAX_FRAME {
        return Err(protocol_error());
    }
    let mut bytes = vec![0; size];
    stream.read_exact(&mut bytes).await?;
    serde_json::from_slice(&bytes).map_err(|_| protocol_error())
}

pub async fn write_frame<T: Serialize>(
    stream: &mut (impl tokio::io::AsyncWrite + Unpin),
    value: &T,
) -> Result<()> {
    // A capped writer avoids allocating an oversized serialized frame.
    struct Capped(Vec<u8>);
    impl std::io::Write for Capped {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > MAX_FRAME - self.0.len() {
                return Err(std::io::Error::other("frame limit"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut bytes = Capped(Vec::new());
    serde_json::to_writer(&mut bytes, value).map_err(|_| protocol_error())?;
    stream.write_u32(bytes.0.len() as u32).await?;
    stream.write_all(&bytes.0).await?;
    Ok(())
}

/// No credential accessor, Debug, serialization, or reconnect.
pub struct HostChannel {
    stream: UnixStream,
    capability: [u8; 16],
}

pub struct BrokerClient {
    completion_proposal: std::sync::Mutex<Option<tachyon_api::work::CompletionProposal>>,
    instruction_revision: std::sync::atomic::AtomicU64,
    connection: Mutex<Option<(UnixStream, Option<[u8; 16]>)>>,
    pub controls: Vec<tachyon_api::agents::Control>,
}

/// Secret bootstrap, delivered only on the child's private stdin, never logged.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    path: std::path::PathBuf,
    capability: [u8; 16],
    controls: Vec<tachyon_api::agents::Control>,
}

/// One accept only. The directory is created atomically with mode 0700.
#[cfg(target_os = "linux")]
pub struct PrivateListener {
    listener: tokio::net::UnixListener,
    directory: tempfile::TempDir,
    capability: [u8; 16],
}

#[cfg(target_os = "linux")]
impl PrivateListener {
    pub fn bind() -> std::io::Result<Self> {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::Builder::new()
            .prefix("ghost-broker-")
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()?;
        let listener = tokio::net::UnixListener::bind(directory.path().join("model.sock"))?;
        Ok(Self {
            listener,
            directory,
            capability: *uuid::Uuid::new_v4().as_bytes(),
        })
    }

    pub async fn bootstrap(&self, stdin: &mut (impl tokio::io::AsyncWrite + Unpin)) -> Result<()> {
        self.bootstrap_controls(stdin, Vec::new()).await
    }

    pub async fn bootstrap_controls(
        &self,
        stdin: &mut (impl tokio::io::AsyncWrite + Unpin),
        controls: Vec<tachyon_api::agents::Control>,
    ) -> Result<()> {
        write_frame(
            stdin,
            &Bootstrap {
                path: self.directory.path().join("model.sock"),
                capability: self.capability,
                controls,
            },
        )
        .await
    }

    pub async fn accept(self, pid: u32, uid: u32) -> Result<HostChannel> {
        let (stream, _) = self.listener.accept().await?;
        let peer = stream.peer_cred()?;
        if peer.pid().and_then(|pid| u32::try_from(pid).ok()) != Some(pid) || peer.uid() != uid {
            return Err(protocol_error());
        }
        Ok(HostChannel {
            stream,
            capability: self.capability,
        })
    }
}

impl Bootstrap {
    pub async fn connect(self) -> Result<BrokerClient> {
        let stream = UnixStream::connect(self.path).await?;
        Ok(BrokerClient {
            completion_proposal: Default::default(),
            instruction_revision: 0.into(),
            connection: Mutex::new(Some((stream, Some(self.capability)))),
            controls: self.controls,
        })
    }
}

/// Trusted host creates a fresh pair for exactly one assignment binding.
pub fn private_pair() -> std::io::Result<(HostChannel, BrokerClient)> {
    let (host, worker) = UnixStream::pair()?;
    let capability = *uuid::Uuid::new_v4().as_bytes();
    Ok((
        HostChannel {
            stream: host,
            capability,
        },
        BrokerClient {
            completion_proposal: Default::default(),
            instruction_revision: 0.into(),
            connection: Mutex::new(Some((worker, Some(capability)))),
            controls: Vec::new(),
        },
    ))
}

impl HostChannel {
    pub async fn authenticate(mut self) -> Result<UnixStream> {
        let mut supplied = [0; 16];
        self.stream.read_exact(&mut supplied).await?;
        if supplied != self.capability {
            return Err(protocol_error());
        }
        self.stream.write_u8(1).await?;
        Ok(self.stream)
    }
}

impl BrokerClient {
    /// One immediate request/reply. Busy never retains the socket or its mutex.
    pub async fn cpu_job(&self, request: CpuJobRequest) -> Result<CpuJobReply> {
        let mut guard = self.connection.lock().await;
        let (mut stream, capability) = guard.take().ok_or_else(protocol_error)?;
        if let Some(capability) = capability {
            stream.write_all(&capability).await?;
            if stream.read_u8().await? != 1 {
                return Err(protocol_error());
            }
        }
        write_frame(&mut stream, &FrameRequest::CpuJob(request.clone())).await?;
        let FrameReply::CpuJob(reply) = read_frame(&mut stream).await? else {
            return Err(protocol_error());
        };
        if !matches!(
            (&request, &reply),
            (_, CpuJobReply::Denied)
                | (CpuJobRequest::Profile, CpuJobReply::Profile { .. })
                | (
                    CpuJobRequest::TryAcquire,
                    CpuJobReply::Acquired { .. } | CpuJobReply::Busy
                )
                | (CpuJobRequest::Release { .. }, CpuJobReply::Released)
                | (CpuJobRequest::Unspawned { .. }, CpuJobReply::Released)
                | (
                    CpuJobRequest::Acquire { .. },
                    CpuJobReply::Granted { .. } | CpuJobReply::Busy
                )
        ) {
            return Err(protocol_error());
        }
        *guard = Some((stream, None));
        Ok(reply)
    }
    pub async fn resource_upload(&self, request: ResourceUpload) -> Result<UploadReply> {
        let mut guard = self.connection.lock().await;
        let (mut stream, capability) = guard.take().ok_or_else(protocol_error)?;
        if let Some(capability) = capability {
            stream.write_all(&capability).await?;
            if stream.read_u8().await? != 1 {
                return Err(protocol_error());
            }
        }
        write_frame(&mut stream, &FrameRequest::ResourceUpload(request)).await?;
        let FrameReply::ResourceUpload(reply) = read_frame(&mut stream).await? else {
            return Err(protocol_error());
        };
        *guard = Some((stream, None));
        Ok(reply)
    }
    pub fn completion_proposal(&self) -> Option<tachyon_api::work::CompletionProposal> {
        self.completion_proposal.lock().unwrap().clone()
    }

    pub async fn work(
        &self,
        request: &tachyon_api::work::Request,
    ) -> Result<tachyon_api::work::Reply> {
        request.validate().map_err(|_| protocol_error())?;
        let mut guard = self.connection.lock().await;
        if let tachyon_api::work::Request::Complete {
            summary,
            candidate_refs,
            unresolved_questions,
        } = request
        {
            if let Some(proposal) = self.completion_proposal() {
                return Ok(
                    if summary == &proposal.summary
                        && candidate_refs == &proposal.candidate_refs
                        && unresolved_questions == &proposal.unresolved_questions
                    {
                        tachyon_api::work::Reply::Proposed { proposal }
                    } else {
                        tachyon_api::work::Reply::Denied
                    },
                );
            }
        }
        let (mut stream, capability) = guard.take().ok_or_else(protocol_error)?;
        if let Some(capability) = capability {
            stream.write_all(&capability).await?;
            if stream.read_u8().await? != 1 {
                return Err(protocol_error());
            }
        }
        write_frame(&mut stream, &FrameRequest::Work(request.clone())).await?;
        let FrameReply::Work(reply) = read_frame(&mut stream).await? else {
            return Err(protocol_error());
        };
        use tachyon_api::work::{Reply as WorkReply, Request as WorkRequest};
        let valid = match (request, &reply) {
            (_, WorkReply::Denied) => true,
            (
                WorkRequest::Status {},
                WorkReply::Status {
                    objective,
                    phase,
                    pending_questions,
                    ..
                },
            ) => objective.len() <= 16384 && phase.len() <= 64 && pending_questions.len() <= 32,
            (
                WorkRequest::Ask { request_id, .. },
                WorkReply::Answer {
                    request_id: returned,
                    answer,
                    resumed,
                },
            ) => {
                request_id == returned
                    && *resumed
                    && answer.as_ref().is_none_or(|s| s.len() <= 4096)
            }
            (WorkRequest::Complete { .. }, WorkReply::Proposed { .. }) => true,
            _ => false,
        };
        if !valid {
            return Err(protocol_error());
        }
        if let tachyon_api::work::Reply::Proposed { proposal } = &reply {
            let tachyon_api::work::Request::Complete {
                summary,
                candidate_refs,
                unresolved_questions,
            } = request
            else {
                return Err(protocol_error());
            };
            if summary != &proposal.summary
                || candidate_refs != &proposal.candidate_refs
                || unresolved_questions != &proposal.unresolved_questions
            {
                return Err(protocol_error());
            }
            self.instruction_revision.store(
                proposal.instruction_revision,
                std::sync::atomic::Ordering::Release,
            );
            *self.completion_proposal.lock().unwrap() = Some(proposal.clone());
        }
        *guard = Some((stream, None));
        Ok(reply)
    }
    pub fn instruction_revision(&self) -> Option<u64> {
        match self
            .instruction_revision
            .load(std::sync::atomic::Ordering::Acquire)
        {
            0 => None,
            revision => Some(revision),
        }
    }
    pub async fn control(
        &self,
        request: &tachyon_api::agents::Request,
    ) -> Result<tachyon_api::agents::Reply> {
        request.validate().map_err(|_| protocol_error())?;
        let mut guard = self.connection.lock().await;
        // As with model calls, cancellation after take permanently closes the channel.
        let (mut stream, capability) = guard.take().ok_or_else(protocol_error)?;
        if let Some(capability) = capability {
            stream.write_all(&capability).await?;
            if stream.read_u8().await? != 1 {
                return Err(protocol_error());
            }
        }
        write_frame(&mut stream, &FrameRequest::Control(request.clone())).await?;
        let FrameReply::Control(reply) = read_frame(&mut stream).await? else {
            return Err(protocol_error());
        };
        use tachyon_api::agents::{Reply as ControlReply, Request as ControlRequest};
        let matches = match (request, &reply) {
            (
                ControlRequest::Templates { after, limit },
                ControlReply::Templates {
                    templates,
                    next_cursor,
                },
            ) => {
                let id = |s: &str| !s.trim().is_empty() && s.len() <= 256;
                templates.len() <= *limit
                    && templates.iter().all(|t| {
                        id(&t.template_id)
                            && t.group_id.as_deref().is_none_or(id)
                            && (1..=32).contains(&t.work_count)
                            && (1..=4096).contains(&t.max_running)
                            && after.as_ref().is_none_or(|a| t.template_id > *a)
                    })
                    && templates
                        .windows(2)
                        .all(|w| w[0].template_id < w[1].template_id)
                    && next_cursor
                        .as_ref()
                        .is_none_or(|c| templates.last().is_some_and(|t| &t.template_id == c))
            }
            (ControlRequest::Resource { request }, ControlReply::Resource { page }) => {
                use tachyon_api::context::{Request, ResourceKind};
                let matched = match request {
                    Request::Read {
                        resource,
                        offset,
                        limit,
                    } => {
                        page.next_cursor.is_none()
                            && page.resources.len() == 1
                            && page.resources[0].reference == *resource
                            && if matches!(
                                resource.kind,
                                ResourceKind::Artifact
                                    | ResourceKind::Trace
                                    | ResourceKind::Document
                            ) {
                                let data = &page.resources[0].data;
                                data["offset"].as_u64() == Some(*offset)
                                    && data["bytes"].as_array().is_some_and(|bytes| {
                                        bytes.len() <= *limit
                                            && bytes
                                                .iter()
                                                .all(|b| b.as_u64().is_some_and(|n| n <= 255))
                                            && offset.checked_add(bytes.len() as u64).is_some_and(
                                                |next| data["next_offset"].as_u64() == Some(next),
                                            )
                                    })
                            } else {
                                *offset == 0
                            }
                    }
                    Request::Search { query }
                    | Request::Snapshot { query }
                    | Request::Attempts { query }
                    | Request::Findings { query }
                    | Request::Traces { query }
                    | Request::Documents { query }
                    | Request::Artifacts { query } => {
                        let kind = match request {
                            Request::Attempts { .. } => Some(ResourceKind::Attempt),
                            Request::Findings { .. } => Some(ResourceKind::Finding),
                            Request::Artifacts { .. } => Some(ResourceKind::Artifact),
                            Request::Traces { .. } => Some(ResourceKind::Trace),
                            Request::Snapshot { .. } => Some(ResourceKind::Trace),
                            Request::Documents { .. } => Some(ResourceKind::Document),
                            _ => None,
                        };
                        page.resources.len() <= query.limit
                            && page.resources.iter().all(|r| {
                                kind.is_none_or(|k| r.reference.kind == k)
                                    && (!matches!(request, Request::Snapshot { .. })
                                        || r.data["phase"] == "work_context_snapshot")
                                    && query
                                        .version
                                        .as_ref()
                                        .is_none_or(|v| r.reference.version == *v)
                                    && query
                                        .since_ms
                                        .is_none_or(|t| r.occurred_at_ms.is_some_and(|at| at >= t))
                                    && query.literal.as_ref().is_none_or(|s| {
                                        serde_json::to_string(r).is_ok_and(|v| v.contains(s))
                                    })
                            })
                    }
                };
                matched
                    && page.next_cursor.as_ref().is_none_or(|c| c.len() <= 1024)
                    && page.resources.iter().all(|r| r.reference.valid())
                    && serde_json::to_vec(&FrameReply::Control(ControlReply::Resource {
                        page: page.clone(),
                    }))
                    .is_ok_and(|b| b.len() <= tachyon_api::context::MAX_PAGE_BYTES)
            }
            (
                ControlRequest::Wait { work_ids, .. },
                ControlReply::Wait {
                    completed,
                    outstanding,
                    resumed,
                    ..
                },
            ) => {
                let returned: std::collections::BTreeSet<_> =
                    completed.iter().chain(outstanding).collect();
                *resumed
                    && completed.len() + outstanding.len() == work_ids.len()
                    && returned.len() == work_ids.len()
                    && work_ids.iter().all(|id| returned.contains(id))
            }
            (
                ControlRequest::Spawn { command_id, .. }
                | ControlRequest::Propose { command_id, .. },
                ControlReply::Admitted {
                    command_id: accepted,
                    work_ids,
                    group_id,
                },
            ) => {
                command_id == accepted
                    && group_id.is_none()
                    && work_ids.len() == 1
                    && work_ids.iter().all(|id| !id.is_empty() && id.len() <= 256)
            }
            (
                ControlRequest::Group { command_id, .. }
                | ControlRequest::ProposeGroup { command_id, .. },
                ControlReply::Admitted {
                    command_id: accepted,
                    work_ids,
                    group_id,
                },
            ) => {
                command_id == accepted
                    && group_id
                        .as_ref()
                        .is_some_and(|id| !id.is_empty() && id.len() <= 256)
                    && (1..=32).contains(&work_ids.len())
                    && work_ids.iter().all(|id| !id.is_empty() && id.len() <= 256)
                    && work_ids
                        .iter()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        == work_ids.len()
            }
            (_, ControlReply::Denied) => true,
            (
                ControlRequest::Cancel {
                    work_id,
                    generation,
                },
                ControlReply::CancellationRequested {
                    work_id: target,
                    generation: accepted,
                },
            ) => work_id == target && generation == accepted,
            (
                ControlRequest::GroupStatus { group_id },
                ControlReply::Group {
                    group_id: target,
                    revision,
                    max_running,
                    active,
                    total,
                    ..
                },
            ) => {
                group_id == target
                    && *revision > 0
                    && *max_running <= 4096
                    && active <= total
                    && *total <= 4096
            }
            (
                ControlRequest::GroupResize {
                    group_id,
                    expected_revision,
                    max_running,
                },
                ControlReply::Group {
                    group_id: target,
                    revision,
                    max_running: accepted,
                    active,
                    total,
                    ..
                },
            ) => {
                group_id == target
                    && expected_revision.checked_add(1) == Some(*revision)
                    && max_running == accepted
                    && active <= total
                    && *total <= 4096
            }
            (ControlRequest::Status { work_id }, ControlReply::Status { status }) => {
                work_id == &status.work_id
            }
            (ControlRequest::List { limit, .. }, ControlReply::List { work_ids, .. }) => {
                work_ids.len() <= *limit
            }
            (ControlRequest::Result { work_id, revision }, ControlReply::Result { snapshot }) => {
                snapshot.as_ref().is_none_or(|s| {
                    work_id == &s.work_id
                        && revision.map_or(s.current, |revision| revision == s.revision)
                        && s.valid()
                })
            }
            (
                ControlRequest::Send { command_id, .. } | ControlRequest::Steer { command_id, .. },
                ControlReply::Accepted {
                    command_id: accepted,
                    ..
                },
            ) => command_id == accepted,
            _ => false,
        };
        if !matches {
            return Err(protocol_error());
        }
        *guard = Some((stream, None));
        Ok(reply)
    }
    pub async fn chat(&self, messages: &[ChatMessage], tools: &[ToolSpec]) -> Result<Completion> {
        self.request_parts(&uuid::Uuid::new_v4().to_string(), messages, tools, None)
            .await
    }

    pub async fn chat_with_context(
        &self,
        messages: &[ChatMessage],
        tools: &[ToolSpec],
        context: &tachyon_api::context::WorkerContextMetadata,
    ) -> Result<Completion> {
        self.request_parts(
            &uuid::Uuid::new_v4().to_string(),
            messages,
            tools,
            Some(context),
        )
        .await
    }

    /// Explicit logical IDs confer no authority and never authorize replay.
    pub async fn request(&self, request: &Request) -> Result<Completion> {
        self.request_parts(
            &request.id,
            &request.messages,
            &request.tools,
            request.context.as_ref(),
        )
        .await
    }

    async fn request_parts(
        &self,
        id: &str,
        messages: &[ChatMessage],
        tools: &[ToolSpec],
        context: Option<&tachyon_api::context::WorkerContextMetadata>,
    ) -> Result<Completion> {
        // Borrow payloads so the capped serializer runs before any payload copy.
        #[derive(Serialize)]
        #[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
        enum BorrowedFrame<'a> {
            Model(BorrowedRequest<'a>),
        }
        #[derive(Serialize)]
        struct BorrowedRequest<'a> {
            id: &'a str,
            messages: &'a [ChatMessage],
            tools: &'a [ToolSpec],
            context: Option<&'a tachyon_api::context::WorkerContextMetadata>,
        }
        let mut guard = self.connection.lock().await;
        // Taking ownership makes cancellation/error close the connection forever.
        // A subsequent call cannot consume a late reply or replay an unknown call.
        let (mut stream, capability) = guard.take().ok_or_else(protocol_error)?;
        if let Some(capability) = capability {
            stream.write_all(&capability).await?;
            if stream.read_u8().await? != 1 {
                return Err(protocol_error());
            }
        }
        if id.trim().is_empty()
            || id.len() > 256
            || messages.len() > MAX_MESSAGES
            || tools.len() > MAX_TOOLS
            || context.is_some_and(|c| !c.valid())
        {
            return Err(protocol_error());
        }
        write_frame(
            &mut stream,
            &BorrowedFrame::Model(BorrowedRequest {
                id,
                messages,
                tools,
                context,
            }),
        )
        .await?;
        let mut revision = None;
        let mut frame = read_frame(&mut stream).await?;
        if let FrameReply::Boundary(boundary) = frame {
            if boundary.id != id
                || boundary.instruction_revision == 0
                || boundary.objective.len() > 32_768
                || boundary.messages.len() > 32
                || boundary.context_messages.len() > 32
                || boundary
                    .instructions
                    .as_ref()
                    .is_some_and(|s| s.len() > 4096)
                || boundary
                    .messages
                    .iter()
                    .chain(&boundary.context_messages)
                    .any(|m| {
                        m.text.len() > 4096 || m.sender.len() > 256 || m.sequence > boundary.cursor
                    })
            {
                return Err(protocol_error());
            }
            revision = Some(boundary.instruction_revision);
            write_frame(
                &mut stream,
                &FrameRequest::BoundaryAck {
                    id: boundary.id,
                    cursor: boundary.cursor,
                },
            )
            .await?;
            frame = read_frame(&mut stream).await?;
        }
        let FrameReply::Model(reply) = frame else {
            return Err(protocol_error());
        };
        if reply.id != id {
            return Err(protocol_error());
        }
        let completion = reply.completion.ok_or_else(protocol_error)?;
        if let Some(revision) = revision {
            self.instruction_revision
                .store(revision, std::sync::atomic::Ordering::Release);
        }
        *guard = Some((stream, None));
        Ok(completion)
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn concurrent_completion_is_first_wins_and_exact_replay_only() {
        use tachyon_api::work::{CompletionProposal, Reply, Request};
        let (host, client) = private_pair().unwrap();
        let request = Request::Complete {
            summary: "candidate".into(),
            candidate_refs: vec!["untrusted-reference".into()],
            unresolved_questions: vec!["review required".into()],
        };
        let server = tokio::spawn(async move {
            let mut stream = host.authenticate().await.unwrap();
            let FrameRequest::Work(Request::Complete {
                summary,
                candidate_refs,
                unresolved_questions,
            }) = read_frame(&mut stream).await.unwrap()
            else {
                panic!()
            };
            write_frame(
                &mut stream,
                &FrameReply::Work(Reply::Proposed {
                    proposal: CompletionProposal {
                        summary,
                        candidate_refs,
                        unresolved_questions,
                        instruction_revision: 1,
                    },
                }),
            )
            .await
            .unwrap();
            // Replays and conflicts are resolved locally without another host call.
            assert!(read_frame::<FrameRequest>(&mut stream).await.is_err());
        });
        let (first, duplicate) = tokio::join!(client.work(&request), client.work(&request));
        assert!(matches!(first.unwrap(), Reply::Proposed { .. }));
        assert!(matches!(duplicate.unwrap(), Reply::Proposed { .. }));
        for conflict in [
            Request::Complete {
                summary: "override".into(),
                candidate_refs: vec!["untrusted-reference".into()],
                unresolved_questions: vec!["review required".into()],
            },
            Request::Complete {
                summary: "candidate".into(),
                candidate_refs: vec![],
                unresolved_questions: vec!["review required".into()],
            },
            Request::Complete {
                summary: "candidate".into(),
                candidate_refs: vec!["untrusted-reference".into()],
                unresolved_questions: vec![],
            },
        ] {
            assert!(matches!(
                client.work(&conflict).await.unwrap(),
                Reply::Denied
            ));
        }
        assert_eq!(
            client.completion_proposal().unwrap().unresolved_questions,
            ["review required"]
        );
        drop(client);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn work_reply_requires_exact_action_question_and_resumed_lease() {
        use tachyon_api::work::{Reply, Request};
        for reply in [
            Reply::Answer {
                request_id: "other".into(),
                answer: Some("2".into()),
                resumed: true,
            },
            Reply::Answer {
                request_id: "q".into(),
                answer: Some("2".into()),
                resumed: false,
            },
            Reply::Proposed {
                proposal: tachyon_api::work::CompletionProposal {
                    summary: "forged".into(),
                    candidate_refs: vec![],
                    unresolved_questions: vec![],
                    instruction_revision: 1,
                },
            },
        ] {
            let (host, client) = private_pair().unwrap();
            let server = tokio::spawn(async move {
                let mut stream = host.authenticate().await.unwrap();
                let _: FrameRequest = read_frame(&mut stream).await.unwrap();
                write_frame(&mut stream, &FrameReply::Work(reply))
                    .await
                    .unwrap();
            });
            assert!(client
                .work(&Request::Ask {
                    request_id: "q".into(),
                    question: "?".into(),
                    timeout_ms: 1
                })
                .await
                .is_err());
            assert!(client.work(&Request::Status {}).await.is_err());
            assert!(client.completion_proposal().is_none());
            server.await.unwrap();
        }
    }
    use super::*;

    #[tokio::test]
    async fn upload_cancel_or_wrong_frame_cannot_feed_a_later_call() {
        for cancelled in [true, false] {
            let (host, client) = private_pair().unwrap();
            let (arrived, arrival) = tokio::sync::oneshot::channel();
            let (release, released) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let mut stream = host.authenticate().await.unwrap();
                assert!(matches!(
                    read_frame::<FrameRequest>(&mut stream).await.unwrap(),
                    FrameRequest::ResourceUpload(_)
                ));
                arrived.send(()).unwrap();
                released.await.unwrap();
                let reply = if cancelled {
                    FrameReply::ResourceUpload(UploadReply::Accepted { offset: 0 })
                } else {
                    FrameReply::Model(Reply {
                        id: "wrong-operation".into(),
                        completion: None,
                    })
                };
                let _ = write_frame(&mut stream, &reply).await;
            });
            let request = ResourceUpload::Begin {
                handle: format!("output:{}", uuid::Uuid::new_v4()),
                retained: 0,
                total: 0,
                storage_failed: false,
                sha256: "0".repeat(64),
            };
            let mut upload = Box::pin(client.resource_upload(request.clone()));
            tokio::select! {
                _ = arrival => {},
                _ = &mut upload => panic!("upload completed before barrier"),
            }
            if cancelled {
                drop(upload);
                release.send(()).unwrap();
            } else {
                release.send(()).unwrap();
                assert!(upload.await.is_err());
            }
            server.await.unwrap();
            assert!(client.resource_upload(request).await.is_err());
            assert!(client.chat(&[], &[]).await.is_err());
        }
    }

    #[tokio::test]
    async fn boundary_ack_is_exact_and_revision_requires_successful_completion() {
        for success in [false, true] {
            let (host, client) = private_pair().unwrap();
            let server = async {
                let mut stream = host.authenticate().await.unwrap();
                let FrameRequest::Model(request) = read_frame(&mut stream).await.unwrap() else {
                    panic!()
                };
                write_frame(
                    &mut stream,
                    &FrameReply::Boundary(Boundary {
                        id: request.id.clone(),
                        objective: "immutable".into(),
                        cursor: 7,
                        instruction_revision: 3,
                        instructions: Some("refine".into()),
                        messages: vec![],
                        context_messages: vec![],
                    }),
                )
                .await
                .unwrap();
                assert!(
                    matches!(read_frame::<FrameRequest>(&mut stream).await.unwrap(),
                    FrameRequest::BoundaryAck { id, cursor: 7 } if id == request.id)
                );
                assert_eq!(client.instruction_revision(), None);
                write_frame(
                    &mut stream,
                    &FrameReply::Model(Reply {
                        id: request.id,
                        completion: success.then(|| Completion {
                            text: "ok".into(),
                            tool_calls: vec![],
                            usage: Default::default(),
                            finish_reason: None,
                        }),
                    }),
                )
                .await
                .unwrap();
            };
            let ((), result) = tokio::join!(server, client.chat(&[], &[]));
            assert_eq!(result.is_ok(), success);
            assert_eq!(client.instruction_revision(), success.then_some(3));
        }
        assert!(serde_json::from_value::<FrameRequest>(serde_json::json!({
            "kind":"boundary_ack", "payload":{"id":"x", "cursor":1,"instruction_revision":999}
        }))
        .is_err());
    }

    #[tokio::test]
    async fn control_mismatched_payloads_poison_connection() {
        use tachyon_api::agents::{Reply, Request};
        for (request, reply) in [
            (
                Request::Spawn {
                    template_id: "approved".into(),
                    command_id: "expected".into(),
                },
                Reply::Admitted {
                    command_id: "wrong".into(),
                    work_ids: vec!["child".into()],
                    group_id: None,
                },
            ),
            (
                Request::Spawn {
                    template_id: "approved".into(),
                    command_id: "expected".into(),
                },
                Reply::Admitted {
                    command_id: "expected".into(),
                    work_ids: vec!["child".into()],
                    group_id: Some("unexpected".into()),
                },
            ),
            (
                Request::Group {
                    template_id: "approved".into(),
                    command_id: "expected".into(),
                    max_running: None,
                },
                Reply::Admitted {
                    command_id: "expected".into(),
                    work_ids: vec!["child".into(), "child".into()],
                    group_id: Some("group".into()),
                },
            ),
            (
                Request::Cancel {
                    work_id: "child".into(),
                    generation: 1,
                },
                Reply::CancellationRequested {
                    work_id: "child".into(),
                    generation: 2,
                },
            ),
            (
                Request::GroupStatus {
                    group_id: "owned".into(),
                },
                Reply::Group {
                    group_id: "foreign".into(),
                    revision: 1,
                    max_running: 2,
                    active: 1,
                    total: 3,
                    cancellation_requested: false,
                },
            ),
            (
                Request::GroupResize {
                    group_id: "owned".into(),
                    expected_revision: 1,
                    max_running: 0,
                },
                Reply::Group {
                    group_id: "owned".into(),
                    revision: 1,
                    max_running: 0,
                    active: 1,
                    total: 3,
                    cancellation_requested: false,
                },
            ),
            (
                Request::List {
                    after: None,
                    limit: 1,
                },
                Reply::Result { snapshot: None },
            ),
            (
                Request::List {
                    after: None,
                    limit: 1,
                },
                Reply::List {
                    work_ids: vec!["a".into(), "b".into()],
                    next_cursor: None,
                },
            ),
            (
                Request::Send {
                    work_id: "child".into(),
                    command_id: "expected".into(),
                    text: "hello".into(),
                },
                Reply::Accepted {
                    command_id: "wrong".into(),
                    sequence: 1,
                    accepted_revision: 1,
                },
            ),
        ] {
            let (host, client) = private_pair().unwrap();
            let server = async {
                let mut stream = host.authenticate().await.unwrap();
                let _: FrameRequest = read_frame(&mut stream).await.unwrap();
                write_frame(&mut stream, &FrameReply::Control(reply))
                    .await
                    .unwrap();
                assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
            };
            let worker = async {
                assert!(client.control(&request).await.is_err());
                assert!(client.control(&request).await.is_err());
            };
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                tokio::join!(server, worker);
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn cancelled_mutex_waiter_does_not_poison_inflight_control() {
        use tachyon_api::agents::{Reply, Request};
        let (host, client) = private_pair().unwrap();
        let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let request = Request::List {
            after: None,
            limit: 1,
        };
        let server = async {
            let mut stream = host.authenticate().await.unwrap();
            let _: FrameRequest = read_frame(&mut stream).await.unwrap();
            seen_tx.send(()).unwrap();
            release_rx.await.unwrap();
            for n in 0..2 {
                if n == 1 {
                    let _: FrameRequest = read_frame(&mut stream).await.unwrap();
                }
                write_frame(&mut stream, &FrameReply::Control(Reply::Denied))
                    .await
                    .unwrap();
            }
        };
        let first = async {
            assert!(client.control(&request).await.is_ok());
        };
        let waiter = async {
            seen_rx.await.unwrap();
            assert!(tokio::time::timeout(
                std::time::Duration::from_millis(10),
                client.control(&request)
            )
            .await
            .is_err());
            release_tx.send(()).unwrap();
            assert!(client.control(&request).await.is_ok());
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(server, first, waiter);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn control_wrong_reply_and_cancellation_poison_connection() {
        for wrong_reply in [true, false] {
            let (host, client) = private_pair().unwrap();
            let server = async {
                let mut stream = host.authenticate().await.unwrap();
                assert!(matches!(
                    read_frame::<FrameRequest>(&mut stream).await.unwrap(),
                    FrameRequest::Control(_)
                ));
                if wrong_reply {
                    write_frame(
                        &mut stream,
                        &FrameReply::Model(Reply {
                            id: "unexpected".into(),
                            completion: None,
                        }),
                    )
                    .await
                    .unwrap();
                }
                assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
            };
            let worker = async {
                let request = tachyon_api::agents::Request::List {
                    after: None,
                    limit: 1,
                };
                let result = tokio::time::timeout(
                    std::time::Duration::from_millis(50),
                    client.control(&request),
                )
                .await;
                if wrong_reply {
                    assert!(result.unwrap().is_err());
                } else {
                    assert!(result.is_err());
                }
                assert!(client.control(&request).await.is_err());
                assert!(client.chat(&[], &[]).await.is_err());
            };
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                tokio::join!(server, worker);
            })
            .await
            .unwrap();
        }
        for value in [
            serde_json::json!({"id":"old","messages":[],"tools":[]}),
            serde_json::json!({"kind":"control","payload":{"action":"status","work_id":"w","actor":"forged"}}),
            serde_json::json!({"kind":"control","payload":{"action":"list","limit":1},"authority":true}),
        ] {
            assert!(serde_json::from_value::<FrameRequest>(value).is_err());
        }
    }

    #[tokio::test]
    async fn research_reply_validation_fences_mismatches_and_channel_reuse() {
        use tachyon_api::{agents, context::*};
        for case in [
            "valid",
            "work",
            "id",
            "version",
            "kind",
            "offset",
            "next",
            "overflow",
            "bytes",
            "limit",
            "cursor",
            "count",
            "query_kind",
            "query_version",
            "query_time",
            "query_literal",
            "frame_bound",
        ] {
            let reference = ResourceRef {
                kind: ResourceKind::Artifact,
                work_id: "work".into(),
                id: "candidate".into(),
                version: "a".repeat(64),
            };
            let mut request = Request::Read {
                resource: reference.clone(),
                offset: 2,
                limit: 1,
            };
            let mut page = Page {
                resources: vec![Resource {
                    reference,
                    occurred_at_ms: Some(10),
                    data: serde_json::json!({"bytes":[255], "offset":2, "next_offset":3}),
                }],
                next_cursor: None,
            };
            match case {
                "work" => page.resources[0].reference.work_id = "foreign".into(),
                "id" => page.resources[0].reference.id = "foreign".into(),
                "version" => page.resources[0].reference.version = "b".repeat(64),
                "kind" => page.resources[0].reference.kind = ResourceKind::Finding,
                "offset" => page.resources[0].data["offset"] = 1.into(),
                "next" => page.resources[0].data["next_offset"] = 4.into(),
                "overflow" => {
                    if let Request::Read { offset, .. } = &mut request {
                        *offset = u64::MAX;
                    }
                    page.resources[0].data["offset"] = u64::MAX.into();
                    page.resources[0].data["next_offset"] = 0.into();
                }
                "bytes" => page.resources[0].data["bytes"] = serde_json::json!([256]),
                "limit" => page.resources[0].data["bytes"] = serde_json::json!([0, 0]),
                "cursor" => page.next_cursor = Some("cursor".into()),
                "count" => page.resources.push(page.resources[0].clone()),
                "frame_bound" => {
                    let size = serde_json::to_vec(&FrameReply::Control(agents::Reply::Resource {
                        page: page.clone(),
                    }))
                    .unwrap()
                    .len();
                    // Reply alone fits, but its required transport wrapper does not.
                    page.resources[0].data["padding"] = "x".repeat(MAX_PAGE_BYTES - size).into();
                }
                c if c.starts_with("query_") => {
                    let query = Query {
                        literal: (c == "query_literal").then(|| "missing".into()),
                        version: (c == "query_version").then(|| "wrong".into()),
                        since_ms: (c == "query_time").then_some(11),
                        after: None,
                        limit: 1,
                    };
                    request = if c == "query_kind" {
                        Request::Attempts { query }
                    } else {
                        Request::Search { query }
                    };
                }
                _ => {}
            }
            let (host, client) = private_pair().unwrap();
            let server = async {
                let mut stream = host.authenticate().await.unwrap();
                for _ in 0..if case == "valid" { 2 } else { 1 } {
                    let _: FrameRequest = read_frame(&mut stream).await.unwrap();
                    write_frame(
                        &mut stream,
                        &FrameReply::Control(agents::Reply::Resource { page: page.clone() }),
                    )
                    .await
                    .unwrap();
                }
            };
            let worker = async {
                let request = agents::Request::Resource { request };
                assert_eq!(
                    client.control(&request).await.is_ok(),
                    case == "valid",
                    "{case}"
                );
                assert_eq!(
                    client.control(&request).await.is_ok(),
                    case == "valid",
                    "{case}"
                );
            };
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                tokio::join!(server, worker);
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn wait_reply_requires_resumed_and_exact_reference_partition() {
        use tachyon_api::agents::{Reply, Request, WaitMode};
        for (completed, outstanding, resumed) in [
            (vec!["child"], vec![], false),
            (vec!["foreign"], vec![], true),
            (vec!["child"], vec!["child"], true),
            (vec![], vec![], true),
        ] {
            let (host, client) = private_pair().unwrap();
            let server = async {
                let mut stream = host.authenticate().await.unwrap();
                let _: FrameRequest = read_frame(&mut stream).await.unwrap();
                write_frame(
                    &mut stream,
                    &FrameReply::Control(Reply::Wait {
                        completed: completed.into_iter().map(String::from).collect(),
                        outstanding: outstanding.into_iter().map(String::from).collect(),
                        resumed,
                        resource_blocked: false,
                    }),
                )
                .await
                .unwrap();
                assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
            };
            let worker = async {
                assert!(client
                    .control(&Request::Wait {
                        work_ids: vec!["child".into()],
                        mode: WaitMode::All,
                        timeout_ms: 1
                    })
                    .await
                    .is_err());
                assert!(client.chat(&[], &[]).await.is_err());
            };
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                tokio::join!(server, worker);
            })
            .await
            .unwrap();
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn listener_is_private_one_use_and_checks_peer() {
        use std::os::unix::fs::PermissionsExt;
        for wrong_pid in [false, true] {
            let listener = PrivateListener::bind().unwrap();
            let path = listener.directory.path().to_owned();
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o700
            );
            let stream = UnixStream::connect(path.join("model.sock")).await.unwrap();
            let uid = stream.peer_cred().unwrap().uid();
            let result = listener
                .accept(
                    if wrong_pid {
                        std::process::id() + 1
                    } else {
                        std::process::id()
                    },
                    if wrong_pid { uid } else { uid.wrapping_add(1) },
                )
                .await;
            assert!(result.is_err());
            assert!(!path.exists());
        }
        let listener = PrivateListener::bind().unwrap();
        let path = listener.directory.path().to_owned();
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(10),
            listener.accept(std::process::id(), 0)
        )
        .await
        .is_err());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn malformed_frames_and_counts_are_bounded() {
        for size in [0, MAX_FRAME as u32 + 1, u32::MAX] {
            let (mut a, mut b) = UnixStream::pair().unwrap();
            a.write_u32(size).await.unwrap();
            assert!(read_frame::<Request>(&mut b).await.is_err());
        }
        for bytes in [
            b"{".as_slice(),
            br#"{"id":"x","messages":[],"tools":[],"model":"forged"}"#,
            br#"{"id":"x","id":"forged","messages":[],"tools":[]}"#,
        ] {
            let (mut a, mut b) = UnixStream::pair().unwrap();
            a.write_u32(bytes.len() as u32).await.unwrap();
            a.write_all(bytes).await.unwrap();
            assert!(read_frame::<Request>(&mut b).await.is_err());
        }
        let mut request = Request {
            context: None,
            id: "x".into(),
            messages: vec![],
            tools: vec![],
        };
        request.messages = vec![ChatMessage::new(crate::Role::User, ""); MAX_MESSAGES + 1];
        assert!(request.validate().is_err());
        let (mut a, _) = UnixStream::pair().unwrap();
        assert!(write_frame(&mut a, &"x".repeat(MAX_FRAME)).await.is_err());
    }

    #[tokio::test]
    async fn oversized_tools_fail_before_sending_a_frame() {
        for tools in [
            vec![ToolSpec::new("x", "", serde_json::json!({})); MAX_TOOLS + 1],
            vec![ToolSpec::new(
                "x",
                "",
                serde_json::json!({"schema": "x".repeat(MAX_FRAME)}),
            )],
        ] {
            let (host, client) = private_pair().unwrap();
            let server = async {
                let mut stream = host.authenticate().await.unwrap();
                assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
            };
            let ((), result) = tokio::join!(server, client.chat(&[], &tools));
            assert!(result.is_err());
            assert!(client.chat(&[], &[]).await.is_err());
        }
    }

    #[tokio::test]
    async fn oversized_reply_and_cancel_never_reuse_old_stream() {
        for cancel in [false, true] {
            let (host, client) = private_pair().unwrap();
            let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
            let server = async {
                let mut stream = host.authenticate().await.unwrap();
                let _: FrameRequest = read_frame(&mut stream).await.unwrap();
                seen_tx.send(()).unwrap();
                if !cancel {
                    stream.write_u32(MAX_FRAME as u32 + 1).await.unwrap();
                    let _ = stream.write_all(b"old reply tail").await;
                }
                let result = stream.read(&mut [0]).await;
                assert!(matches!(result, Ok(0) | Err(_)));
            };
            let worker = async {
                if cancel {
                    let call = client.chat(&[], &[]);
                    tokio::pin!(call);
                    tokio::select! {
                        _ = &mut call => panic!("request should be pending"),
                        _ = seen_rx => {}
                    }
                } else {
                    assert!(client.chat(&[], &[]).await.is_err());
                }
                assert!(client.chat(&[], &[]).await.is_err());
            };
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                tokio::join!(server, worker);
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn cross_pair_handshake_and_wrong_reply_fail_closed() {
        let (host, client) = private_pair().unwrap();
        let (_, other) = private_pair().unwrap();
        let (_, other_capability) = other.connection.lock().await.take().unwrap();
        client.connection.lock().await.as_mut().unwrap().1 = other_capability;
        let (host_result, client_result) = tokio::join!(host.authenticate(), client.chat(&[], &[]));
        assert!(host_result.is_err());
        assert!(client_result.is_err());
        assert!(client.chat(&[], &[]).await.is_err());

        let (host, client) = private_pair().unwrap();
        let server = async {
            let mut stream = host.authenticate().await.unwrap();
            let _: FrameRequest = read_frame(&mut stream).await.unwrap();
            write_frame(
                &mut stream,
                &FrameReply::Model(Reply {
                    id: "wrong".into(),
                    completion: None,
                }),
            )
            .await
            .unwrap();
        };
        let ((), result) = tokio::join!(server, client.chat(&[], &[]));
        assert!(result.is_err());
        assert!(client.chat(&[], &[]).await.is_err());
    }
}
