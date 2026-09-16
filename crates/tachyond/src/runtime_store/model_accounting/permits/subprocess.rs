//! Explicit host-only, one-shot launch. No campaign scheduler or IPC entry point.
use super::*;
use std::path::Path;
use tachyon_api::types::{EventEnvelope, WorkRequest};
use tachyon_model::broker::{protocol_error, write_frame, PrivateListener, MAX_FRAME};
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

const LAUNCHES: TableDefinition<&str, &str> = TableDefinition::new("campaign_ghost_launches");

pub(in crate::runtime_store) struct LaunchCapacity {
    resident: crate::runtime_store::host_capacity::CapacityPermit,
    execution: crate::runtime_store::host_capacity::CapacityPermit,
}

fn validate_execution_in(
    tx: &redb::WriteTransaction,
    funding: &AdmittedWork,
    reservation: &RequestReservation,
    work: &WorkRequest,
) -> tachyon_model::Result<()> {
    use crate::runtime_store::execution::{decode_record, ExecutionPhase, EXECUTIONS};
    let executions = tx.open_table(EXECUTIONS).map_err(err)?;
    if let Some(row) = executions.get(work.work_id.as_str()).map_err(err)? {
        let current = decode_record(row.value(), &work.work_id).map_err(err)?;
        if current.policy.work != *work
            || current.policy.funding != *funding
            || current.policy.model != *reservation
            || current.phase != ExecutionPhase::ExecutingUnknown
            || current.settled
        {
            return Err(protocol_error());
        }
    } else if work.attempt.is_some() {
        return Err(protocol_error());
    }
    Ok(())
}

struct LaunchGuard {
    capacity_identity: String,
    capacity: Option<crate::runtime_store::host_capacity::CapacityPermit>,
    execution: Option<crate::runtime_store::host_capacity::CapacityPermit>,
    child: tokio::process::Child,
    pid: Option<nix::unistd::Pid>,
    lease: PermitLease,
    permit: ModelPermit,
}

impl Drop for LaunchGuard {
    fn drop(&mut self) {
        if let Some(pid) = self.pid {
            if let Some(execution) = self.execution.take() {
                execution.retain_identity(&self.capacity_identity);
            }
            if let Some(capacity) = self.capacity.take() {
                capacity.retain_identity(&self.capacity_identity);
            }
            let _ = nix::sys::signal::killpg(pid, nix::sys::signal::Signal::SIGKILL);
            let _ = self.child.start_kill();
        }
        self.lease.close();
    }
}

impl ModelBroker {
    pub(in crate::runtime_store) async fn admit_launch(
        &self,
        funding: &AdmittedWork,
        work: &WorkRequest,
        deadline: Instant,
    ) -> tachyon_model::Result<LaunchCapacity> {
        let resident = self
            .admit_capacity(funding, work, deadline, &self.store.host_capacity.resident)
            .await?;
        let execution = self
            .admit_capacity(funding, work, deadline, &self.store.host_capacity.execution)
            .await?;
        Ok(LaunchCapacity {
            resident,
            execution,
        })
    }

    async fn admit_capacity(
        &self,
        funding: &AdmittedWork,
        work: &WorkRequest,
        deadline: Instant,
        capacity: &Arc<crate::runtime_store::host_capacity::FairCapacity>,
    ) -> tachyon_model::Result<crate::runtime_store::host_capacity::CapacityPermit> {
        let key = (
            funding.admission.campaign_id.clone(),
            work.work_id.clone(),
            work.generation,
        );
        let mut cancellation = self
            .launches
            .lock()
            .map_err(|_| protocol_error())?
            .get(&key)
            .map(|sender| sender.subscribe());
        let cancelled = async {
            if let Some(receiver) = &mut cancellation {
                while !*receiver.borrow_and_update() {
                    if receiver.changed().await.is_err() {
                        break;
                    }
                }
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::select! {
            biased;
            _ = cancelled => Err(err("host capacity admission cancelled before spawn")),
            result = tokio::time::timeout_at(deadline, capacity.acquire(&funding.admission.campaign_id)) =>
                result.map_err(|_| err("host capacity admission deadline elapsed"))?.map_err(err),
        }
    }

    /// The host must first register this worker identity through admission. Claims
    /// survive spawn failure/restart: unknown launch outcomes never authorize replay.
    #[allow(dead_code, clippy::too_many_arguments)]
    pub(crate) async fn launch_private(
        &self,
        executable: &Path,
        workspace: &Path,
        home: &Path,
        funding: AdmittedWork,
        reservation: RequestReservation,
        work: WorkRequest,
        deadline: Instant,
    ) -> tachyon_model::Result<Vec<EventEnvelope>> {
        self.launch_private_admitted(
            executable,
            workspace,
            home,
            funding,
            reservation,
            work,
            deadline,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime_store) async fn launch_private_admitted(
        &self,
        executable: &Path,
        workspace: &Path,
        home: &Path,
        funding: AdmittedWork,
        reservation: RequestReservation,
        work: WorkRequest,
        deadline: Instant,
        capacity: Option<LaunchCapacity>,
    ) -> tachyon_model::Result<Vec<EventEnvelope>> {
        use crate::runtime_store::admission::DispatchState;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| protocol_error())?
            .as_millis();
        if deadline <= Instant::now()
            || u128::from(work.deadline_ms) <= now_ms
            || work.work_id != funding.admission.work_id
            || work.generation != funding.admission.generation
            || work.objective != funding.admission.objective
            || work.assignment == 0
            || work.context_refs.len() > 4
            || work.context_refs.iter().any(|r| !r.valid())
            || work
                .attempt
                .as_ref()
                .is_some_and(|a| a.id != reservation.identity.attempt_id)
            || !executable.is_absolute()
            || !workspace.is_absolute()
            || !home.is_absolute()
        {
            return Err(protocol_error());
        }
        let deadline = deadline.min(
            Instant::now()
                + std::time::Duration::from_millis(
                    (u128::from(work.deadline_ms) - now_ms).min(u128::from(u64::MAX)) as u64,
                ),
        );
        let capacity = match capacity {
            Some(capacity) => capacity,
            None => self.admit_launch(&funding, &work, deadline).await?,
        };
        // The waiter owns liveness even before issuance: a late blocking result
        // cannot leave live authority behind if this launch future is dropped.
        let lease = PermitLease(Arc::new(false.into()));
        let closed = lease.0.clone();
        let store = self.store.clone();
        let prepared_funding = funding.clone();
        let prepared_work = work.clone();
        let prepared_reservation = reservation.clone();
        let artifacts = self.research_artifacts.clone();
        let trace_workspace = workspace.to_owned();
        let trace_home = home.to_owned();
        let preparation = tokio::task::spawn_blocking(move || {
            let parent = store
                .trace_root
                .parent()
                .ok_or_else(protocol_error)?
                .canonicalize()
                .map_err(err)?;
            let trace_root = parent.join(store.trace_root.file_name().ok_or_else(protocol_error)?);
            for path in [trace_workspace, trace_home] {
                let path = path.canonicalize().map_err(err)?;
                if trace_root.starts_with(&path) || path.starts_with(&trace_root) {
                    return Err(protocol_error());
                }
            }
            let funding = prepared_funding;
            let work = prepared_work;
            for resource in &work.context_refs {
                store
                    .host_research_context(
                        &funding.admission.campaign_id,
                        &tachyon_api::context::Request::Read {
                            resource: resource.clone(),
                            offset: 0,
                            limit: 1,
                        },
                        artifacts.as_deref(),
                    )
                    .map_err(err)?;
            }
            let worker_id = {
                let write = store.database.begin_write().map_err(err)?;
                RuntimeStore::admitted_funding_in(&write, &funding).map_err(err)?;
                validate_execution_in(&write, &funding, &prepared_reservation, &work)?;
                let registered =
                    RuntimeStore::admitted_work_in(&write, &work.work_id).map_err(err)?;
                let DispatchState::Registered { worker_id } = registered.state else {
                    return Err(protocol_error());
                };
                worker_id
            };
            let replacement = if work.attempt.is_some() {
                let state = store.model_permits.lock().map_err(|_| protocol_error())?;
                match state.current.get(&work.work_id) {
                    Some(id) => {
                        let grant = state.grants.get(id).ok_or_else(protocol_error)?;
                        if !grant.closed.load(std::sync::atomic::Ordering::Acquire) {
                            return Err(protocol_error());
                        }
                        Some(ModelPermit(*id))
                    }
                    None => None,
                }
            } else {
                None
            };
            let permit = store.issue_model_permit(
                prepared_reservation.clone(),
                funding.clone(),
                replacement.as_ref(),
                closed,
            )?;
            let claim = (|| {
                let write = store.database.begin_write().map_err(err)?;
                RuntimeStore::admitted_funding_in(&write, &funding).map_err(err)?;
                let ledger =
                    RuntimeStore::campaign_ledger_in(&write, &funding.admission.campaign_id)
                        .map_err(err)?;
                let hold = ledger
                    .reservations
                    .get(&funding.dispatch_id)
                    .ok_or_else(protocol_error)?;
                if ledger.admissions_paused
                    || hold.cancellation_requested
                    || hold.usage != Usage::Unknown
                {
                    return Err(protocol_error());
                }
                // Typed attempts are host-authorized by the current durable execution,
                // not by choosing a new launch key (or dropping the attempt field).
                validate_execution_in(&write, &funding, &prepared_reservation, &work)?;
                let mut launches = write.open_table(LAUNCHES).map_err(err)?;
                let launch_key = match &work.attempt {
                    Some(attempt) => {
                        serde_json::to_string(&(&funding.dispatch_id, &attempt.id)).map_err(err)?
                    }
                    None => funding.dispatch_id.clone(),
                };
                if launches.get(launch_key.as_str()).map_err(err)?.is_some() {
                    return Err(protocol_error());
                }
                launches
                    .insert(launch_key.as_str(), worker_id.as_str())
                    .map_err(err)?;
                drop(launches);
                write
                    .open_table(crate::runtime_store::research_context::traces::TRACE_ASSIGNMENTS)
                    .map_err(err)?
                    .insert(
                        (
                            funding.admission.campaign_id.as_str(),
                            work.work_id.as_str(),
                            prepared_reservation.identity.attempt_id.as_str(),
                        ),
                        serde_json::to_vec(&(&work, &worker_id))
                            .map_err(err)?
                            .as_slice(),
                    )
                    .map_err(err)?;
                write.commit().map_err(err)
            })();
            if let Err(error) = claim {
                let _ = store.host_revoke_model_permit(&permit);
                return Err(error);
            }
            Ok((worker_id, permit))
        });
        let (worker_id, permit) = tokio::time::timeout_at(deadline, preparation)
            .await
            .map_err(|_| protocol_error())?
            .map_err(err)??;
        let cancelled = self
            .launches
            .lock()
            .map_err(|_| protocol_error())?
            .get(&(
                funding.admission.campaign_id.clone(),
                work.work_id.clone(),
                work.generation,
            ))
            .is_some_and(|sender| *sender.borrow());
        if cancelled {
            // Preparation committed, but this owner knows no process was spawned.
            return Ok(Vec::new());
        }
        let listener = PrivateListener::bind()?;
        let mut command = tokio::process::Command::new(executable);
        command
            .arg("--broker")
            .arg("--agent-id")
            .arg(&worker_id)
            .arg("--cwd")
            .arg(workspace)
            .current_dir(workspace)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", home)
            .env("LANG", "C.UTF-8")
            .env("GHOST_BROKER_SESSION", "1")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .process_group(0);
        if Instant::now() >= deadline {
            return Err(protocol_error());
        }
        let child = match command.spawn() {
            Ok(child) => child,
            Err(_) => {
                return Err(protocol_error());
            }
        };
        let pid = child.id().ok_or_else(protocol_error)?;
        let pid = nix::unistd::Pid::from_raw(pid as i32);
        let mut guard = LaunchGuard {
            capacity_identity: crate::runtime_store::host_capacity::identity(
                &funding.admission.campaign_id,
                &work.work_id,
                &reservation.identity.attempt_id,
                work.generation,
                &funding.dispatch_id,
            ),
            capacity: Some(capacity.resident),
            execution: Some(capacity.execution),
            child,
            pid: Some(pid),
            lease,
            permit,
        };
        let run = async {
            let mut stdin = guard.child.stdin.take().ok_or_else(protocol_error)?;
            listener
                .bootstrap_controls(&mut stdin, self.allowed_controls.iter().copied().collect())
                .await?;
            write_frame(&mut stdin, &work).await?;
            let channel = listener
                .accept(pid.as_raw() as u32, nix::unistd::geteuid().as_raw())
                .await?;
            let mut stdout = guard.child.stdout.take().ok_or_else(protocol_error)?;
            if let Some(expected) = work.attempt.as_ref().and_then(|a| a.continuation.as_ref()) {
                let observed = async {
                    use sha2::{Digest, Sha256};
                    let mut file = tokio::fs::File::open(format!("/proc/{}/exe", pid.as_raw())).await
                        .map_err(|_| err("continuation configuration mismatch: launched binary attestation unavailable"))?;
                    let mut hash = Sha256::new();
                    let mut buffer = vec![0u8; 65536];
                    loop {
                        let n = file.read(&mut buffer).await
                            .map_err(|_| err("continuation configuration mismatch: launched binary attestation unavailable"))?;
                        if n == 0 {
                            break;
                        }
                        hash.update(&buffer[..n]);
                    }
                    Ok::<_, tachyon_model::ModelError>(format!("{:x}", hash.finalize()))
                };
                let handshake = async {
                    tachyon_model::broker::read_frame(&mut stdout).await
                        .map_err(|_| err("continuation configuration mismatch: missing or invalid bootstrap attestation"))
                };
                let (actual, observed): (tachyon_api::continuation::ContinuationBootstrap, String) =
                    tokio::try_join!(handshake, observed)?;
                if observed != expected.executable_sha256 {
                    return Err(err(
                        "continuation configuration mismatch: actual launched binary build pin",
                    ));
                }
                expected.validate_observed(&actual).map_err(err)?;
                write_frame(&mut stdin, &true).await?;
            }
            drop(stdin);
            let output = async {
                let mut stdout = tokio::io::BufReader::new(stdout);
                let mut events = Vec::new();
                let mut retained = 0usize;
                let mut semantic_bytes = 0usize;
                let mut seen = std::collections::BTreeMap::new();
                loop {
                    let mut bytes = Vec::new();
                    let bound = if stdout.fill_buf().await?.first() == Some(&b'{') {
                        64 * 1024 * 1024
                    } else {
                        MAX_FRAME
                    };
                    let n = (&mut stdout)
                        .take(bound as u64 + 1)
                        .read_until(b'\n', &mut bytes)
                        .await?;
                    if n == 0 {
                        break;
                    }
                    if n > bound {
                        return Err(protocol_error());
                    }
                    let parsed = parse_events(&bytes, &worker_id)?;
                    for event in parsed {
                        use sha2::{Digest, Sha256};
                        let hash = Sha256::digest(&bytes);
                        if let Some(previous) = seen.insert(event.event_id, hash) {
                            if previous != hash {
                                return Err(protocol_error());
                            }
                            continue;
                        }
                        if seen.len() > 20_000 {
                            return Err(protocol_error());
                        }
                        let store = self.store.clone();
                        let funding = funding.clone();
                        let work = work.clone();
                        let attempt = reservation.identity.attempt_id.clone();
                        let worker = worker_id.clone();
                        let mut trace = event.clone();
                        match &mut trace.kind {
                            tachyon_api::types::AgentEvent::ToolStarted { arguments, .. } => {
                                *arguments = self.model.redact_trace(arguments)
                            }
                            tachyon_api::types::AgentEvent::ToolFinished { output, .. } => {
                                *output = self.model.redact_trace(output)
                            }
                            _ => {}
                        }
                        tokio::task::spawn_blocking(move || {
                            store.record_tool_trace(&funding, &work, &attempt, &worker, &trace)
                        })
                        .await
                        .map_err(err)?
                        .map_err(err)?;
                        // Raw tool bytes are durable already. Keep the existing collection bound
                        // without throwing away semantic Work transitions after verbose tools.
                        let raw = matches!(
                            event.kind,
                            tachyon_api::types::AgentEvent::ToolStarted { .. }
                                | tachyon_api::types::AgentEvent::ToolFinished { .. }
                        );
                        if retained.saturating_add(n) > MAX_FRAME && raw {
                            continue;
                        }
                        if !raw {
                            semantic_bytes = semantic_bytes.saturating_add(n);
                            if semantic_bytes > MAX_FRAME {
                                return Err(protocol_error());
                            }
                        }
                        retained = retained.saturating_add(n);
                        if events.len() >= 20_000 {
                            return Err(protocol_error());
                        }
                        events.push(event);
                    }
                }
                if serde_json::to_vec(&events).map_err(err)?.len() > MAX_FRAME {
                    events.retain(|event| {
                        !matches!(
                            event.kind,
                            tachyon_api::types::AgentEvent::ToolStarted { .. }
                                | tachyon_api::types::AgentEvent::ToolFinished { .. }
                        )
                    });
                    if serde_json::to_vec(&events).map_err(err)?.len() > MAX_FRAME {
                        return Err(protocol_error());
                    }
                }
                Ok(events)
            };
            let (_, events) = tokio::try_join!(
                async {
                    // Clean worker EOF is also a channel error. Output failures,
                    // however, must cancel provider I/O without waiting for EOF.
                    let _ = self
                        .serve_private_admitted(
                            channel,
                            &guard.permit,
                            reservation.clone(),
                            deadline,
                            &mut guard.execution,
                        )
                        .await;
                    let campaign = funding.admission.campaign_id.clone();
                    let work_id = work.work_id.clone();
                    let state = self
                        .store
                        .storage(move |store| store.campaign_work_status(&campaign, &work_id))
                        .await;
                    if state.is_ok_and(|s| s.wait.is_some() || s.cancellation_requested) {
                        return Err(protocol_error());
                    }
                    Ok::<_, tachyon_model::ModelError>(())
                },
                output
            )?;
            // Observe exit without reaping: the group ID cannot be recycled before
            // descendant cleanup. Tokio remains the sole owner of the final wait.
            use nix::sys::wait::{waitid, Id, WaitPidFlag, WaitStatus};
            loop {
                match waitid(
                    Id::Pid(pid),
                    WaitPidFlag::WEXITED | WaitPidFlag::WNOWAIT | WaitPidFlag::WNOHANG,
                )
                .map_err(|_| protocol_error())?
                {
                    WaitStatus::StillAlive => {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await
                    }
                    _ => break,
                }
            }
            Ok(events)
        };
        let key = (
            funding.admission.campaign_id.clone(),
            work.work_id.clone(),
            work.generation,
        );
        let mut cancellation = self
            .launches
            .lock()
            .map_err(|_| protocol_error())?
            .get(&key)
            .map(|sender| sender.subscribe());
        let cancelled = async {
            if let Some(receiver) = &mut cancellation {
                if !*receiver.borrow() {
                    let _ = receiver.changed().await;
                }
            } else {
                std::future::pending::<()>().await;
            }
        };
        let (result, cancelled) = tokio::select! {
            biased;
            _ = cancelled => (Ok(Vec::new()), true),
            result = tokio::time::timeout_at(deadline, run) =>
                (result.map_err(|_| protocol_error()).and_then(|r| r), false),
        };
        // Kill the entire group before reaping its leader, even after normal EOF.
        let cleanup = nix::sys::signal::killpg(pid, nix::sys::signal::Signal::SIGKILL);
        let _ = guard.child.start_kill();
        guard.lease.close();
        let status = tokio::time::timeout(std::time::Duration::from_secs(1), guard.child.wait())
            .await
            .map_err(|_| protocol_error())??;
        match cleanup {
            Ok(()) | Err(nix::errno::Errno::ESRCH) => {}
            Err(_) => return Err(protocol_error()),
        }
        guard.pid = None;
        drop(guard.execution.take());
        drop(guard.capacity.take());
        if result.is_err() || cancelled || !status.success() {
            let campaign = funding.admission.campaign_id.clone();
            let work_id = work.work_id.clone();
            let attempt = reservation.identity.attempt_id.clone();
            self.store.storage(move |store| {
                store.record_context_object(&campaign, &work_id, &attempt, "work-end",
                    "output_retention_gap", br#"{"full_retention":false,"reason":"work failed or cancelled; unexported live outputs are unavailable"}"#)
            }).await.map_err(err)?;
        }
        let parked = self
            .store
            .storage(move |store| {
                let parked =
                    store.campaign_work_status(&funding.admission.campaign_id, &work.work_id);
                if parked.is_ok_and(|s| s.wait.is_some() || s.cancellation_requested) {
                    store.host_cancel_work(
                        &funding.admission.campaign_id,
                        &work.work_id,
                        work.generation,
                    )?;
                    // Only the launch owner, after kill/reap, can acknowledge termination.
                    // Empty evidence marks the execution unverified, never replays a cell.
                    store.host_acknowledge_work_terminal(&funding)?;
                    return Ok(true);
                }
                Ok(false)
            })
            .await
            .map_err(err)?;
        if parked {
            return Ok(Vec::new());
        }
        if result.is_ok() && !cancelled && !status.success() {
            // Parsed output plus kill/reap proves cleanup, not a valid candidate
            // or final billing. Let the execution owner record Unverified while
            // preserving unknown charges, rather than strand an active lease.
            return result.map(|mut events| {
                for event in &mut events {
                    if let tachyon_api::types::AgentEvent::WorkCandidate { candidate } =
                        &mut event.kind
                    {
                        candidate.outcome = tachyon_api::types::WorkOutcome::Failed {
                            message: "worker exited unsuccessfully".into(),
                        };
                    }
                }
                events
            });
        }
        result
    }
}

fn parse_events(bytes: &[u8], worker_id: &str) -> tachyon_model::Result<Vec<EventEnvelope>> {
    let text = std::str::from_utf8(bytes).map_err(|_| protocol_error())?;
    let mut events = Vec::new();
    for line in text.lines().filter(|line| line.starts_with('{')) {
        let event: EventEnvelope = serde_json::from_str(line).map_err(|_| protocol_error())?;
        if event.session_id != worker_id
            || !matches!(&event.actor, tachyon_api::types::Actor::Worker { id } if id == worker_id)
            || event.task_id.as_deref() != Some(worker_id)
            || event.conversation_id.is_some()
            || event.parent_task_id.is_some()
        {
            return Err(protocol_error());
        }
        events.push(event);
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn drop_kills_group_without_waiting_for_authority_or_storage() {
        let (_dir, store, funding, request) = super::super::tests::setup();
        let lease = PermitLease(Arc::new(false.into()));
        let permit = store
            .issue_model_permit(request, funding, None, lease.0.clone())
            .unwrap();
        let closed = lease.0.clone();
        let mut command = tokio::process::Command::new("/bin/sleep");
        let child = command
            .arg("30")
            .process_group(0)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = nix::unistd::Pid::from_raw(child.id().unwrap() as i32);
        let guard = LaunchGuard {
            capacity_identity: "test-guard".into(),
            capacity: Some(store.host_capacity.resident.acquire("test").await.unwrap()),
            execution: Some(store.host_capacity.execution.acquire("test").await.unwrap()),
            child,
            pid: Some(pid),
            lease,
            permit,
        };
        let store = Arc::new(store);
        let blocked = store.clone();
        let (ready, started) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let locks = tokio::task::spawn_blocking(move || {
            let _authority = blocked.model_permits.lock().unwrap();
            let _write = blocked.database.begin_write().unwrap();
            ready.send(()).unwrap();
            let _ = wait.recv_timeout(std::time::Duration::from_secs(3));
        });
        started.await.unwrap();
        let start = Instant::now();
        drop(guard);
        let elapsed = start.elapsed();
        assert_eq!(
            store.host_capacity.execution.available(),
            store.host_capacity.limits.max_execution_jobs - 1
        );
        assert_eq!(
            store.host_capacity.resident.available(),
            store.host_capacity.limits.max_resident_workers - 1,
            "drop-only cleanup retains the slot even if the kill subsequently succeeds"
        );
        assert!(closed.load(std::sync::atomic::Ordering::Acquire));
        let killed = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while nix::sys::signal::killpg(pid, None).is_ok() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await;
        let _ = release.send(());
        locks.await.unwrap();
        assert!(elapsed < std::time::Duration::from_millis(100));
        killed.unwrap();
        assert_eq!(
            store.host_capacity.execution.available(),
            store.host_capacity.limits.max_execution_jobs - 1,
            "late process exit does not restore an uncertain execution lease"
        );
    }

    #[test]
    fn worker_events_cannot_spoof_envelope_identity() {
        let valid = serde_json::json!({
            "event_id": 1, "sequence": 1, "occurred_at_ms": 0,
            "session_id": "worker", "task_id": "worker",
            "actor": {"kind": "worker", "id": "worker"},
            "kind": "reply", "text": "ok", "final_reply": true
        });
        assert_eq!(
            parse_events(&serde_json::to_vec(&valid).unwrap(), "worker")
                .unwrap()
                .len(),
            1
        );
        for (field, value) in [
            ("session_id", serde_json::json!("other")),
            ("task_id", serde_json::json!("other")),
            ("task_id", serde_json::Value::Null),
            ("actor", serde_json::json!({"kind": "system"})),
            (
                "actor",
                serde_json::json!({"kind": "worker", "id": "other"}),
            ),
            ("conversation_id", serde_json::json!("other")),
            ("parent_task_id", serde_json::json!("other")),
        ] {
            let mut forged = valid.clone();
            forged[field] = value;
            assert!(
                parse_events(&serde_json::to_vec(&forged).unwrap(), "worker").is_err(),
                "{field}"
            );
        }
        assert!(parse_events(b"{malformed", "worker").is_err());
        assert!(parse_events(&[0xff], "worker").is_err());
    }
}
