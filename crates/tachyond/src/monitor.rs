//! One bounded, latest-value sampler per daemon. Never holds registry/cache locks during I/O.
use crate::{
    runtime_store::monitor::{entry, finish_page, now_ms},
    Registry, RuntimeStore,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex, Weak,
    },
    time::{Duration, Instant},
};
use tachyon_api::monitor::*;

const MAX_SCOPES: usize = 32;
const MAX_SUBSCRIBERS: usize = 64;
const PERIOD: Duration = Duration::from_secs(1);

struct Cached {
    snapshot: Option<MonitorSnapshot>,
    users: usize,
    touched: Instant,
}
struct State {
    entries: BTreeMap<MonitorQuery, Cached>,
    subscribers: usize,
    sequence: u64,
}
pub(crate) struct Monitor {
    shutdown: Arc<AtomicBool>,
    epoch: String,
    state: Mutex<State>,
    changed: Condvar,
}
fn host_query() -> MonitorQuery {
    MonitorQuery {
        scope: MonitorScope::Host,
        after: None,
        limit: MAX_PAGE,
    }
}

pub(crate) struct Lease {
    monitor: Arc<Monitor>,
    query: MonitorQuery,
    subscription: bool,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = self.monitor.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = state.entries.get_mut(&self.query) {
            entry.users -= 1;
            entry.touched = Instant::now();
        }
        if self.subscription {
            state.subscribers -= 1;
        }
    }
}
impl Lease {
    /// None means no value change. Timeouts only allow socket/shutdown checks, not sampling.
    pub(crate) fn latest(
        &self,
        after: Option<&MonitorVersion>,
    ) -> Result<Option<MonitorSnapshot>, MonitorError> {
        let mut state = self
            .monitor
            .state
            .lock()
            .map_err(|_| MonitorError::Unavailable)?;
        let ready = |s: &State| {
            s.entries
                .get(&self.query)
                .and_then(|e| e.snapshot.as_ref())
                .is_some_and(|snapshot| Some(&snapshot.version) != after)
        };
        if !self.monitor.shutdown.load(Ordering::Acquire) && !ready(&state) {
            state = self
                .monitor
                .changed
                .wait_timeout(state, PERIOD)
                .map_err(|_| MonitorError::Unavailable)?
                .0;
        }
        if self.monitor.shutdown.load(Ordering::Acquire) {
            return Err(MonitorError::Stopped);
        }
        Ok(state
            .entries
            .get(&self.query)
            .and_then(|e| e.snapshot.as_ref())
            .filter(|s| Some(&s.version) != after)
            .cloned())
    }
}

impl Monitor {
    pub(crate) fn start(
        registry: Weak<Mutex<Registry>>,
        store: Arc<RuntimeStore>,
        shutdown: Arc<AtomicBool>,
    ) -> (Arc<Self>, std::thread::JoinHandle<()>) {
        let monitor = Arc::new(Self {
            shutdown,
            epoch: uuid::Uuid::new_v4().to_string(),
            state: Mutex::new(State {
                entries: BTreeMap::from([(
                    host_query(),
                    Cached {
                        snapshot: None,
                        users: 0,
                        touched: Instant::now(),
                    },
                )]),
                subscribers: 0,
                sequence: 0,
            }),
            changed: Condvar::new(),
        });
        let sampler = monitor.clone();
        let thread = std::thread::spawn(move || {
            loop {
                let started = Instant::now();
                let queries = {
                    let mut state = sampler.state.lock().unwrap_or_else(|e| e.into_inner());
                    if sampler.shutdown.load(Ordering::Acquire) {
                        break;
                    }
                    state.entries.retain(|q, e| {
                        *q == host_query()
                            || e.users != 0
                            || e.touched.elapsed() < Duration::from_secs(60)
                    });
                    state.entries.keys().cloned().collect::<Vec<_>>()
                };
                let results = match store.monitor_sample(&queries) {
                    Ok(mut output) => {
                        let mut campaigns = None;
                        if let Some(registry) = registry.upgrade() {
                            match registry.try_lock() {
                                Ok(registry) => {
                                    campaigns = registry.campaigns.clone();
                                    for (query, result) in queries.iter().zip(&mut output) {
                                        if matches!(query.scope, MonitorScope::Host) {
                                            if let Ok(payload) = result {
                                                payload.registered = registered(&registry, query);
                                            }
                                        }
                                    }
                                }
                                Err(_) => {
                                    for (query, result) in queries.iter().zip(&mut output) {
                                        if matches!(query.scope, MonitorScope::Host) {
                                            *result = Err(MonitorError::Unavailable);
                                        }
                                    }
                                }
                            }
                        } else {
                            break;
                        }
                        if let Some(campaigns) = campaigns {
                            let capacity = campaigns
                                .monitor_capacity()
                                .map_err(|_| MonitorError::Unavailable);
                            for (query, result) in queries.iter().zip(&mut output) {
                                if matches!(query.scope, MonitorScope::Host) {
                                    match (&capacity, result.as_mut()) {
                                        (Ok(capacity), Ok(payload)) => {
                                            payload.capacities.insert(0, capacity.clone())
                                        }
                                        (Err(error), _) => *result = Err(error.clone()),
                                        _ => {}
                                    }
                                }
                            }
                        }
                        output
                    }
                    // Do not expose storage error strings: they can contain host paths/data.
                    Err(_) => vec![Err(MonitorError::Unavailable); queries.len()],
                };
                sampler.publish(queries.into_iter().zip(results));
                let state = sampler.state.lock().unwrap_or_else(|e| e.into_inner());
                let _ = sampler.changed.wait_timeout_while(
                    state,
                    PERIOD.saturating_sub(started.elapsed()),
                    |_| !sampler.shutdown.load(Ordering::Acquire),
                );
            }
            sampler.stop();
        });
        (monitor, thread)
    }

    fn publish(
        &self,
        results: impl IntoIterator<Item = (MonitorQuery, Result<MonitorPayload, MonitorError>)>,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        for (query, result) in results {
            let Some(cached) = state.entries.get(&query) else {
                continue;
            };
            let old = cached.snapshot.as_ref();
            let (payload, stale) = match result {
                Ok(payload) => (Some(payload), None),
                Err(error) => (old.and_then(|s| s.payload.clone()), Some(error)),
            };
            let same = old.is_some_and(|old| {
                old.stale == stale
                    && match (&old.payload, &payload) {
                        (Some(a), Some(b)) => a.same_values(b),
                        (None, None) => true,
                        _ => false,
                    }
            });
            let version = if same {
                old.unwrap().version.clone()
            } else {
                state.sequence += 1;
                MonitorVersion {
                    epoch: self.epoch.clone(),
                    sequence: state.sequence,
                }
            };
            state.entries.get_mut(&query).unwrap().snapshot = Some(MonitorSnapshot {
                query: query.clone(),
                version,
                payload,
                stale,
            });
        }
        self.changed.notify_all();
    }

    pub(crate) fn acquire(
        self: &Arc<Self>,
        query: MonitorQuery,
        subscription: bool,
    ) -> Result<Lease, MonitorError> {
        query.validate()?;
        let mut state = self.state.lock().map_err(|_| MonitorError::Unavailable)?;
        if self.shutdown.load(Ordering::Acquire) {
            return Err(MonitorError::Stopped);
        }
        if subscription && state.subscribers >= MAX_SUBSCRIBERS {
            return Err(MonitorError::SubscriberLimit);
        }
        if !state.entries.contains_key(&query) && state.entries.len() >= MAX_SCOPES {
            let evict = state
                .entries
                .iter()
                .filter(|(q, e)| **q != host_query() && e.users == 0)
                .min_by_key(|(_, e)| e.touched)
                .map(|(q, _)| q.clone());
            if let Some(evict) = evict {
                state.entries.remove(&evict);
            } else {
                return Err(MonitorError::ScopeLimit);
            }
        }
        state
            .entries
            .entry(query.clone())
            .or_insert_with(|| Cached {
                snapshot: None,
                users: 0,
                touched: Instant::now(),
            })
            .users += 1;
        if subscription {
            state.subscribers += 1;
        }
        Ok(Lease {
            monitor: self.clone(),
            query,
            subscription,
        })
    }

    pub(crate) fn stop(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.changed.notify_all();
    }
}

fn registered(registry: &Registry, query: &MonitorQuery) -> Registered {
    let mut page = Registered {
        sampled_at_ms: now_ms(),
        ..Default::default()
    };
    for (id, task) in &registry.tasks {
        let role = if registry.foreground_id.as_ref() == Some(id) {
            RegisteredRole::Foreground
        } else if id == tachyon_api::BACKGROUND_ID {
            RegisteredRole::Background
        } else {
            RegisteredRole::Agent
        };
        entry(
            &mut page,
            query,
            RegisteredEntry {
                id: id.clone(),
                role,
                state: task.info.state.to_string(),
                pid: task.info.pid,
            },
        );
    }
    for (id, work) in &registry.works {
        entry(
            &mut page,
            query,
            RegisteredEntry {
                id: format!("work:{id}"),
                role: RegisteredRole::OrdinaryWork,
                state: if work.terminal_result.is_some() {
                    "terminal"
                } else if work.review.is_some() {
                    "review_pending"
                } else {
                    "registered"
                }
                .into(),
                pid: None,
            },
        );
    }
    // Background is supervised separately from the ordinary task registry.
    if registry.background_online && !registry.tasks.contains_key(tachyon_api::BACKGROUND_ID) {
        entry(
            &mut page,
            query,
            RegisteredEntry {
                id: tachyon_api::BACKGROUND_ID.into(),
                role: RegisteredRole::Background,
                state: "online".into(),
                pid: None,
            },
        );
    }
    if registry.memory_store.is_some() && !registry.tasks.contains_key(tachyon_api::MEMORY_ID) {
        entry(
            &mut page,
            query,
            RegisteredEntry {
                id: tachyon_api::MEMORY_ID.into(),
                role: RegisteredRole::MemoryService,
                state: "available".into(),
                pid: None,
            },
        );
    }
    finish_page(&mut page, query);
    page
}

pub(crate) fn serve(
    writer: &mut std::os::unix::net::UnixStream,
    monitor: Option<Arc<Monitor>>,
    query: MonitorQuery,
    subscribe: bool,
) -> std::io::Result<()> {
    use tachyon_api::types::ApiResponse;
    let shutdown = monitor
        .as_ref()
        .map(|m| m.shutdown.clone())
        .unwrap_or_default();
    let lease = monitor
        .ok_or(MonitorError::Unavailable)
        .and_then(|m| m.acquire(query, subscribe));
    let lease = match lease {
        Ok(lease) => lease,
        Err(MonitorError::Stopped) => return Ok(()),
        Err(error) => {
            return crate::write_service_response(
                writer,
                &ApiResponse::MonitorError { error },
                &shutdown,
            )
        }
    };
    // Every connection starts with a full snapshot, even after an epoch mismatch.
    let mut after = None;
    loop {
        match lease.latest(after.as_ref()) {
            Ok(Some(snapshot)) => {
                after = Some(snapshot.version.clone());
                crate::write_service_response(
                    writer,
                    &ApiResponse::Monitor { snapshot },
                    &shutdown,
                )?;
                if !subscribe {
                    return Ok(());
                }
            }
            Ok(None) => {}
            Err(MonitorError::Stopped) => return Ok(()),
            Err(error) => {
                return crate::write_service_response(
                    writer,
                    &ApiResponse::MonitorError { error },
                    &shutdown,
                )
            }
        }
        // Detect idle disconnects without heartbeats or per-client DB polling.
        use nix::sys::socket::{recv, MsgFlags};
        use std::os::fd::AsRawFd;
        match recv(
            writer.as_raw_fd(),
            &mut [0u8; 1],
            MsgFlags::MSG_PEEK | MsgFlags::MSG_DONTWAIT,
        ) {
            Ok(0) => return Ok(()),
            Ok(_) if subscribe => return Ok(()), // Subscriptions cannot pipeline commands.
            Ok(_) => {}
            Err(nix::errno::Errno::EAGAIN) | Err(nix::errno::Errno::EINTR) => {}
            Err(error) => return Err(std::io::Error::from_raw_os_error(error as i32)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use tachyon_api::types::{ApiRequest, ApiResponse};

    fn monitor() -> Arc<Monitor> {
        Arc::new(Monitor {
            shutdown: Arc::new(AtomicBool::new(false)),
            epoch: uuid::Uuid::new_v4().to_string(),
            state: Mutex::new(State {
                entries: BTreeMap::new(),
                subscribers: 0,
                sequence: 0,
            }),
            changed: Condvar::new(),
        })
    }
    fn payload(n: u128) -> MonitorPayload {
        MonitorPayload {
            durable: Durable {
                admitted_work: Decimal(n),
                ..Default::default()
            },
            capacities: vec![],
            registered: Registered::default(),
        }
    }

    #[test]
    fn monitor_coalesces_ignores_clocks_preserves_stale_and_bounds_scopes() {
        let m = monitor();
        let lease = m.acquire(host_query(), true).unwrap();
        m.publish([(host_query(), Ok(payload(0)))]);
        let first = lease.latest(None).unwrap().unwrap();
        let mut p = payload(0);
        p.durable.sampled_at_ms = 100;
        p.registered.sampled_at_ms = 200;
        m.publish([(host_query(), Ok(p))]);
        let second = lease.latest(None).unwrap().unwrap();
        assert_eq!(first.version, second.version);
        assert_eq!(second.payload.unwrap().durable.sampled_at_ms, 100);
        for n in 1..1000 {
            m.publish([(host_query(), Ok(payload(n)))]);
        }
        let latest = lease.latest(Some(&first.version)).unwrap().unwrap();
        assert_eq!(
            latest.payload.as_ref().unwrap().durable.admitted_work.0,
            999
        );
        assert_eq!(m.state.lock().unwrap().entries.len(), 1);
        m.publish([(host_query(), Err(MonitorError::Unavailable))]);
        let stale = lease.latest(Some(&latest.version)).unwrap().unwrap();
        assert_eq!(stale.payload, latest.payload);
        assert_eq!(stale.stale, Some(MonitorError::Unavailable));
        m.publish([(host_query(), Err(MonitorError::Unavailable))]);
        assert_eq!(lease.latest(None).unwrap().unwrap().version, stale.version);
        m.publish([(host_query(), Ok(payload(999)))]);
        assert_ne!(lease.latest(None).unwrap().unwrap().version, stale.version);
        let mut pins = Vec::new();
        for n in 1..MAX_SCOPES {
            pins.push(
                m.acquire(
                    MonitorQuery {
                        scope: MonitorScope::Campaign {
                            campaign_id: n.to_string(),
                        },
                        after: None,
                        limit: 100,
                    },
                    false,
                )
                .unwrap(),
            );
        }
        assert!(matches!(
            m.acquire(
                MonitorQuery {
                    scope: MonitorScope::Campaign {
                        campaign_id: "overflow".into()
                    },
                    after: None,
                    limit: 100
                },
                false
            ),
            Err(MonitorError::ScopeLimit)
        ));
        let mut subscriptions = Vec::new();
        for _ in 1..MAX_SUBSCRIBERS {
            subscriptions.push(m.acquire(host_query(), true).unwrap());
        }
        assert!(matches!(
            m.acquire(host_query(), true),
            Err(MonitorError::SubscriberLimit)
        ));
        drop(pins);
        assert!(m
            .acquire(
                MonitorQuery {
                    scope: MonitorScope::Campaign {
                        campaign_id: "overflow".into()
                    },
                    after: None,
                    limit: 100
                },
                false
            )
            .is_ok());
        m.stop();
        assert_eq!(lease.latest(None), Err(MonitorError::Stopped));
    }

    #[test]
    fn monitor_registered_ordinary_agents_bounded_pages_and_secret_redaction() {
        let mut registry = Registry::default();
        for n in 0..256 {
            let id = format!("agent-{n:03}");
            let mut info = crate::memory_agent_info(0, 0);
            info.id = id.clone();
            info.task = "SECRET_PROMPT".into();
            info.workspace = "/home/SECRET_HOME".into();
            info.description = "https://SECRET_CONFIG".into();
            info.pid = Some(n);
            registry.tasks.insert(
                id,
                crate::Task {
                    info,
                    depends_on: vec![],
                    process: None,
                    stdin: None,
                    subs: vec![],
                    generation: 0,
                    assignment: 0,
                    warm: false,
                    ready: false,
                    owner: None,
                    last_used_secs: 0,
                    control_socket: Some("SECRET_SOCKET".into()),
                    terminal_usage: None,
                    terminal_result: Some("SECRET_RESULT".into()),
                },
            );
        }
        let dir = tempfile::tempdir().unwrap();
        registry.memory_store = Some(Arc::new(
            crate::MemoryStore::open(dir.path().join("memory.redb")).unwrap(),
        ));
        let mut query = host_query();
        let first = registered(&registry, &query);
        assert_eq!(first.total.0, 257);
        assert_eq!(first.entries.len(), 100);
        assert!(first
            .entries
            .iter()
            .all(|e| e.role == RegisteredRole::Agent));
        query.after = first.next_after;
        let second = registered(&registry, &query);
        assert_eq!(second.entries[0].id, "agent-100");
        query.after = second.next_after;
        let last = registered(&registry, &query);
        assert_eq!(last.entries.len(), 57);
        assert_eq!(last.next_after, None);
        let memory = last.entries.last().unwrap();
        assert_eq!(memory.role, RegisteredRole::MemoryService);
        assert_eq!(memory.pid, None);
        assert!(!serde_json::to_string(&last).unwrap().contains("SECRET"));
    }

    #[test]
    fn monitor_ipc_epoch_resnapshot_and_stop_without_registry_socket_lock() {
        let m = monitor();
        let pin = m.acquire(host_query(), false).unwrap();
        m.publish([(host_query(), Ok(payload(7)))]);
        let old_epoch = monitor().epoch.clone();
        let registry = Arc::new(Mutex::new(Registry {
            monitor: Some(m.clone()),
            ..Registry::default()
        }));
        let (mut client, server) = std::os::unix::net::UnixStream::pair().unwrap();
        nix::sys::socket::setsockopt(&server, nix::sys::socket::sockopt::SndBuf, &1024usize)
            .unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let r = registry.clone();
        let (done, ended) = std::sync::mpsc::channel();
        let handler = std::thread::spawn(move || {
            done.send(crate::handle_connection(server, r)).unwrap();
        });
        let request = ApiRequest::MonitorSubscribe {
            query: host_query(),
            after: Some(MonitorVersion {
                epoch: old_epoch.clone(),
                sequence: u64::MAX,
            }),
        };
        writeln!(client, "{}", serde_json::to_string(&request).unwrap()).unwrap();
        let mut reader = BufReader::new(client.try_clone().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let ApiResponse::Monitor { snapshot } = serde_json::from_str(&line).unwrap() else {
            panic!()
        };
        assert_ne!(snapshot.version.epoch, old_epoch);
        assert_eq!(snapshot.payload.unwrap().durable.admitted_work.0, 7);
        // A non-reading socket cannot hold the registry or stop the sampler.
        for n in 8..40 {
            let mut p = payload(n);
            p.registered.entries = (0..100)
                .map(|i| RegisteredEntry {
                    id: format!("{i:03}{}", "x".repeat(250)),
                    role: RegisteredRole::Agent,
                    state: "registered".into(),
                    pid: None,
                })
                .collect();
            m.publish([(host_query(), Ok(p))]);
        }
        // Observe the start of a large frame, then leave the rest unread and connected.
        reader.read_exact(&mut [0u8; 1]).unwrap();
        assert!(registry.try_lock().is_ok());
        let started = Instant::now();
        m.stop();
        assert!(started.elapsed() < Duration::from_secs(1));
        let result = ended.recv_timeout(Duration::from_secs(1));
        if result.is_err() {
            client.shutdown(std::net::Shutdown::Both).unwrap();
        }
        handler.join().unwrap();
        assert!(result.is_ok());
        drop(pin);
        assert_eq!(m.state.lock().unwrap().subscribers, 0);
    }

    #[test]
    fn monitor_shared_shutdown_releases_connected_idle_subscriber() {
        let m = monitor();
        let pin = m.acquire(host_query(), false).unwrap();
        m.publish([(host_query(), Ok(payload(7)))]);
        let (client, mut server) = std::os::unix::net::UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let serving = m.clone();
        let (done, ended) = std::sync::mpsc::channel();
        let handler = std::thread::spawn(move || {
            done.send(serve(&mut server, Some(serving), host_query(), true))
                .unwrap();
        });
        let mut reader = BufReader::new(client);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        // Signal-handler path: no condvar notification and no further client traffic.
        m.shutdown.store(true, Ordering::Release);
        let result = ended.recv_timeout(Duration::from_secs(2));
        if result.is_err() {
            reader.get_ref().shutdown(std::net::Shutdown::Both).unwrap();
        }
        handler.join().unwrap();
        result.unwrap().unwrap();
        drop(pin);
        let state = m.state.lock().unwrap();
        assert_eq!(state.subscribers, 0);
        assert_eq!(state.entries[&host_query()].users, 0);
    }

    #[test]
    fn monitor_disconnected_unsampled_clients_release_leases() {
        for subscribe in [false, true] {
            let m = monitor();
            let (client, mut server) = std::os::unix::net::UnixStream::pair().unwrap();
            drop(client);
            let serving = m.clone();
            let (done, ended) = std::sync::mpsc::channel();
            let handler = std::thread::spawn(move || {
                let result = serve(&mut server, Some(serving), host_query(), subscribe);
                done.send(result).unwrap();
            });
            let result = ended.recv_timeout(Duration::from_secs(3));
            m.stop();
            handler.join().unwrap();
            result.unwrap().unwrap();
            let state = m.state.lock().unwrap();
            assert_eq!(state.subscribers, 0);
            assert_eq!(state.entries[&host_query()].users, 0);
        }
    }

    #[test]
    fn monitor_registry_contention_does_not_block_sampling_or_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let registry = Arc::new(Mutex::new(Registry::default()));
        let guard = registry.lock().unwrap();
        let (m, thread) = Monitor::start(
            Arc::downgrade(&registry),
            store,
            Arc::new(AtomicBool::new(false)),
        );
        let lease = m.acquire(host_query(), true).unwrap();
        let snapshot = lease.latest(None).unwrap();
        m.shutdown.store(true, Ordering::Release);
        let (done, ended) = std::sync::mpsc::channel();
        let joiner = std::thread::spawn(move || {
            thread.join().unwrap();
            done.send(()).unwrap();
        });
        let stopped = ended.recv_timeout(Duration::from_secs(2));
        drop(guard);
        joiner.join().unwrap();
        stopped.unwrap();
        assert_eq!(snapshot.unwrap().stale, Some(MonitorError::Unavailable));
    }

    #[test]
    fn monitor_shared_sampler_ticks_no_faster_than_one_second() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let registry = Arc::new(Mutex::new(Registry::default()));
        let (m, thread) = Monitor::start(
            Arc::downgrade(&registry),
            store,
            Arc::new(AtomicBool::new(false)),
        );
        let lease = m.acquire(host_query(), true).unwrap();
        let first = loop {
            if let Some(s) = lease.latest(None).unwrap() {
                break s;
            }
        };
        let clock = first.payload.as_ref().unwrap().durable.sampled_at_ms;
        let started = Instant::now();
        while started.elapsed() < Duration::from_millis(200) {
            assert_eq!(
                lease
                    .latest(None)
                    .unwrap()
                    .unwrap()
                    .payload
                    .unwrap()
                    .durable
                    .sampled_at_ms,
                clock
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        // A concurrent registry change is separately clocked and sampled on the shared tick.
        registry.lock().unwrap().background_online = true;
        let next = loop {
            if let Some(s) = lease.latest(Some(&first.version)).unwrap() {
                break s;
            }
        };
        let p = next.payload.unwrap();
        assert!(p.durable.sampled_at_ms >= clock + 1000);
        assert!(p.registered.sampled_at_ms >= p.durable.sampled_at_ms);
        assert_eq!(p.registered.entries[0].role, RegisteredRole::Background);
        m.stop();
        thread.join().unwrap();
    }
}
