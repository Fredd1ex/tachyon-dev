//! Bounded checkpoint service. No lock is held across serialization or I/O.
mod loading;
use super::super::{attention, session_archive, Thread};
pub(in crate::app) use loading::Navigator;
use std::{
    fmt, io,
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const RETRY: Duration = Duration::from_secs(1);
const DRAIN_ATTEMPTS: usize = 3;

struct Snapshot {
    body: session_archive::Snapshot,
    receipt: Option<attention::State>,
}

impl Snapshot {
    fn save(&mut self) -> io::Result<()> {
        self.body.save()?;
        if let Some(receipt) = &mut self.receipt {
            receipt.save()?;
        }
        Ok(())
    }
}

struct Work<T> {
    revision: u64,
    receipt_generation: Option<u64>,
    snapshot: T,
}

struct Ack {
    revision: u64,
    receipt_generation: Option<u64>,
    error: Option<String>,
}

struct Mailbox<T> {
    pending: Option<Work<T>>,
    ack: Option<Ack>,
    newest: u64,
    stopping: bool,
}

// Retain the uncommitted latest snapshot in the returned error, rather than
// claiming a successful flush or silently discarding it on permanent failure.
struct Uncommitted<T> {
    work: Work<T>,
    error: io::Error,
}

impl<T> fmt::Debug for Uncommitted<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "checkpoint {} not committed: {}",
            self.work.revision, self.error
        )
    }
}
impl<T> fmt::Display for Uncommitted<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl<T> std::error::Error for Uncommitted<T> {}

struct Worker<T> {
    shared: Arc<(Mutex<Mailbox<T>>, Condvar)>,
    join: Option<JoinHandle<io::Result<()>>>,
}

impl<T: Send + Sync + 'static> Worker<T> {
    fn start(mut save: impl FnMut(&mut T) -> io::Result<()> + Send + 'static) -> io::Result<Self> {
        let shared = Arc::new((
            Mutex::new(Mailbox::<T> {
                pending: None,
                ack: None,
                newest: 0,
                stopping: false,
            }),
            Condvar::new(),
        ));
        let state = shared.clone();
        let join = thread::Builder::new()
            .name("tui-history".into())
            .spawn(move || {
                let (lock, wake) = &*state;
                let mut active: Option<Work<T>> = None;
                let mut retry_at = Instant::now();
                let mut drain_failures = 0;
                loop {
                    let mut mailbox = lock.lock().unwrap();
                    loop {
                        if let Some(newer) = mailbox.pending.take() {
                            let previous = active.replace(newer);
                            drop(mailbox);
                            drop(previous);
                            mailbox = lock.lock().unwrap();
                            // New snapshots do not bypass failure backoff.
                        }
                        if mailbox.stopping || (active.is_some() && Instant::now() >= retry_at) {
                            break;
                        }
                        mailbox = if active.is_some() {
                            wake.wait_timeout(
                                mailbox,
                                retry_at.saturating_duration_since(Instant::now()),
                            )
                            .unwrap()
                            .0
                        } else {
                            wake.wait(mailbox).unwrap()
                        };
                    }
                    let stopping = mailbox.stopping;
                    drop(mailbox);
                    let Some(mut work) = active.take() else {
                        return Ok(());
                    };
                    let result = save(&mut work.snapshot);
                    let mut mailbox = lock.lock().unwrap();
                    mailbox.ack = Some(Ack {
                        revision: work.revision,
                        receipt_generation: work.receipt_generation,
                        error: result.as_ref().err().map(ToString::to_string),
                    });
                    match result {
                        Ok(()) => {
                            drain_failures = 0;
                            retry_at = Instant::now();
                        }
                        Err(error) => {
                            if stopping {
                                drain_failures += 1;
                                if drain_failures >= DRAIN_ATTEMPTS {
                                    // A newer pending snapshot must still get its drain attempts.
                                    if mailbox.pending.is_none() {
                                        return Err(io::Error::other(Uncommitted { work, error }));
                                    }
                                    drain_failures = 0;
                                }
                            }
                            active = Some(work);
                            retry_at = Instant::now() + RETRY;
                        }
                    }
                }
            })?;
        Ok(Self {
            shared,
            join: Some(join),
        })
    }

    fn submit(&self, work: Work<T>) {
        let (lock, wake) = &*self.shared;
        let mut mailbox = lock.lock().unwrap();
        if !mailbox.stopping && work.revision > mailbox.newest {
            mailbox.newest = work.revision;
            let previous = mailbox.pending.replace(work);
            drop(mailbox);
            wake.notify_one();
            drop(previous);
        }
    }

    fn take(&self) -> Option<Ack> {
        self.shared.0.lock().unwrap().ack.take()
    }

    fn shutdown(&mut self) -> io::Result<()> {
        self.stop();
        self.join.take().map_or(Ok(()), |join| {
            join.join()
                .unwrap_or_else(|_| Err(io::Error::other("history worker panicked")))
        })
    }
}

impl<T> Worker<T> {
    fn stop(&self) {
        self.shared.0.lock().unwrap().stopping = true;
        self.shared.1.notify_one();
    }
}

impl<T> Drop for Worker<T> {
    fn drop(&mut self) {
        self.stop();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

pub(crate) struct History {
    worker: Worker<Snapshot>,
    revision: u64,
    acknowledged: u64,
    receipt_generation: Option<u64>,
}

impl History {
    pub(crate) fn start() -> io::Result<Self> {
        Ok(Self {
            worker: Worker::start(Snapshot::save)?,
            revision: 0,
            acknowledged: 0,
            receipt_generation: None,
        })
    }

    pub(crate) fn checkpoint(
        &mut self,
        visits: &mut session_archive::Visits,
        threads: &[Thread],
        attention: &attention::State,
    ) {
        let generation = attention.pending_generation();
        let Some(body) = visits.capture(
            threads,
            generation.is_some() && generation != self.receipt_generation,
        ) else {
            return;
        };
        self.receipt_generation = generation;
        self.revision += 1;
        self.worker.submit(Work {
            revision: self.revision,
            receipt_generation: generation,
            snapshot: Snapshot {
                body,
                receipt: attention.snapshot(),
            },
        });
    }

    pub(crate) fn poll(&mut self, attention: &mut attention::State) -> Option<String> {
        let ack = self.worker.take()?;
        if ack.revision < self.acknowledged {
            return None;
        }
        if let Some(error) = ack.error {
            return Some(format!("History checkpoint failed (retrying): {error}"));
        }
        self.acknowledged = ack.revision;
        if let Some(generation) = ack.receipt_generation {
            attention.persisted(generation);
        }
        None
    }

    // Call after leaving the interactive loop, never from rendering/input code.
    // Attempts are bounded; an OS filesystem call itself cannot safely be cancelled.
    pub(crate) fn shutdown(&mut self) -> io::Result<()> {
        self.worker.shutdown()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{ClickTarget, ItemKind};
    use std::{fs, path::PathBuf, sync::mpsc};
    use tachyon_api::{
        attention::AttentionFrameMetadata,
        todo::TodoScope,
        types::{HistoryEntry, HistoryKind, HistoryRole},
    };

    fn work(revision: u64) -> Work<u64> {
        Work {
            revision,
            receipt_generation: Some(revision),
            snapshot: revision,
        }
    }

    #[test]
    fn blocked_sink_coalesces_ten_thousand_snapshots_and_rejects_reordered_revisions() {
        let (started, rx) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        let ui_thread = thread::current().id();
        let mut worker = Worker::start(move |value: &mut u64| {
            assert_ne!(thread::current().id(), ui_thread);
            started.send(*value).unwrap();
            if *value == 1 {
                wait.recv_timeout(Duration::from_secs(10)).unwrap();
            }
            Ok(())
        })
        .unwrap();
        worker.submit(work(1));
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), 1);
        for revision in 2..=10_000 {
            worker.submit(work(revision));
        }
        worker.submit(work(3));
        {
            let mailbox = worker.shared.0.lock().unwrap();
            assert_eq!(mailbox.pending.as_ref().unwrap().revision, 10_000);
            assert!(mailbox.ack.is_none());
        }
        release.send(()).unwrap();
        worker.shutdown().unwrap();
        assert_eq!(rx.try_iter().collect::<Vec<_>>(), [10_000]);
        let ack = worker.take().unwrap();
        assert_eq!(
            (ack.revision, ack.receipt_generation),
            (10_000, Some(10_000))
        );
        assert!(ack.error.is_none());
        worker.submit(work(10_001));
        assert!(worker.shared.0.lock().unwrap().pending.is_none());
    }

    #[test]
    fn failed_snapshot_retries_without_another_ui_checkpoint() {
        let (out, rx) = mpsc::channel();
        let mut attempts = 0;
        let mut worker = Worker::start(move |value: &mut u64| {
            attempts += 1;
            out.send((*value, attempts)).unwrap();
            if attempts == 1 {
                Err(io::Error::other("disk unavailable"))
            } else {
                Ok(())
            }
        })
        .unwrap();
        worker.submit(work(1));
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), (1, 1));
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), (1, 2));
        worker.shutdown().unwrap();
        assert!(worker.take().unwrap().error.is_none());
    }

    #[test]
    fn failed_in_flight_revision_is_superseded_by_latest_during_drain() {
        let (started, rx) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        let mut worker = Worker::start(move |value: &mut u64| {
            started.send(*value).unwrap();
            if *value == 1 {
                wait.recv_timeout(Duration::from_secs(10)).unwrap();
                Err(io::Error::other("old write failed"))
            } else {
                Ok(())
            }
        })
        .unwrap();
        worker.submit(work(1));
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), 1);
        for revision in 2..=500 {
            worker.submit(work(revision));
        }
        worker.stop();
        release.send(()).unwrap();
        worker.shutdown().unwrap();
        assert_eq!(rx.try_iter().collect::<Vec<_>>(), [500]);
        let ack = worker.take().unwrap();
        assert_eq!(ack.revision, 500);
        assert!(ack.error.is_none());
    }

    #[test]
    fn stop_has_bounded_failure_attempts_and_returns_latest_uncommitted_snapshot() {
        let (out, rx) = mpsc::channel();
        let mut worker = Worker::start(move |value: &mut u64| {
            out.send(*value).unwrap();
            Err(io::Error::other("disk full"))
        })
        .unwrap();
        // Hold the mailbox so the worker cannot start until stop is requested.
        {
            let mut mailbox = worker.shared.0.lock().unwrap();
            mailbox.pending = Some(work(9));
            mailbox.stopping = true;
        }
        let error = worker.shutdown().unwrap_err();
        let failed = error
            .get_ref()
            .unwrap()
            .downcast_ref::<Uncommitted<u64>>()
            .unwrap();
        assert_eq!(failed.work.snapshot, 9);
        assert_eq!(rx.try_iter().collect::<Vec<_>>(), vec![9; DRAIN_ATTEMPTS]);
        assert!(worker.join.is_none());
    }

    #[test]
    fn drop_drains_and_joins_instead_of_detaching() {
        let (out, rx) = mpsc::channel();
        let worker = Worker::start(move |value: &mut u64| {
            out.send(*value).unwrap();
            Ok(())
        })
        .unwrap();
        worker.submit(work(4));
        drop(worker);
        assert_eq!(rx.try_iter().collect::<Vec<_>>(), [4]);
    }

    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let path = PathBuf::from("/tmp/opencode").join(format!(
                "history-worker-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn notice(id: &str) -> HistoryEntry {
        HistoryEntry {
            attention: Some(AttentionFrameMetadata {
                scope: TodoScope::Conversation {
                    id: "foreground".into(),
                },
                ids: vec![id.into()],
            }),
            event_id: id.into(),
            kind: HistoryKind::Conversation,
            conversation_id: "foreground".into(),
            turn_id: None,
            occurred_at_ms: 123,
            role: HistoryRole::Notification,
            text: format!("durable body {id}"),
            task_id: None,
            task_state: None,
        }
    }

    #[test]
    fn stale_ack_cannot_release_newer_receipts_and_reordered_ack_is_ignored() {
        let directory = Directory::new();
        let mut attention = attention::State::open(&directory.0).unwrap();
        let mut threads = vec![Thread::new_foreground()];
        attention.receive(notice("first"), &mut threads);
        let first = attention.generation();
        attention.receive(notice("second"), &mut threads);
        let second = attention.generation();
        let mut history = History::start().unwrap();
        history.worker.shared.0.lock().unwrap().ack = Some(Ack {
            revision: 1,
            receipt_generation: Some(second),
            error: Some("receipt fsync failed".into()),
        });
        assert!(history.poll(&mut attention).unwrap().contains("retrying"));
        assert!(attention.snapshot().is_some());
        let acknowledge = |revision, generation, history: &History| {
            history.worker.shared.0.lock().unwrap().ack = Some(Ack {
                revision,
                receipt_generation: Some(generation),
                error: None,
            });
        };
        acknowledge(1, first, &history);
        history.poll(&mut attention);
        assert!(attention.snapshot().is_some());
        acknowledge(3, first, &history);
        history.poll(&mut attention);
        acknowledge(2, second, &history);
        history.poll(&mut attention);
        assert!(attention.snapshot().is_some());
        acknowledge(4, second, &history);
        history.poll(&mut attention);
        assert!(attention.snapshot().is_none());
        history.shutdown().unwrap();
    }

    #[test]
    fn real_body_and_receipt_failures_preserve_order_and_recover_after_retry() {
        let directory = Directory::new();
        let mut visits = session_archive::Visits::open(&directory.0).unwrap();
        let mut attention = attention::State::open(&directory.0).unwrap();
        let mut threads = vec![Thread::new_foreground()];
        attention.receive(notice("first"), &mut threads);
        attention.visible(&threads, &[Some(ClickTarget::Attention(0, 0))], false);
        let generation = attention.generation();
        let mut snapshot = Snapshot {
            body: visits.capture(&threads, true).unwrap(),
            receipt: attention.snapshot(),
        };
        let body_dir = directory.0.join("tui-visits");
        let moved = directory.0.join("moved");
        let receipt_path = directory.0.join("tui-attention.json");
        fs::rename(&body_dir, &moved).unwrap();
        assert!(snapshot.save().is_err());
        assert!(!receipt_path.exists());
        assert!(attention.snapshot().is_some());
        fs::rename(&moved, &body_dir).unwrap();
        fs::create_dir(&receipt_path).unwrap();
        assert!(snapshot.save().is_err());
        assert!(attention.snapshot().is_some());
        // Even a receipt failure leaves a readable, durable matching body.
        let mut reopened = session_archive::Visits::open(&directory.0).unwrap();
        let mut restored = vec![Thread::new_foreground()];
        reopened.latest(&mut restored).unwrap();
        assert!(restored[0]
            .items
            .iter()
            .any(|item| item.text == "durable body first"));
        fs::remove_dir(&receipt_path).unwrap();
        snapshot.save().unwrap();
        attention.persisted(generation);
        let receipt: serde_json::Value =
            serde_json::from_slice(&fs::read(receipt_path).unwrap()).unwrap();
        assert_eq!(receipt["visible"], serde_json::json!(["first"]));
        let mut recovered = attention::State::open(&directory.0).unwrap();
        assert!(!recovered.receive(notice("first"), &mut restored));
        assert!(attention.snapshot().is_none());
    }

    #[test]
    fn service_shutdown_saves_latest_body_receipts_and_pending_recovery() {
        let directory = Directory::new();
        let mut visits = session_archive::Visits::open(&directory.0).unwrap();
        let mut attention = attention::State::open(&directory.0).unwrap();
        let mut threads = vec![Thread::new_foreground()];
        let mut history = History::start().unwrap();
        for event in crate::app::tests::two_turn_fixture().iter().take(5) {
            visits.observe(event);
        }
        for index in 0..100 {
            threads[0].add(ItemKind::User, format!("question {index}"));
            attention.receive(notice(&index.to_string()), &mut threads);
            history.checkpoint(&mut visits, &threads, &attention);
        }
        let revision = history.revision;
        history.checkpoint(&mut visits, &threads, &attention);
        assert_eq!(history.revision, revision);
        // Visibility changes only receipts, not the visit's thread revisions.
        attention.visible(&threads, &[Some(ClickTarget::Attention(0, 199))], false);
        history.checkpoint(&mut visits, &threads, &attention);
        assert_eq!(history.revision, revision + 1);
        history.shutdown().unwrap();
        history.poll(&mut attention);
        assert!(attention.snapshot().is_none());
        let mut reopened = session_archive::Visits::open(&directory.0).unwrap();
        assert_eq!(reopened.recovery(), visits.recovery());
        assert!(!reopened.recovery().is_empty());
        let mut restored = vec![Thread::new_foreground()];
        reopened.latest(&mut restored).unwrap();
        assert!(restored[0]
            .items
            .iter()
            .any(|item| item.text == "durable body 99"));
        let mut recovered = attention::State::open(&directory.0).unwrap();
        assert!(!recovered.receive(notice("99"), &mut restored));
        let receipt: serde_json::Value =
            serde_json::from_slice(&fs::read(directory.0.join("tui-attention.json")).unwrap())
                .unwrap();
        assert_eq!(receipt["visible"], serde_json::json!(["99"]));
    }
}
