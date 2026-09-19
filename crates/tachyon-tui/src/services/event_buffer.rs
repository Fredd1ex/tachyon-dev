//! Source-independent FIFO admission. Full buffers park producers, never the UI.
use std::sync::{mpsc, Arc, Condvar, Mutex};

struct State {
    count: usize,
    bytes: usize,
    open: bool,
}

struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    count_limit: usize,
    byte_limit: usize,
}

pub(super) struct Sender<T> {
    tx: mpsc::SyncSender<(T, usize)>,
    shared: Arc<Shared>,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
}

pub(super) struct Receiver<T> {
    rx: mpsc::Receiver<(T, usize)>,
    shared: Arc<Shared>,
}

pub(super) fn channel<T>(count_limit: usize, byte_limit: usize) -> (Sender<T>, Receiver<T>) {
    assert!(count_limit > 0 && byte_limit > 0);
    let (tx, rx) = mpsc::sync_channel(count_limit);
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            count: 0,
            bytes: 0,
            open: true,
        }),
        changed: Condvar::new(),
        count_limit,
        byte_limit,
    });
    (
        Sender {
            tx,
            shared: shared.clone(),
            cancelled: Arc::new(false.into()),
        },
        Receiver { rx, shared },
    )
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            shared: self.shared.clone(),
            cancelled: self.cancelled.clone(),
        }
    }
}

impl<T> Sender<T> {
    /// Independent cancellation for a new source using the same global budget.
    pub(super) fn fork(&self) -> Self {
        Self {
            cancelled: Arc::new(false.into()),
            ..self.clone()
        }
    }

    pub(super) fn cancel(&self) {
        let _state = self.shared.state.lock().unwrap();
        self.cancelled
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.shared.changed.notify_all();
    }

    pub(super) fn send(&self, value: T, bytes: usize) -> Result<(), ()> {
        let mut state = self.shared.state.lock().unwrap();
        loop {
            if !state.open || self.cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(());
            }
            // No protocol maximum exists: an oversized value is admitted alone,
            // rather than silently losing a valid final or tool result.
            if state.count < self.shared.count_limit
                && (state.count == 0
                    || (state.bytes <= self.shared.byte_limit
                        && bytes <= self.shared.byte_limit - state.bytes))
            {
                break;
            }
            state = self.shared.changed.wait(state).unwrap();
        }
        self.tx.try_send((value, bytes)).map_err(|_| ())?;
        state.count += 1;
        state.bytes += bytes;
        Ok(())
    }
}

impl<T> Receiver<T> {
    #[cfg(test)]
    pub(super) fn usage(&self) -> (usize, usize) {
        let state = self.shared.state.lock().unwrap();
        (state.count, state.bytes)
    }

    pub(super) fn try_recv(&self) -> Option<T> {
        // Never call mpsc recv (even try_recv) while holding our accounting lock.
        let (value, bytes) = self.rx.try_recv().ok()?;
        let mut state = self.shared.state.lock().unwrap();
        state.count -= 1;
        state.bytes -= bytes;
        self.shared.changed.notify_all();
        Some(value)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.state.lock().unwrap().open = false;
        self.shared.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn count_and_bytes_each_backpressure_without_losing_fifo_final() {
        for (count, bytes, weight, filled) in [(2, 100, 1, 2), (10, 10, 4, 2)] {
            let (tx, rx) = channel(count, bytes);
            for value in 0..filled {
                tx.send(value, weight).unwrap();
            }
            assert_eq!(rx.usage(), (filled, filled * weight));
            let (done, completed) = mpsc::channel();
            let producer = std::thread::spawn(move || {
                tx.send(filled, weight).unwrap();
                done.send(()).unwrap();
            });
            assert!(completed.recv_timeout(Duration::from_millis(30)).is_err());
            assert_eq!(rx.try_recv(), Some(0));
            completed.recv_timeout(Duration::from_secs(2)).unwrap();
            for expected in 1..=filled {
                assert_eq!(rx.try_recv(), Some(expected));
            }
            assert_eq!(rx.usage(), (0, 0));
            producer.join().unwrap();
        }
    }

    #[test]
    fn oversized_payload_is_preserved_but_admitted_alone() {
        let (tx, rx) = channel(8, 10);
        tx.send("tool-output", 11).unwrap();
        let (done, completed) = mpsc::channel();
        let producer = std::thread::spawn(move || {
            tx.send("final", 0).unwrap();
            done.send(()).unwrap();
        });
        assert!(completed.recv_timeout(Duration::from_millis(30)).is_err());
        assert_eq!(rx.usage(), (1, 11));
        assert_eq!(rx.try_recv(), Some("tool-output"));
        completed.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(rx.try_recv(), Some("final"));
        producer.join().unwrap();
    }

    #[test]
    fn cancellation_or_receiver_drop_wakes_full_buffer_sender() {
        for cancel in [true, false] {
            let (tx, rx) = channel(1, 1);
            tx.send(0, 1).unwrap();
            let producer_tx = tx.clone();
            let (done, completed) = mpsc::channel();
            let producer = std::thread::spawn(move || {
                done.send(producer_tx.send(1, 1)).unwrap();
            });
            assert!(completed.recv_timeout(Duration::from_millis(30)).is_err());
            if cancel {
                tx.cancel();
            } else {
                drop(rx);
            }
            assert_eq!(
                completed.recv_timeout(Duration::from_secs(2)).unwrap(),
                Err(())
            );
            producer.join().unwrap();
        }
    }

    #[test]
    fn cancelling_one_source_does_not_cancel_its_replacement() {
        let (tx, rx) = channel(1, 1);
        let old = tx.fork();
        let new = tx.fork();
        old.cancel();
        assert_eq!(old.send("old", 1), Err(()));
        new.send("new", 1).unwrap();
        assert_eq!(rx.try_recv(), Some("new"));
    }
}
