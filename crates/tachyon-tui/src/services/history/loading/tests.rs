use super::*;
use std::{sync::mpsc, time::Duration};

#[test]
fn stale_daemon_page_cannot_replace_selection_after_cancel() {
    let (started, wait) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let mut navigator = Navigator::with_loader(move |_| {
        started.send(()).unwrap();
        gate.recv().unwrap();
        Ok(Some(LoadedPage {
            before: 1,
            entries: Vec::new(),
        }))
    })
    .unwrap();
    navigator.request(PageRequest { until: 20 }, Selection::Show);
    wait.recv_timeout(Duration::from_secs(2)).unwrap();
    navigator.cancel();
    release.send(()).unwrap();
    navigator.shutdown().unwrap();
    assert!(navigator.take().is_none());
}
