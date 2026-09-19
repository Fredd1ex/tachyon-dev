//! Read-side navigation is independent of the durable checkpoint worker.
use crate::app::{
    input, reset_transcript, select_trace_turn,
    session_archive::{LoadedPage, PageRequest, Visits},
    toggle_history, Thread, TranscriptScroll, TranscriptView, TurnLayoutCache, TurnProjection,
};
use crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers, MouseEventKind};
use std::{
    io,
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
};

#[derive(Clone, Copy)]
enum Selection {
    Show,
    Step { older: bool, entering: bool },
}

struct Request {
    generation: u64,
    page: PageRequest,
    selection: Selection,
}

pub(in crate::app) struct Loaded {
    generation: u64,
    page: io::Result<Option<LoadedPage>>,
    selection: Selection,
}

impl Loaded {
    // Called only on the UI thread after the generation check. Errors cannot
    // erase the displayed page, advance its cursor, or change the copy target.
    pub(in crate::app) fn apply(
        self,
        visits: &mut Visits,
        threads: &mut Vec<Thread>,
        selected: &mut Option<usize>,
        view: &mut TranscriptView,
        scroll: &mut TranscriptScroll,
        cache: &mut TurnLayoutCache,
        projection: &mut TurnProjection,
    ) -> io::Result<()> {
        let page = self.page?;
        let changed = page.is_some();
        if let Some(page) = page {
            visits.install(page, threads);
        }
        match self.selection {
            Selection::Show => {
                if changed {
                    reset_transcript(scroll, view, cache, selected, projection);
                }
            }
            Selection::Step { older, entering } if changed || entering => {
                reset_transcript(scroll, view, cache, selected, projection);
                projection.update(&threads[0]);
                let history = projection
                    .cells
                    .iter()
                    .take_while(|cell| cell.prompt < threads[0].history_len)
                    .count();
                *selected = if older {
                    history.checked_sub(1)
                } else {
                    (history > 0).then_some(0)
                };
                scroll.follow = selected.is_none();
            }
            Selection::Step { older, .. } => {
                select_trace_turn(selected, view, scroll, if older { -1 } else { 1 });
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct Mailbox {
    generation: u64,
    pending: Option<Request>,
    result: Option<Loaded>,
    stopping: bool,
}

pub(in crate::app) struct Navigator {
    shared: Arc<(Mutex<Mailbox>, Condvar)>,
    generation: u64,
    join: Option<JoinHandle<()>>,
    loading: bool,
}

impl Navigator {
    pub(in crate::app) fn start() -> io::Result<Self> {
        Self::with_loader(PageRequest::load)
    }

    fn with_loader(
        mut load: impl FnMut(PageRequest) -> io::Result<Option<LoadedPage>> + Send + 'static,
    ) -> io::Result<Self> {
        let shared = Arc::new((Mutex::new(Mailbox::default()), Condvar::new()));
        let state = shared.clone();
        let join = thread::Builder::new()
            .name("tui-history-load".into())
            .spawn(move || {
                let (lock, wake) = &*state;
                loop {
                    let mut mailbox = lock.lock().unwrap();
                    while !mailbox.stopping && mailbox.pending.is_none() {
                        mailbox = wake.wait(mailbox).unwrap();
                    }
                    if mailbox.stopping {
                        return;
                    }
                    let request = mailbox.pending.take().unwrap();
                    drop(mailbox);
                    let result = Loaded {
                        generation: request.generation,
                        selection: request.selection,
                        page: load(request.page),
                    };
                    let mut mailbox = lock.lock().unwrap();
                    if !mailbox.stopping && result.generation == mailbox.generation {
                        let old = mailbox.result.replace(result);
                        drop(mailbox);
                        drop(old);
                    }
                    // Stale results and their buffers are dropped outside the lock.
                }
            })?;
        Ok(Self {
            shared,
            generation: 0,
            join: Some(join),
            loading: false,
        })
    }

    pub(in crate::app) fn cancel(&mut self) {
        self.generation += 1;
        self.loading = false;
        let mut mailbox = self.shared.0.lock().unwrap();
        mailbox.generation = self.generation;
        let old = (mailbox.pending.take(), mailbox.result.take());
        drop(mailbox);
        drop(old);
    }

    fn request(&mut self, page: PageRequest, selection: Selection) {
        self.cancel();
        let mut mailbox = self.shared.0.lock().unwrap();
        if !mailbox.stopping {
            self.loading = true;
            mailbox.pending = Some(Request {
                generation: self.generation,
                page,
                selection,
            });
            self.shared.1.notify_one();
        }
    }

    pub(in crate::app) fn take(&mut self) -> Option<Loaded> {
        let result = self.shared.0.lock().unwrap().result.take()?;
        if result.generation != self.generation {
            return None;
        }
        self.loading = false;
        Some(result)
    }

    pub(in crate::app) fn is_loading(&self) -> bool {
        self.loading
    }

    pub(in crate::app) fn toggle(&mut self, visits: &Visits, threads: &mut [Thread]) {
        self.cancel();
        toggle_history(threads);
        if !threads[0].hide_history {
            self.request(visits.request(true, true), Selection::Show);
        }
    }

    pub(in crate::app) fn select(
        &mut self,
        visits: &Visits,
        threads: &[Thread],
        selected: &mut Option<usize>,
        view: &TranscriptView,
        scroll: &mut TranscriptScroll,
        projection: &mut TurnProjection,
        direction: i8,
    ) -> bool {
        self.cancel();
        let thread = &threads[0];
        projection.update(thread);
        let history = projection
            .cells
            .iter()
            .take_while(|cell| cell.prompt < thread.history_len)
            .count();
        let older = direction < 0;
        let boundary = !thread.hide_history
            && history > 0
            && if older {
                *selected == Some(0)
            } else {
                *selected == Some(history - 1)
            };
        let entering = !thread.hide_history
            && history > 0
            && older
            && (*selected == Some(history)
                || (selected.is_none() && scroll.follow && history == projection.cells.len()));
        if entering || boundary {
            self.request(
                visits.request(entering, older),
                Selection::Step { older, entering },
            );
            true
        } else {
            select_trace_turn(selected, view, scroll, direction);
            false
        }
    }

    // Invalidate before routing can consume End/Esc/scroll/overlay actions.
    // Copy and resize leave the still-displayed selection and request intact.
    pub(in crate::app) fn input(
        &mut self,
        event: &Event,
        surface: input::Surface,
        empty: bool,
        capture: bool,
    ) {
        let cancel = match event {
            Event::Key(key) if key.kind != KeyEventKind::Release => {
                input::navigation(
                    event,
                    empty || surface != input::Surface::Transcript,
                    capture,
                    1,
                )
                .is_some()
                    || (input::accepts_key(surface, *key, empty)
                        && match key.code {
                            KeyCode::Esc | KeyCode::Tab | KeyCode::Enter => true,
                            KeyCode::Char('o' | 'p' | 'i' | 'l')
                                if key.modifiers.contains(KeyModifiers::CONTROL) =>
                            {
                                true
                            }
                            KeyCode::Char('?') if empty => true,
                            _ => false,
                        })
            }
            Event::Paste(_) => surface == input::Surface::Transcript,
            Event::Mouse(mouse) if capture => matches!(
                mouse.kind,
                MouseEventKind::Down(_) | MouseEventKind::ScrollUp | MouseEventKind::ScrollDown
            ),
            _ => false,
        };
        if cancel {
            self.cancel();
        }
    }

    pub(in crate::app) fn stop(&mut self) {
        self.cancel();
        self.shared.0.lock().unwrap().stopping = true;
        self.shared.1.notify_one();
    }

    // Reads have no durability obligation: discard queued/results, finish only
    // the in-flight OS call, and join outside the interactive loop.
    pub(in crate::app) fn shutdown(&mut self) -> io::Result<()> {
        self.stop();
        self.join.take().map_or(Ok(()), |join| {
            join.join()
                .map_err(|_| io::Error::other("history loader panicked"))
        })
    }
}

impl Drop for Navigator {
    fn drop(&mut self) {
        self.stop();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

#[cfg(test)]
mod tests;
