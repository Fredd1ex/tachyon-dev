//! Copy selection, bounded clipboard submission, and platform clipboard commands.
use crate::app::model::items::ItemKind;
use crate::app::model::thread::Thread;
use crate::app::transcript::projection::build_turn_cells;
use crate::app::transcript::text::sanitize_reply_text;
use crate::app::{names, session_file, MouseCapture, TuiEvent};
use crossterm::event;
use crossterm::event::{KeyCode, KeyModifiers};
use std::io;
use std::sync::mpsc;

pub(in crate::app) fn handle_copy_key(
    key: event::KeyEvent,
    mouse_capture: MouseCapture,
    pane_open: bool,
    input: &str,
    threads: &[Thread],
    selected: Option<usize>,
    copy: impl FnOnce(&str) -> bool,
) -> bool {
    let shortcut = matches!(key.code, KeyCode::Char('c' | 'C'))
        && key
            .modifiers
            .contains(KeyModifiers::CONTROL | KeyModifiers::SHIFT);
    if shortcut && !mouse_capture.0 {
        // Native selection is invisible to us. Consume forwarded Copy without
        // copying a cell, inserting text, or falling through to Ctrl+C (quit).
        return true;
    }
    let yank = key.code == KeyCode::Char('y')
        && key.modifiers.is_empty()
        && !pane_open
        && input.is_empty();
    if !shortcut && !yank {
        return false;
    }
    if key.kind != event::KeyEventKind::Release {
        yank_chat_cell(threads, selected, copy);
    }
    true
}

pub(in crate::app) fn yank_chat_cell(
    threads: &[Thread],
    selected: Option<usize>,
    copy: impl FnOnce(&str) -> bool,
) {
    let Some(text) = selected_chat_cell_text(threads, selected) else {
        return;
    };
    // Never write status text to stderr while the alternate-screen TUI is active.
    let _ = copy(&text);
}

pub(in crate::app) fn selected_chat_cell_text(
    threads: &[Thread],
    selected: Option<usize>,
) -> Option<String> {
    let thread = threads.iter().find(|thread| thread.is_foreground)?;
    let cells = build_turn_cells(thread);
    let cell = cells.get(selected.unwrap_or_else(|| cells.len().saturating_sub(1)))?;
    let mut sections = Vec::new();
    for item in cell.items.iter().map(|index| &thread.items[*index]) {
        let label = match item.kind {
            ItemKind::User => names().user.as_str(),
            ItemKind::Reply => names().conversation.as_str(),
            _ => continue,
        };
        let text = if item.kind == ItemKind::Reply {
            sanitize_reply_text(&item.text)
        } else {
            item.text.clone()
        };
        if !text.is_empty() {
            sections.push(format!("{label}:\n{text}"));
        }
    }
    (!sections.is_empty()).then(|| sections.join("\n\n"))
}

#[derive(Debug, PartialEq, Eq)]
pub(in crate::app) enum CopyOutcome {
    SystemClipboard,
    Saved(std::path::PathBuf),
    Failed,
}

impl CopyOutcome {
    pub(in crate::app) fn notice(&self) -> String {
        match self {
            Self::SystemClipboard => "System clipboard copied".into(),
            Self::Saved(path) => {
                format!("Saved to {} (system clipboard unavailable)", path.display())
            }
            Self::Failed => "Copy failed: clipboard helpers and file fallback unavailable".into(),
        }
    }
}

pub(in crate::app) fn clipboard_worker(
    mut deliver: impl FnMut(&str) -> CopyOutcome + Send + 'static,
    notifications: mpsc::Sender<TuiEvent>,
) -> io::Result<mpsc::SyncSender<String>> {
    // One in-flight payload and one waiting; reject new requests when full.
    let (sender, receiver) = mpsc::sync_channel::<String>(1);
    std::thread::Builder::new()
        .name("tui-clipboard".into())
        .spawn(move || {
            while let Ok(text) = receiver.recv() {
                let _ = notifications.send(TuiEvent::Clipboard(deliver(&text)));
            }
        })?;
    // Detached: dropping the session sender closes the queue, never waits for I/O.
    Ok(sender)
}

pub(in crate::app) fn copy_to_clipboard(text: &str) -> CopyOutcome {
    copy_to_clipboard_with(text, clipboard_command, |text| {
        let path = session_file().parent()?.join("clipboard.txt");
        std::fs::write(&path, text).ok()?;
        Some(path)
    })
}

pub(in crate::app) fn clipboard_command(cmd: &[&str], text: &str) -> bool {
    use nix::fcntl::{fcntl, FcntlArg, OFlag};
    use std::io::Write;
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    let Ok(mut child) = std::process::Command::new(cmd[0])
        .args(&cmd[1..])
        .process_group(0)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
    else {
        return false;
    };
    let result = (|| {
        let mut stdin = child.stdin.take()?;
        // A clipboard helper may stop reading while its display server is hung.
        fcntl(stdin.as_raw_fd(), FcntlArg::F_SETFL(OFlag::O_NONBLOCK)).ok()?;
        let mut remaining = text.as_bytes();
        while !remaining.is_empty() {
            if std::time::Instant::now() >= deadline {
                return None;
            }
            match stdin.write(remaining) {
                Ok(0) => return None,
                Ok(n) => remaining = &remaining[n..],
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                Err(_) => return None,
            }
        }
        drop(stdin);
        loop {
            if let Some(status) = child.try_wait().ok()? {
                return Some(status.success());
            }
            if std::time::Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    })();
    // Successful helpers may fork a clipboard owner. Only failed/timed-out
    // helpers should lose their process group; never kill a successful owner.
    if result != Some(true) {
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(child.id() as i32),
            nix::sys::signal::Signal::SIGKILL,
        );
        let _ = child.kill();
        // Reaping must not keep the UI blocked, even for an uninterruptible child.
        std::thread::spawn(move || {
            let _ = child.wait();
        });
    }
    result.unwrap_or(false)
}

pub(in crate::app) fn copy_to_clipboard_with(
    text: &str,
    mut command: impl FnMut(&[&str], &str) -> bool,
    fallback: impl FnOnce(&str) -> Option<std::path::PathBuf>,
) -> CopyOutcome {
    // Try common clipboard tools.
    let cmds: [&[&str]; 3] = [
        &["wl-copy"],
        &["xclip", "-selection", "clipboard"],
        &["xsel", "-b"],
    ];
    for cmd in cmds {
        if command(cmd, text) {
            return CopyOutcome::SystemClipboard;
        }
    }
    // Fallback: write to a file next to the session.
    fallback(text).map_or(CopyOutcome::Failed, CopyOutcome::Saved)
}
