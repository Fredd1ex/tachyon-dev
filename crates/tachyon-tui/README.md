# Canonical Transcript

The TUI reads conversation history through daemon `HistoryQuery` and live/recovery
state through `InteractionAttach` snapshots and replacement projections. It never
opens `~/.local/share/tachyon/databases/history.redb` itself.

- Snapshots are assembled at one exact revision, including projection pages and
  opaque byte-oriented content references, on the subscription worker. A gap
  reconnects without resubmitting a command or erasing the last canonical prefix.
- Responses use host turn/session IDs. Acceptance, pending status, prefixes,
  failures and finals are presentation mappings of canonical response records.
  Raw foreground stdout, stderr and AgentSubscribe events cannot create answers,
  change roles, or supply lifecycle state.
- Work cards use admitted work records, typed phases and latest-tool counters.
  Progress uses canonical full-scope counts, not the first page of Todo.List.
  Explicit item details still use the existing daemon todo/evidence interfaces.
- PageUp or captured wheel-up at the loaded top, or selection past the oldest
  cell, requests another daemon history page asynchronously. Stale worker results
  cannot advance the displayed cursor. Timestamp-saturated pages fail explicitly;
  the current HistoryQuery API cannot continue within a millisecond containing
  1,000 or more records. There is no silent skip.
- Local `.visit`, `tui-session.json` and `tui-attention.json` files are not read,
  imported, written or deleted by the running TUI. Old TUI-only content is not
  canonical and remains preserved on disk. There is no periodic transcript
  checkpoint, second-transcript receipt prerequisite, or automatic import.
- Display receipts are sent after terminal display, against daemon-durable bodies.
  Repeated receipts after reconnect are idempotent at the daemon.

## Navigation

- Alt+Up/Down selects a turn. The cyan rail marks selection; green name/metadata
  chrome marks the conversation role/recency, not streamed answer text.
- Selection keeps its fixed gutter and does not rewrap the answer.
- Ctrl+O is inert: no menu, notice, selection change or modal transition.
- Alt+Left/Right focuses a task. Enter/Space with an empty draft, or a captured
  task click, opens its floating details. Escape closes those details.
- Native terminal selection remains the default; `/mouse` enables captured clicks.
- Ctrl+D controls diagnostic details. Copy retains the selected conversation text,
  not card descriptions or control messages.

## Offline Verification

`cargo test --offline -p tachyon-tui --lib`

`cargo test --offline -p tachyon-tui offline_pty_event_loop -- --ignored --nocapture`

Fixtures use temporary files and fake Unix sockets, not an installed daemon,
provider calls, or service restarts. See `docs/interaction/consumers.md` for the
shared client/CLI contract and remaining API limits.
