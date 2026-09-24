# Interaction Consumers

The wire contract is documented in [manager.md](manager.md). Consumers do not
open the daemon's redb database. CLI `history` uses `HistoryQuery`; CLI `chat`
and the TUI use the manager stream and canonical history publications.

## Shared Client

`Client::interaction_snapshot` returns a fully assembled snapshot. Projection
pages must have the original revision and increasing offsets. An invalidated
revision yields `ClientError::InteractionGap`; callers restart assembly rather
than merging pages. `InteractionSubscription::recv` performs the same assembly
and response-content hydration on its dedicated connection's consumer thread.

Content handles are opaque. Reads stop at the advertised snapshot byte length,
even if an append-prefix handle has grown. Byte pages are concatenated before
UTF-8 decoding. No content reference is used as a path.

The TUI worker performs assembly before sending a snapshot to the UI. Queued
updates are then applied in revision order. Every replacement/removal is applied
before advancing the cursor; event-less work/progress updates are valid. Optional
canonical publications supply history/admission identity, not another answer.

CLI JSON mode emits the assembled canonical frames. Text mode emits canonical
history/publications once per event ID and labels response projection replacements
with exact turn and phase, including in-flight state recovered on attachment.
Final event IDs deduplicate a response replacement against its history publication.
Projection gaps reattach; transport errors do not resubmit accepted work.

## Persistence

The running TUI has no transcript writer or local receipt checkpoint. Legacy
archive codecs remain test-only for preservation regression coverage. Existing
archives are untouched, never auto-imported, and never used to manufacture
canonical IDs. Content that existed only in those archives is not canonical
daemon history. Canonical accepted responses survive reconnect independently of
any UI files. A response whose prompt has left the latest-history window is still
shown under the conversation role; older prompt history is fetched on demand.

## API Limits

`HistoryQuery` currently returns ascending timestamp ranges with a maximum of
1,000 records and no event-ID continuation. The TUI subdivides saturated time
ranges newest-first and advances only past complete windows. A saturated single
millisecond returns an explicit error instead of dropping records. Full cursor-safe
traversal of that case needs a backend `(timestamp, event_id)` continuation API.
No backend files were changed for this migration.

Full content hydration is intentionally requested for transcript copy fidelity;
the visible projection's total text can exceed the bounded inline wire payload.
History pages are fetched only on explicit navigation, not periodic database scans.

## Verification

Run `cargo check --offline --workspace --all-targets`,
`cargo test --offline -p tachyon-client`,
`cargo test --offline -p tachyon --test interaction`, and
`cargo test --offline -p tachyon-tui --lib`.
The optional Linux PTY test is documented in `crates/tachyon-tui/README.md`.
