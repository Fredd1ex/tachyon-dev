# TUI Ownership Rules

The public facade is `tachyon_tui::run` in `src/lib.rs`. Everything below `app`
is private to the crate. Paths in this page are relative to
`crates/tachyon-tui/src`. This is a code ownership map, not a new event or storage
contract. See [TUI_REFACTOR.md](TUI_REFACTOR.md) for behavior and service limits.

| Owner | Put Changes Here |
| --- | --- |
| `app/mod.rs` | Private `App` state, startup, service construction, shared event/hit identities, and module wiring |
| `app/event_loop.rs` | Bounded polling, redraw deadlines, checkpoint coordination, terminal restoration, and ordered shutdown |
| `app/input.rs` | Surface routing policy and the key/mouse/action dispatcher; quit is returned to the loop |
| `app/actions.rs`, `app/editor.rs` | Command admission/target resolution and character-indexed composer edits |
| `app/navigation.rs`, `app/clipboard.rs` | Selection/unread/view transitions and copy selection/clipboard commands |
| `app/update.rs`, `app/update/events.rs` | Conversation reducers and UI application of service notifications |
| `app/update/metrics.rs`, `app/update/raw_line.rs` | Correlated aggregate metrics, event identity qualification, and legacy wire-line reduction |
| `app/render.rs` | Whole-frame composition and overlay ordering, delegating surface content |
| `transcript/projection.rs`, `transcript/cache.rs` | Cell identity/revisions and viewport/layout caching |
| `transcript/render.rs`, `transcript/layout.rs` | Visible-row painting and cell composition |
| `transcript/trace.rs`, `transcript/text.rs` | Diagnostic evidence, markdown/text, and compact accounting labels |
| `transcript/response.rs`, `transcript/elapsed.rs` | Conversation bodies and elapsed-time overlays |
| `panels/mod.rs`, `panels/tabs.rs` | Inspector cache/scroll/hits and shared tab registry |
| `panels/agents.rs`, `panels/orchestrators.rs` | Tab content and stable-ID catalog selection |
| `ui/chrome.rs`, `ui/overlays.rs` | Composer/footer and help/info drawing |
| `ui/mod.rs`, `ui/activity.rs`, `ui/format.rs` | Shared geometry/shell, activity styling, and time/lifetime labels |
| `model/` | Thread/item storage, revisions, persisted metrics, and turn activity |
| `services/`, `session_archive.rs` | Existing transport, worker lifetimes, persistence, recovery, and archive schemas |

## Dependency Rules

- Keep `App` fields private. App child modules implement orchestration methods;
  renderers and model reducers receive the specific data they need, not `App`.
- Import owners explicitly. Do not add production `use super::*`, public facade
  exports, or root aliases merely to avoid naming an owning module. Existing root
  aliases support older modules; test-only aliases are gated with `cfg(test)`.
- Do not move the input match, pane drawing, or event reduction back into the event
  loop. Scheduling and shutdown must remain readable independently of content.
- Renderers do not start workers or do filesystem/socket I/O. Input admits work;
  service results update UI state; only a successful terminal draw marks attention
  visible. Preserve those ordering boundaries.
- Service internals are independent owners. An app refactor must not silently alter
  subscription generations, FIFO admission, history durability, or command results.
- Keep regressions beside their owner or in `app/tests/`. Preserve screen/copy
  fixtures and archive schemas. Delete legacy code only after verifying it is
  disabled or has no reachable callers; do not delete tests to simplify a move.

## Verification

Run `cargo test -p tachyon-tui --offline` and
`cargo check -p tachyon-tui --offline`. Screen/copy goldens are ordinary test
assertions, not files to regenerate for a relocation. The synthetic frame benchmark
is opt-in: `cargo test -p tachyon-tui --offline synthetic_frames -- --ignored`.

Known residuals: the dispatcher is still a large match, legacy regression tests
share app-level helpers, some older modules retain root aliases/path declarations,
and conversation hits still use shared mutexes. This extraction does not claim
full pseudo-terminal coverage, new runtime bounds, or new persistence guarantees.
