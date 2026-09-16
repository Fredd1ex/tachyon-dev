### Output Context
Use `ctx` to navigate this work's exec stream references, not workspace files.
`action=list` returns up to 32 references with an entry cursor. `action=read`
takes `reference:{"id":"output:..."}`, a byte `cursor` (default 0), and `limit`
(default/max 8192 bytes). Read pages use lossy UTF-8 and report decoding loss,
next cursor, retained/total/discarded bytes, storage failure, and more-data flags.

`action=search` also takes a literal UTF-8 `query` (1..1024 bytes, shorter than
limit). It scans one bounded page of raw bytes, returning at most 128 overlapping
match offsets. Follow its overlapping `next_cursor` to find cross-page matches.
At the current end, poll status before treating absence as final. There is no
regex, workspace search, or database access in live-reference mode.
Inspect a bounded sample of unfamiliar output before extracting. Follow relevant
continuations before claiming full coverage; one empty search page proves no more
than that page. Report decoding/storage errors and discarded bytes as coverage
limits, not successful omissions. Python can retain pages for aggregation without
printing every intermediate result.

References are validated against this live work and expire at work end. Legacy
durable envelope references are not in this catalog. Discarded bytes cannot be
recovered. Page content is at most 8 KiB; JSON framing/metadata is additional and
the registry's normal response budget still applies.
## Durable Resources

With host-advertised history enabled, `ctx` also accepts
`{"action":"read","resource":{"kind":"trace","work_id":"...","id":"...","version":"..."},"cursor":0,"limit":1024}`.
This calls the same scoped history adapter as `history.read`; no filesystem path
or database selector is accepted. Do not combine `resource` with a live
`reference` or a search query. External resource pages are limited to 1024 bytes;
their returned history envelope uses `next_offset`, not the live `next_cursor`.

`list(scope="campaign", kinds=["document","artifact"], limit=8)` and
`search(query="literal", scope="current_work", limit=8, cursor="...")` navigate
durable descriptors through the same authorized history tool. Native calls add
`action`; Python uses `ctx = require('ctx')` and awaits these methods. Omit scope
and kinds to preserve the live list/reference-search behavior. Supplying kinds
without scope selects durable `current_work`, using Rust's host Work identity.

Durable kinds are exactly `attempt`, `finding`, `artifact`, `trace`, `document`.
Observations are attempt/trace evidence, not a separate kind. Omit kinds for all.
Scope selects the current Work or its host-authorized campaign, never an arbitrary
campaign ID. History must be installed, enabled, and granted `Control::Resource`;
ctx itself grants no history access. Ready artifact and document backend checks
remain host-owned. Registration pending is not publication.

The durable result has JSON `resources` and `next_cursor` in `content`, bounded
by history's 8 KiB reply cap. Each call fetches exactly one bounded history search
page, then filters by Work and kinds; it never scans until it finds a match. Limits
are 1..16 pre-filter items, literals at most 256 bytes, cursors at most 1024 bytes.
An empty filtered page with a cursor is not exhaustion. Pass the unchanged opaque
string cursor with the same scope/query/kinds; these are live index pages, not a
snapshot. Search matches descriptors, not raw document/artifact/trace bytes.
