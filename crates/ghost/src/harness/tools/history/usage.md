Read local research data only when the host advertises history. Use search,
attempts, findings or artifacts with a bounded query. Search is literal,
case-sensitive, not regex or semantic recall. Follow next_cursor until null;
an empty page can still have a cursor. Keep the same query filters when paging.
ResourceRefs identify exact versions, not paths or authority. read takes
resource, offset and limit (1..1024 bytes); non-artifacts require offset 0.
Artifact data is a byte array, never implicitly decoded or executed.

Attempts are host observations, including unknown outcomes. A command failure
is not a theorem. Findings are explicitly host-authored interpretations with
conditions, evidence and optional parents. No finding creation tool exists.
No raw command logs or conversation history are automatically inserted.
since_ms excludes legacy evidence with unknown timestamps. Artifact listing
is limited to retained candidate references and requires a configured host
artifact store. No arbitrary file paths or cross-campaign access are supported.
Artifact retrieval validates the stored checksum and rejects snapshots larger
than 4 MiB, even when requesting a smaller byte range.

Python uses the same Rust-generated mappings:
`h = require('history'); p = await h.attempts(query={'limit': 8})`.
Inspect `p['content']` and package `.schemas`/`.guidance`.
Durable tool events are available with `traces(query)`, and explicitly registered
host inputs with `documents(query)`. Use `read(resource, offset, limit)` with an
exact returned ResourceRef; limit is 1..1024 bytes. Search covers descriptors,
not raw content. Trace bytes preserve emitted tool events, not all ephemeral
output spools. Neither history nor a resource reference restores a Python kernel.
## Boundary Snapshots

`history(action="snapshot", query={"limit":8})` lists immutable host-authored
Work context snapshot references. Read them with the normal exact-version
`read` operation, following byte offsets until EOF; a snapshot may span many
pages. Python methods are generated from the same advertised schema.

Snapshots retain host Work/attempt/generation/revision, objective, pending
question IDs, selected input references and allocation availability. Package
versions and live output handles are explicitly informational worker claims,
not host-verified installation, permissions or resumable handles. Null budget
means unavailable, whereas zero means exhausted. No snapshot grants authority
or restores a Python kernel. `traces` also lists bounded model-result evidence.
