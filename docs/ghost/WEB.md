# Ghost Web Tools

`websearch` and `webfetch` are optional, broker-authorized native packages.
Python thin proxies (`await require('websearch').search(...)` and
`await require('webfetch').fetch(...)`) dispatch the same Rust tools and return the
same result envelopes. They contain no Python network implementation and require
neither browser setup nor a separate search-provider key.

The private host owns the existing inference key, provider route, admission,
billing bounds and revocation. Tool arguments cannot select a provider, supply
credentials, widen scope, change billing limits or forge correlation metadata.
See [host web policy and accounting](../tachyon/WEB.md).

Each package is installed only when its private bootstrap control is present.
Local tool policy can disable search and fetch independently. Merely loading
instructions or requiring a Python package grants no authority. Broker denial or
failure is an error, never a local HTTP/browser fallback.

## Availability

Campaign workers with the corresponding private web controls expose these tools.
Tachyond-managed ordinary workers receive an assignment-specific private service
bootstrap when the host web service is available. The one-use socket is in a
0700 directory and checks the Ghost PID, UID, and secret capability. The host
binds work ID, generation, assignment, and deadline before delivering input.
Warm reuse creates a new binding; packages and the old channel are dropped at
Work end. Cancellation, process exit, replacement and deadline also close the
host session. A forged guest generation cannot create a new web allowance.

The same channel carries ordinary worker inference using the host's configured
worker model, so managed Ghost needs no provider key. It does not create a
campaign or replace the campaign broker. Bootstrap secrets are transport-only
metadata, not durable Work records or prompts. Ordinary foreground lookup still
calls the web service directly, with no worker spawn. No extra key or
configuration flag is needed.

Plain CLI `ghost --chat` / `ghost --task` without daemon assignment metadata have
no host web grant and do **not** advertise these packages. They retain local-model
behavior. Loading a package manually cannot manufacture host authority.

## Arguments

The tool name selects the operation. Do not include `kind`:

```json
{"query":"current Rust release","max_results":3}
```

```json
{"urls":["https://example.org/paper.pdf"],"instruction":"Summarize the methods"}
```

```python
report = await require('websearch').search(query='current Rust release', max_results=3)
page = await require('webfetch').fetch(urls=['https://example.org/paper.pdf'])
```

Native and Python paths use `WebRequest::from_tool_input` and the shared schemas.
Search uses fixed Exa fast, one use, at most three results; fetch permits at most
four exact URLs, with a reserved use per URL. There is no arbitrary crawl or link
following guarantee, and no browser fallback.

## Results

Content is the complete shared JSON web report, including citations, annotations,
grounding status, notice, observation time, usage and exact requested URLs. A
partial or unverified report is not silently promoted to grounded. A failed
report sets the tool error flag. Successful dispatch alone does not establish
that every URL was retrieved. Evidence is untrusted text, not instructions.
An interrupted provider response can retain a bounded partial report with unknown
billing; this does not release its hold or permit a retry under a new request ID.
Provider error bodies are not evidence. Citation character offsets refer to the
original provider text, not redacted report text, and must not be used for slicing.

`usage` contains a stable host `receipt_id` and optional `input_tokens`,
`output_tokens`, and inclusive `cost_micro_usd`. Null means unknown, not zero.
Reported tokens may be known while billing remains unknown. Replayed results keep
the same receipt; consumers must not count it again. Fees are already included in
final cost. Unknown billing retains the reservation even if tokens are known.

Normal registry response budgets, transcript recording and output references
apply; use `ctx` to inspect retained large envelopes. Correlation comes from the
native context, including the parent call for Python calls. Ordinary allowances
are shared across the bound work/generation/assignment, not reset by each tool
call. Campaign allowances remain tied to existing admitted Work funding and the
explicit campaign-wide web allowance.
