# Durable Plans And Monitoring

The optional `todo` and `monitor` packages use the private permit-bound broker,
not Resource/history authority or public operator IPC. The trusted host opts in
with `Control::Todo` and `Control::Monitor`. The default broker allowlist stays
empty. Neither package changes Conversation defaults or installs Python.

`scope` defaults to `current_work`. `current_campaign` requires the independent
`TodoCampaign` or `MonitorCampaign` grant. Membership alone is not authority.
There are no supplied Work/campaign IDs, conversation selectors, actors, or budget
overrides. The host derives identity from the current active Work permit and
revalidates durable funding/admission before every operation. Conversation todos
remain a frontend host/operator service.

## Tool Usage

Native tools are `todo` with `action: list|add|update`, and `monitor` with
`action: snapshot`. Python methods are generated from those exact native schemas:

```python
import json
todo = require('todo')
page = json.loads((await todo.list(limit=8))['content'])
created = json.loads((await todo.add(
    title='Check evidence', expected_revision=page['scope_revision'],
    command_id='unique-check-command'
))['content'])
record = created['todo']
await todo.update(id=record['id'], expected_revision=record['revision'],
                  command_id='unique-finish-command', status='completed')
monitor = require('monitor')
sample = json.loads((await monitor.snapshot())['content'])
```

Todo pages contain records, record revisions, scope revision, operational watermark,
and `next_cursor`. Default and maximum private page size is eight records to fit
the broker frame even for escaped descriptions. Echo a non-null cursor unchanged;
restart the query on a stale cursor. No full-plan prompt injection or automatic
listing occurs. Eager guidance survives context reset; query tools for current data.

Both mutations require `expected_revision` and `command_id`. Add compares the
scope revision; update compares the record revision. Identical replay returns the
original durable receipt, even after later edits. Changed command payloads, stale
revisions and stale cursors are structured `conflict` errors with typed metadata.
The generic Python bridge raises `RuntimeError` carrying the same error dictionary.
No Python cache, receipt log, plan file, or policy is authoritative.

Completing or cancelling a todo edits a plan record only. It never completes,
accepts, cancels, or evaluates Work. `work` remains the separate core lifecycle tool.

## Monitor Authority

Snapshots return the exact resolved query and typed payload from the same durable
helper used by daemon monitoring. Sample times are actual source clocks, not model
call times or invented global atomicity. Wide counters are decimal strings;
`unknown` differs from a known `"0"`. CPU/GPU charged wall time is not CPU utilization,
and model calls remain a separate resource class.

Only the selected authorized scope contributes durable records and counts. There
is no Ghost Host scope, unrelated Research browsing, monitor mutation, hidden
cancel, or budget grant. `Control::MonitorAvailability` additionally permits host
aggregate capacity counts with their own sample clocks, without registry identities
or other campaigns' durable data. Without that grant, `capacities` is empty.
An empty capacity list does not mean zero capacity.
Capacity sampling never waits for host source locks. If availability was granted
but a source is contended or poisoned, the private snapshot returns the existing
typed `Unavailable` error rather than a successful empty capacity list. Without
the availability grant, scoped snapshots do not sample those host sources.

Private snapshots are on-demand samples, not daemon subscription versions. Operator
monitoring owns its epoch, sequence, stale-payload cache and subscriptions. Todo
authority is the runtime database and transactional operational feed; monitoring is
a read-only projection, not a second source of truth.

All synchronous durable calls run in the broker's `spawn_blocking` boundary.
Authority locks fence operations but do not cross awaits. The existing private
stream mutex serializes request/reply framing, not durable database transactions.

## Offline Verification

```sh
cargo test --offline -p ghost --lib service_native_python
cargo test --offline -p tachyond durable_services_private
cargo build --offline -p ghost --bin ghost
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test --offline -p tachyond actual_ghost_services_local_model_native_and_python -- --ignored
```

The last fixture uses a fresh Ghost binary, temporary durable stores, localhost
fake providers and existing IPython. It never installs packages or contacts a live
provider. Native and Python model calls add a durable todo, read monitoring, then
read core Work status under the unchanged host-selected model.
