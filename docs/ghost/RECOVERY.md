# Local Operational Recovery

This is a privileged, same-user development control, not a sandbox or an
authenticated separation from hostile local processes. Worker receipts, stdout,
tool results and candidate claims are **not authoritative billing evidence**.
Only the operator may submit independently obtained evidence through the local
host CLI. Same-user processes already share the daemon control trust boundary.

## Inspect, Recover, Reconcile

`tachyon campaign inspect <id>` starts no jobs. It reports retained staging,
admissions, execution identities, model dispatch identities, the ledger, and an
`expected_state_sha256`. Active durable leases are last-known state, not proof
that an OS process is alive. Inspection does not clear Unknown usage.

`tachyon campaign recover <id> --unisolated-development` reapproves stored staging
policy only. It does not replay execution or settle billing.

```sh
tachyon campaign reconcile <id> receipt.json \
  --unisolated-development --confirm-authoritative
```

Both flags are mandatory. Omitted API flags default to false and are rejected.
The command applies a bounded, strict JSON receipt on the blocking IPC connection
thread, outside the actor/registry lock. The service lifecycle guard rejects a
still-running campaign thread, including cancellation whose scheduler tasks have
not finished. No transaction is held across asynchronous work or network I/O.
One database transaction commits the receipt, billing, execution, admissions,
group lease releases, allocation closure and campaign status, or none of them.

## Evidence Model

There is **no automated provider receipt fetch**. Independently obtain the final
inclusive provider invoice/usage and its request ID. Attest that it belongs to the
exact host dispatch identified by the receipt. Provider response IDs were not
stored by the existing request adapter: their association with legacy reservations
is an operator attestation, not a host-observed match. The host checks the durable
request ID, reservation, allocation, campaign, work, attempt, generation,
instruction revision and provider. It persistently binds the supplied provider
request ID to that exact record; conflicting reuse is rejected across campaigns.

Use a nonsecret evidence identifier such as `case-42` or an evidence digest.
Identifiers permit ASCII letters, digits, `-`, `_`, `.`, and `:` only, up to 256
bytes. Do not put credentials, signed URLs, raw invoices, worker transcripts or
personal information in a receipt. An identifier's syntax cannot prove it is
nonsecret; selecting it is the operator's responsibility.

The local launcher does not persist a PID/start-identity registry sufficient to
safely kill a historical process group. Reconciliation neither kills PIDs nor
infers termination from a missing PID or a cancellation signal. Independently
identify and clean up the original worker, command evaluator and all their
descendants before attesting cleanup. If termination cannot be established, do
not submit cleanup. A free-text assertion is not accepted in place of the explicit
`operator_attests_all_processes_terminated` confirmation.

## Receipt Format

The following is illustrative, not evidence. Substitute exact inspection values
and independently observed usage. All fields are required, unknown fields and
duplicate targets are rejected, and the file is limited to 65,536 bytes and 256
records. The receipt can contain billing only, cleanup only, or both.

```json
{
  "schema_version": 1,
  "command_id": "operator-case-42-final",
  "campaign_id": "campaign-00000000000000000000000000000000",
  "expected_state_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
  "evidence_reference": "case-42",
  "records": [
    {
      "kind": "model_usage",
      "work_id": "work-1",
      "attempt_id": "attempt-1",
      "generation": 1,
      "instruction_revision": 1,
      "reservation_id": "model:00000000-0000-0000-0000-000000000000",
      "allocation_id": "dispatch-1",
      "request_id": "request-1",
      "provider": "openrouter",
      "provider_request_id": "generation-42",
      "input_tokens": 120,
      "output_tokens": 30,
      "cost_micro_usd": 250
    },
    {
      "kind": "cleanup",
      "work_id": "work-1",
      "attempt_id": "attempt-1",
      "generation": 1,
      "reservation_id": "dispatch-1",
      "confirmation": "operator_attests_all_processes_terminated",
      "outcome": "unverified"
    }
  ]
}
```

`request_id` is the host broker's durable logical request ID, not the provider's
response ID. `reservation_id` in cleanup is the original work's admission hold,
not its model request hold. Cleanup targets a recorded execution identity and
covers its worker and verifier processes; it does not cover other child Work.

The expected hash covers the relevant durable control/accounting tables, including
command gates, coordination and prior reconciliation receipts. It is deliberately
global and conservative: another campaign changing those tables can make it stale.
Reinspect and reassess evidence before issuing a new command. Exact retries of a
committed command ID succeed even with its original hash; a changed payload or
campaign scope under that ID conflicts. Receipts and provider bindings are durable.

## Invariants And Limits

- Final model tokens and microUSD are observed cumulative values, never estimates
  or display usage. Totals cannot fall below provisional usage. Final usage is
  immutable, even under a different operator command ID.
- Overruns record debt and leave admissions persistently paused. Reconciliation
  cannot increase envelopes, reset money, forgive debt or override caps.
- Cleanup preserves the stored candidate and produces `Reviewed(Unverified)`,
  never acceptance or rejection. Existing Cancelled campaign status is retained;
  otherwise the campaign becomes Unverified. Only an actual configured gate can
  establish acceptance through the existing verification path.
- Cleanup does not settle model billing. A known non-spending command evaluator's
  own hold may settle at zero after explicit cleanup; an unknown model request
  never does. Untouched admission funding can be released only where no model
  request exists and the host broker never converted the admission into an
  allocation. This is durable proof of unused host funding, not inferred provider
  usage or a claim that a historical process never ran.
- Allocations close only after their request holds are final and their logical
  execution is terminal. Cleanup releases process/group leases while unresolved
  inference holds continue to reserve money. Billing-only reconciliation leaves
  an ExecutingUnknown or ReviewingUnknown execution unresolved.
- Outstanding descendants need their own evidence. Resolving a parent neither
  resolves its descendants nor makes the campaign verified.
- A missing execution identity, unbound/non-command unknown reviewer, missing
  request-to-funding record or uncertain external cleanup is not repaired with an
  unchecked state setter. Such evidence remains unresolved.
- No process, model request, evaluator or retry is launched by reconciliation.
  Unknown originals cannot be retried. Existing bounded repair logic for known,
  completed, rejected command gates is unchanged and cannot be selected in a
  reconciliation receipt.
- Startup retains final reconciled status and does not resume execution. Ordinary
  `resume` still requires an existing ready verification state, never Unknown or
  manually terminated execution.
