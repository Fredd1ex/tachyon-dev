# v0.7+ - Evaluation And Hardening

Estimated progress: **15%**. Status: **Early**.

## Scope

| Item | Status | Notes |
| --- | --- | --- |
| MLE-bench subset | Missing | No adapter, fixtures, scorer, or baseline. |
| physics case study | Missing | No case-study harness or reproducible corpus. |
| fault injection | Early | A few local failure tests exist; there is no systematic provider/process/storage/restart matrix. |
| performance work | Partial | Timing and token events exist, but no benchmark aggregation, cost model, percentile targets, or regression gates. |
| UX polish | Partial | The TUI has typed-event rendering and compact metrics, but replay, recovery, task control, and failure presentation remain incomplete. |

## Exit Criteria

- Deterministic end-to-end scripted model, tool, worker, and event harnesses.
- Replay tests using anonymized real traces.
- Provider, worker, daemon, persistence, timeout, cancellation, and partial-batch
  fault injection.
- Concurrent soak and restart tests.
- MLE-bench subset and physics case-study baselines.
- Release thresholds for success, failure, timeout, stale publication, latency,
  token usage, and cost.

## Next Work

1. Build the deterministic scenario and replay harness.
2. Add a systematic fault-injection matrix.
3. Add concurrent soak and restart suites.
4. Establish MLE-bench and physics baselines.
5. Define quantitative release thresholds and CI gates.
