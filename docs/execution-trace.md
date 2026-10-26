# Representative speculative execution trace

This is a newly reproduced gated correctness test of the packaged source, not a new performance measurement. Both original and backup ran behind explicit release files. Fifteen comparable peers established the runtime baseline before backup admission.

| Elapsed ms | Event | Attempt / explanation |
| --- | --- | --- |
| 3 | submitted | t0-a1 on worker 0 (normal) |
| 208 | speculation_decision | peers=15 median_us=230 threshold_us=200000 original_age_us=205326 |
| 208 | submitted | t0-a2 on worker 1 (speculation) |
| 221 | accepted | t0-a2 |
| 222 | job_committed | coordinator-owned result.tsv |
| 234 | late_rejected | t0-a1 |

The harness released the backup first, waited for validated acceptance/publication, then released the original. Its late completion was rejected. A subsequent overwrite of the old candidate left all accepted artifacts and final output unchanged. The complete output matched the independent serial oracle. Worker peaks stayed within capacity, and the launcher terminated/waited for all children.

The [sanitized event file](../data/trace/events.tsv), [word-count result](../data/trace/result.tsv) and [verification record](../data/trace/verification.json) retain the evidence. Only the opaque job identifier and absolute final path were replaced; event order, elapsed values, logical IDs and decision values are preserved. Full raw workspaces and logs remain private.

Reproduce in a fresh local directory:

```sh
python3 tests/speculation.py --bin-dir build-release --case backup_wins --keep runs/trace-reproduction
```
