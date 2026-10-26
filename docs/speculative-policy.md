# Frozen speculative policy — v1

Frozen before final measurements on 2026-10-26. This document and the study manifest are inputs to the experiment, not fitted to its outcomes. Fixing a correctness bug requires a new implementation fingerprint and rerunning all final pairs; thresholds and workload schedules stay unchanged.

The existing synchronous Submit/Inspect control loop and fixed worker pools remain. Speculation is optional (`--speculation on`, default `off`). The fixed policy is:

1. Dispatch all ordinary pending work (initial maps, failure retries, deadline retries) first. If any ordinary task is awaiting dispatch, do not speculate.
2. Only a still-eligible first normal attempt can be considered. It must have neither failed nor expired, and its task must have no accepted result or previous speculative launch.
3. Require at least **five accepted first-normal peer tasks** whose input byte sizes are within a **1.25 ratio** of the candidate input. The workload is unchanged: one word-count map per immutable file. Byte size is only a conservative comparability proxy, not a prediction of arbitrary data distributions.
4. Use the **upper median of their worker-observed execution durations**, including controlled service delays and artifact I/O. Require attempt elapsed time of at least **max(200 ms, 3 × reference median)**.
5. Only launch while the original remains at least **one control-RPC deadline away from its execution timeout**, checking again immediately before Submit. Submit/Inspect are short control RPCs, so the proactive boundary is the original *execution-attempt timeout*, not the already-completed Submit call's deadline.
6. Place the backup on another healthy worker with observed unused physical capacity. No two attempts of a task share a worker. Reserve admission conservatively even if its reply is lost.
7. At most **one speculative backup per logical task**, and **two physically outstanding speculative attempts globally**. Completed attempts release this limit; expiration, supersession or unreachable workers alone do not. Unconfirmed attempts on quarantined endpoints remain charged, conservatively.
8. Speculative launches consume the existing per-task total attempt budget (default **three**, maximum five). An exhausted budget suppresses a backup; it fails the task only when no eligible execution remains. Backups are never recursively speculated.

Every submission records `normal`, `failure_retry`, `deadline_retry` or `speculation` in events and per-attempt metrics. Losing or expired attempts remain physically accounted until a terminal observation. Validation and acceptance keep the Phase 1 ownership boundary: parse once, copy accepted data under coordinator ownership, accept one winner, and never consume a mutable worker path again.

For final measurements, attempt timeout is **2000 ms**, control RPC timeout **250 ms**, job timeout **15000 ms**, drain bound **5000 ms**. The drain observes losers for resource measurements *after* final result publication; it is excluded from reported job/map latency and reported separately. Actual per-attempt occupied-slot duration and thread CPU duration are distinguished. Synthetic delay is occupied time, not CPU work. No threshold/parameter search is part of this study.
