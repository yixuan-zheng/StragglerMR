# Curated original Phase 2 evidence

These are sanitized derivatives of the complete final Phase 2 study, not newly run benchmarks. All 30 observations are retained: five scenarios × three matched repetitions × two modes. The original complete raw observations and workspaces remain private. The earlier pre-integrity study is also retained privately and is not mixed into these results.

- `manifest.json`: unchanged experiment manifest, original runtime/source and binary fingerprints, generic platform/compiler context, and original full observations-file hash. Those hashes identify the original measured implementation, not a claim that packaged build/report plumbing was measured again.
- `observations.jsonl`: all original flat numerical/boolean fields, hashes and logical run identifiers, unchanged. Full coordinator metrics and internal workspace-path records are omitted.
- `attempts.jsonl`: logical attempt/task/worker, reason, winner/retirement/terminal-observation status, actual terminal/crash state and measured CPU/occupied time. No process IDs, job UUIDs or filesystem paths.
- `provenance.json`: redaction specification, original/curated hashes and verified unchanged numerical-analysis artifacts.

`reports/phase2-observations.csv`, pair JSON and both plots reproduce the original final analysis byte-for-byte. Narrative directory descriptions are adapted to this curated tree. `packaging/verify_evidence.py` recomputes the headline and per-attempt totals; `benchmarks/analyze.py --raw data/phase2 --output reports/recomputed` reproduces the tables/plots without executing workers or benchmarking.

Definitions: job latency ends at validated final publication; map latency runs from first dispatch to complete validated acceptance. Full coordinator-process duration includes startup and optional drain. Occupied time includes injected delays; CPU measures worker execution threads through candidate completion, not total coordinator/gRPC/system CPU or the final instrumentation-file write. Wasted work includes all nonwinners, including originals defeated by backups. Crash timing is a local record written before injected process exit; the coordinator may lack its terminal RPC observation. No raw missing duration is presented as a complete zero.
