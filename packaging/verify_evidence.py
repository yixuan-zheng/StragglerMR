"""Verify curated historical numbers and their unchanged runtime/policy source."""
import hashlib
import json
import pathlib
import statistics

ROOT = pathlib.Path(__file__).resolve().parents[1]


def main():
    raw = ROOT / "data/phase2"
    manifest = json.loads((raw / "manifest.json").read_text())
    provenance = json.loads((raw / "provenance.json").read_text())
    for name, digest in provenance["curated_sha256"].items():
        assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest, name
    for name, digest in manifest["source_sha256"].items():
        if name.startswith(("src/", "proto/", "tests/")) or name in {"docs/speculative-policy.md", "benchmarks/manifest.json", "benchmarks/run_study.py"}:
            assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest, name
    rows = [json.loads(line) for line in (raw / "observations.jsonl").read_text().splitlines()]
    assert len(rows) == 30 and all(r["correct"] and r["timing_complete"] for r in rows)
    index = {(r["scenario"], r["repetition"], r["speculation"]): r for r in rows}
    assert len(index) == 30
    speedups = [index["severe", r, "off"]["job_ms"] / index["severe", r, "on"]["job_ms"] for r in range(3)]
    assert f"{statistics.median(speedups):.3f}" == "3.708"
    attempts = [json.loads(line) for line in (raw / "attempts.jsonl").read_text().splitlines()]
    for row in rows:
        records = [a for a in attempts if a["study_run"] == row["raw_run"]]
        assert len(records) == row["attempts"]
        assert sum(a["winner"] for a in records) == 24
        assert sum(a["slot_us"] for a in records) == row["total_slot_us"]
        assert sum(a["cpu_us"] for a in records) == row["total_cpu_us"]
        assert sum(a["slot_us"] for a in records if not a["winner"]) == row["wasted_slot_us"]
        assert sum(a["cpu_us"] for a in records if not a["winner"]) == row["wasted_cpu_us"]
    print("PASS: 30 original observations, per-attempt work totals, frozen source/policy, curated hashes and 3.708x paired severe result")


if __name__ == "__main__":
    main()
