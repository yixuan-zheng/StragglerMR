"""Execute the frozen serial matched study, preserving complete raw runs."""
import argparse
import hashlib
import json
import pathlib
import platform
import sys
import time

SOURCE = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SOURCE / "tests"))
from cluster import Cluster
from oracle import serial_result


def fingerprint():
    paths = [SOURCE / "CMakeLists.txt", SOURCE / "docs/speculative-policy.md", SOURCE / "benchmarks/manifest.json"]
    for directory, suffixes in (("src", {".cpp", ".h"}), ("proto", {".proto"}),
                               ("tests", {".py", ".cpp"}), ("benchmarks", {".py"})):
        paths += [p for p in (SOURCE / directory).rglob("*") if p.suffix in suffixes and "__pycache__" not in p.parts]
    return {str(p.relative_to(SOURCE)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}


def run_one(binaries, root, manifest, scenario, repetition, mode):
    root.mkdir()
    config = manifest["input"]
    payload = config["payload"].encode("ascii")
    data = (payload * (config["bytes_per_file"] // len(payload) + 1))[:config["bytes_per_file"]]
    inputs = []
    for task in range(config["files"]):
        path = root / f"input-{task}.txt"
        path.write_bytes(data)
        inputs.append(path)
    expected = serial_result(inputs)
    with Cluster(binaries, root) as c:
        for worker in range(scenario["workers"]):
            options = dict(capacity=manifest["worker_capacity"], service_ms=manifest["service_ms"])
            for fault in scenario["faults"]:
                if fault["worker"] == worker:
                    options.update(fault=fault["kind"], fault_task=fault["task"],
                                   delay_ms=fault["delay_ms"], fault_reason="normal")
            c.worker(**options)
        started = time.perf_counter_ns()
        c.start(inputs, speculation=mode, **manifest["coordinator"])
        c.wait(lambda: (root / "job/result.tsv").exists(), "result publication", seconds=20)
        publication_wall_ms = (time.perf_counter_ns() - started) / 1e6
        metrics = c.finish()
        process_wall_ms = (time.perf_counter_ns() - started) / 1e6
        assert (root / "job/result.tsv").read_bytes() == expected, "oracle mismatch"
        assert metrics["success"] and metrics["accepted"] == config["files"]
        assert all(w["peak_active"] <= manifest["worker_capacity"] for w in metrics["workers"])
        assert metrics["speculative_outstanding_peak"] <= 2
        worker_timings = {}
        for path in root.glob("worker-*/**/timing.tsv"):
            job, task, attempt, reason, state, runtime, cpu = path.read_text().strip().split("\t")
            assert job == metrics["job_id"] and attempt not in worker_timings
            worker_timings[attempt] = dict(task=int(task), reason=int(reason), state=state,
                                         slot_us=int(runtime), cpu_us=int(cpu), file=str(path.relative_to(root)))
        assert set(worker_timings) == {a["id"] for a in metrics["attempt_records"]}, "incomplete timing reconciliation"
        total_slot = total_cpu = wasted_slot = wasted_cpu = spec_slot = spec_cpu = 0
        for attempt in metrics["attempt_records"]:
            timing = worker_timings[attempt["id"]]
            assert timing["task"] == attempt["task"]
            total_slot += timing["slot_us"]
            total_cpu += timing["cpu_us"]
            if not attempt["winner"]:
                wasted_slot += timing["slot_us"]
                wasted_cpu += timing["cpu_us"]
            if attempt["reason"] == "speculation":
                spec_slot += timing["slot_us"]
                spec_cpu += timing["cpu_us"]
        row = dict(scenario=scenario["name"], repetition=repetition, speculation=mode,
                   publication_wall_ms=publication_wall_ms, process_wall_ms=process_wall_ms,
                   job_ms=metrics["job_ms"], map_ms=metrics["map_ms"],
                   total_slot_us=total_slot, total_cpu_us=total_cpu,
                   wasted_slot_us=wasted_slot, wasted_cpu_us=wasted_cpu,
                   speculative_slot_us=spec_slot, speculative_cpu_us=spec_cpu,
                   speculative_attempts=metrics["speculative_attempts"], backup_wins=metrics["backup_wins"],
                   attempts=metrics["attempts"], expired=metrics["expired"], failed_attempts=metrics["failed_attempts"],
                   invalid_candidates=metrics["invalid_candidates"], late_rejected=metrics["late_rejected"],
                   submit_requests=metrics["submit_requests"], admitted_replies=metrics["admitted_replies"],
                   uncertain_submissions=metrics["uncertain_submissions"], admission_rejections=metrics["admission_rejections"],
                   worker_peaks=[w["peak_active"] for w in metrics["workers"]],
                   timing_complete=True, correct=True, input_sha256=hashlib.sha256(data).hexdigest(),
                   result_sha256=hashlib.sha256(expected).hexdigest(), worker_timings=worker_timings,
                   metrics=metrics)
        (root / "observation.json").write_text(json.dumps(row, indent=2) + "\n")
    assert all(item["returncode"] is not None for item in json.loads((root / "cleanup.json").read_text()))
    return row


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin-dir", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--manifest", type=pathlib.Path, default=SOURCE / "benchmarks/manifest.json")
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    binaries = args.bin_dir.resolve()
    frozen = fingerprint()
    identity = dict(source_sha256=frozen, binary_sha256={name: hashlib.sha256((binaries / name).read_bytes()).hexdigest()
                                                      for name in ("mr_worker", "mr_coordinator")},
                    platform=platform.platform(), python=sys.version, manifest=manifest)
    (root / "manifest.json").write_text(json.dumps(identity, indent=2) + "\n")
    scenarios = manifest["scenarios"]
    observations = []
    for rep in range(manifest["repetitions"]):
        for index in [(i + rep) % len(scenarios) for i in range(len(scenarios))]:
            scenario = scenarios[index]
            for mode in (("off", "on") if (rep + index) % 2 == 0 else ("on", "off")):
                assert fingerprint() == frozen, "source changed after study freeze"
                name = f"{scenario['name']}-r{rep}-{mode}"
                try:
                    row = run_one(binaries, root / name, manifest, scenario, rep, mode)
                    row["raw_run"] = name
                    observations.append(row)
                    with (root / "observations.jsonl").open("a") as out:
                        out.write(json.dumps(row) + "\n")
                    print(f"{name}: job={row['job_ms']}ms map={row['map_ms']}ms backups={row['speculative_attempts']} wins={row['backup_wins']}", flush=True)
                except Exception as error:
                    (root / "failure.json").write_text(json.dumps(dict(run=name, error=repr(error)), indent=2) + "\n")
                    raise
    assert len(observations) == len(scenarios) * manifest["repetitions"] * 2
    assert fingerprint() == frozen
    print(f"Complete: {len(observations)} matched-study runs retained at {root}")


if __name__ == "__main__":
    main()
