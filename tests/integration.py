"""Real processes and gRPC, fresh inputs, independent oracle; no course fixtures."""
import argparse
import json
import pathlib
import random
import subprocess
import tempfile
import time

from oracle import serial_result


def wait_for(predicate, processes, description, seconds=10):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        for process in processes:
            if process.poll() is not None:
                raise AssertionError(f"process exited while waiting for {description}: {process.returncode}")
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {description}")


def exercise(bin_dir, case, root):
    processes, logs = [], []
    try:
        samples = [b"Alpha beta ALPHA!", b"beta gamma\nGamma", b"", b"123 - __", b"x\x00X\xffz",
                   "Caf\u00e9 HELLO hello\n".encode(), b"trailingword", b"boundary" * 1500 + b" End"]
        generator = random.Random(20261026)
        samples.append(bytes(generator.choices(b"ABCxyz pq\n\t01\xff\x00", k=32768)))
        if case == "capacity":
            samples = samples * 4
        inputs = []
        for index, sample in enumerate(samples):
            path = root / f"input-{index}.txt"
            path.write_bytes(sample)
            inputs.append(path)
        expected = serial_result(inputs)
        workers = int(case[-1]) if case.startswith("healthy") else 2
        if case in {"capacity", "exhausted", "unavailable", "input_error", "oversized_input", "saturated_timeout", "publication_error", "protocol"}:
            workers = 1
        fault = {"retry_error": "error", "malformed": "malformed", "missing": "missing",
                 "crash": "crash", "late": "hold", "lost_reply": "lost-reply", "exhausted": "error",
                 "capacity": "delay", "protocol": "hold", "saturated_timeout": "hold"}.get(case)
        endpoints = []
        for index in range(workers):
            ready = root / f"ready-{index}"
            command = [str(bin_dir / "mr_worker"), "--root", str(root / f"worker-{index}"),
                       "--listen", "127.0.0.1:0", "--ready-file", str(ready),
                       "--capacity", "2" if case == "capacity" else "1"]
            if index == 0 and fault:
                command += ["--fault", fault, "--event-file", str(root / "fault-started"),
                            "--release-file", str(root / "release"), "--delay-ms", "1500"]
            log = open(root / f"worker-{index}.log", "w+")
            logs.append(log)
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
            processes.append(process)
            wait_for(ready.exists, [process], "worker readiness")
            endpoints.append(ready.read_text().strip())
        if case == "protocol":
            subprocess.run([str(bin_dir / "mr_protocol_probe"), endpoints[0], str(inputs[0]),
                            str(root / "release")], check=True, timeout=10)
            return
        if case == "unavailable":
            processes[0].terminate()
            processes[0].wait(timeout=5)
        run = root / "job"
        command = [str(bin_dir / "mr_coordinator"), "--run-dir", str(run),
                   "--workers", ",".join(endpoints), "--job-timeout-ms", "10000",
                   "--attempt-timeout-ms", "250" if case == "late" else "5000"]
        if case == "exhausted":
            command += ["--max-attempts", "1"]
        if case == "saturated_timeout":
            # No free physical slot exists after retirement: the job must fail boundedly.
            command[command.index("--job-timeout-ms") + 1] = "500"
            command[command.index("--attempt-timeout-ms") + 1] = "100"
        if case == "late":
            command += ["--drain-ms", "3000"]
        if case == "publication_error":
            command += ["--output", str(root / "absent-directory" / "result.tsv")]
        if case == "input_error":
            inputs = [root / "missing-input"]
        if case == "oversized_input":
            inputs[0].write_bytes(b"x" * (4 * 1024 * 1024 + 1))
        command += ["--", *map(str, inputs)]
        log = open(root / "coordinator.log", "w+")
        logs.append(log)
        coordinator = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        processes.append(coordinator)
        if case == "late":
            wait_for((root / "fault-started").exists, [coordinator, processes[0]], "paused original")
            wait_for(lambda: (run / "result.tsv").exists(), [coordinator], "retry result publication")
            assert not (root / "release").exists()
            before = {p.name: p.read_bytes() for p in (run / "accepted").iterdir()}
            assert (run / "result.tsv").read_bytes() == expected
            (root / "release").touch()
        coordinator.wait(timeout=20)
        if case in {"exhausted", "unavailable", "input_error", "oversized_input", "saturated_timeout", "publication_error"}:
            assert coordinator.returncode != 0, "failure was silently reported as success"
            assert not (run / "result.tsv").exists()
            if case not in {"input_error", "oversized_input"}:
                failed = json.loads((run / "metrics.json").read_text())
                assert failed["success"] is False
                if case == "saturated_timeout":
                    assert failed["attempts"] == 1 and failed["expired"] == 1
                    assert failed["workers"][0]["peak_active"] == 1
            assert "failed:" in (root / "coordinator.log").read_text()
            return
        assert coordinator.returncode == 0, (root / "coordinator.log").read_text()
        assert (run / "result.tsv").read_bytes() == expected, "output differs from serial oracle"
        metrics = json.loads((run / "metrics.json").read_text())
        assert metrics["success"] and metrics["accepted"] == len(inputs)
        events = [line.split("\t") for line in (run / "events.tsv").read_text().splitlines()]
        accepted = [e for e in events if e[1] == "accepted"]
        assert len(accepted) == len(inputs)
        assert len({e[2] for e in accepted}) == len(inputs), "logical task accepted twice"
        assert len(list((run / "accepted").iterdir())) == len(inputs)
        for worker in metrics["workers"]:
            assert 0 <= worker["peak_active"] <= worker["capacity"]
        if case.startswith("healthy"):
            assert metrics["attempts"] == len(inputs) and metrics["failed_attempts"] == 0
        if case == "capacity":
            assert metrics["workers"][0]["peak_active"] == 2
        if case in {"malformed", "missing"}:
            assert metrics["invalid_candidates"] == 1
        if case in {"retry_error", "crash", "malformed", "missing"}:
            assert metrics["attempts"] == len(inputs) + 1
            assert metrics["failed_attempts"] >= 1
        if case == "lost_reply":
            assert metrics["uncertain_submissions"] == 1
            assert metrics["attempts"] == len(inputs), "lost reply caused a duplicate execution"
        if case == "late":
            assert metrics["expired"] == 1 and metrics["late_rejected"] == 1
            # The held original occupied its worker's only slot throughout the retry.
            assert len([e for e in events if e[1] == "submitted" and e[4] == "0"]) == 1
            assert {p.name: p.read_bytes() for p in (run / "accepted").iterdir()} == before
            # Even subsequent writes in the old workspace cannot touch accepted bytes.
            for candidate in (root / "worker-0").rglob("candidate.txt"):
                candidate.write_text("late overwrite\n")
            assert {p.name: p.read_bytes() for p in (run / "accepted").iterdir()} == before
            assert (run / "result.tsv").read_bytes() == expected
        print(f"PASS {case}: {metrics['tasks']} tasks, {metrics['attempts']} attempts")
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
        for log in logs:
            log.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin-dir", type=pathlib.Path, required=True)
    parser.add_argument("--case", required=True)
    parser.add_argument("--keep", type=pathlib.Path, help="retain inputs, artifacts, logs and metrics in a new directory")
    args = parser.parse_args()
    if args.keep:
        args.keep.mkdir(parents=True, exist_ok=False)
        exercise(args.bin_dir.resolve(), args.case, args.keep.resolve())
    else:
        with tempfile.TemporaryDirectory(prefix=f"stragglermr-{args.case}-") as directory:
            exercise(args.bin_dir.resolve(), args.case, pathlib.Path(directory))


if __name__ == "__main__":
    main()
