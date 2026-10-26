"""Gated competing attempts; the original word-count workload and oracle."""
import argparse
import collections
import json
import pathlib
import tempfile

from cluster import Cluster
from oracle import serial_result


CASES = ["backup_wins", "original_wins", "backup_fails", "original_crashes", "idle_unavailable",
         "pending_priority", "concurrent_limit", "attempt_budget", "exhausted_budget", "malformed_backup",
         "corrupt_backup", "lost_reply", "insufficient_peers", "incomparable_peers", "no_spare", "cleanup"]


def exercise(binaries, root, case):
    number = 40 if case == "pending_priority" else 20 if case == "concurrent_limit" else 5 if case == "insufficient_peers" else 7 if case in {"idle_unavailable", "no_spare"} else 16
    inputs = []
    for index in range(number):
        path = root / f"input-{index}.txt"
        path.write_bytes(b"Alpha beta gamma\ndelta beta\n" * (512 if case == "incomparable_peers" and index == 0 else 64))
        inputs.append(path)
    expected = serial_result(inputs)
    failure = case == "exhausted_budget"
    with Cluster(binaries, root) as c:
        if case in {"idle_unavailable", "no_spare"}:
            c.worker(fault="hold", fault_task=6)
            if case == "idle_unavailable":
                dead = c.worker()
                dead.terminate()
                dead.wait(timeout=3)
        elif case == "concurrent_limit":
            for task in range(3):
                c.worker(fault="hold", fault_task=task, fault_reason="normal")
            for task in range(3):
                c.worker(fault="hold", fault_task=task, fault_reason="speculation")
        else:
            c.worker(fault="hold-crash" if case == "original_crashes" else "hold-error" if failure else "hold", fault_reason="normal")
            backup_fault = {"backup_fails": "hold-error", "exhausted_budget": "hold-error",
                            "malformed_backup": "hold-malformed", "corrupt_backup": "hold-corrupt"}.get(case, "hold")
            c.worker(fault=backup_fault, fault_reason="speculation", lose_reply=int(case == "lost_reply"),
                     service_ms=10 if case == "pending_priority" else 0)
        c.start(inputs, speculation="on", attempt_timeout_ms=4000, job_timeout_ms=15000,
                drain_ms=50 if case == "cleanup" else 5000,
                max_attempts=1 if case == "attempt_budget" else 2 if failure else 3)
        c.gate(0)
        if case in {"idle_unavailable", "no_spare"}:
            c.event("speculation_blocked", 6, "no_idle_other_worker")
            c.release(0)
        elif case in {"insufficient_peers", "incomparable_peers", "attempt_budget"}:
            c.wait(lambda: len([e for e in c.events() if e[1] == "accepted"]) == number - 1, "all peer completions")
            c.event("speculation_blocked", 0, "attempt_budget" if case == "attempt_budget" else "insufficient_reference")
            c.release(0)
        elif case == "concurrent_limit":
            c.gate(1)
            c.gate(2)
            c.gate(3)
            c.gate(4)
            c.event("speculation_blocked", detail="concurrent_limit")
            assert not any(e[1] == "submitted" and e[2] == "2" and e[5] == "speculation" for e in c.events())
            c.release(0)
            index, _ = c.event("accepted", 0)
            # Supersession must not free the still-held backup's physical speculative slot.
            c.event("speculation_blocked", detail="concurrent_limit", after=index)
            assert not any(e[1] == "submitted" and e[2] == "2" and e[5] == "speculation" for e in c.events())
            c.release(3)
            c.event("submitted", 2, "speculation")
            for worker in (1, 2, 4, 5):
                c.release(worker)
        else:
            c.gate(1)
            decision_index, decision = c.event("speculation_decision", 0)
            detail = dict(part.split("=") for part in decision[5].split())
            assert int(detail["peers"]) >= 5
            assert int(detail["original_age_us"]) >= max(200000, int(detail["median_us"]) * 3)
            assert int(detail["original_age_us"]) + 250000 < 4000000
            assert not any(e[1] in {"expired", "attempt_failed"} and e[2] == "0" for e in c.events()[:decision_index])
            if case == "pending_priority":
                normal = [i for i, e in enumerate(c.events()) if e[1] == "submitted" and e[5] == "normal"]
                assert len(normal) == number and max(normal) < decision_index
            if case in {"backup_wins", "pending_priority", "lost_reply"}:
                c.release(1)
                _, accepted = c.event("accepted", 0)
                assert accepted[4] == "1"
                c.event("job_committed")
                before = {p.name: p.read_bytes() for p in (root / "job/accepted").iterdir()}
                c.release(0)
            elif case in {"original_wins", "cleanup"}:
                c.release(0)
                _, accepted = c.event("accepted", 0)
                assert accepted[4] == "0"
                if case != "cleanup":
                    c.release(1)
            elif case == "original_crashes":
                c.release(0)
                c.event("worker_unavailable")
                assert c.workers[0].poll() == 86
                c.release(1)
            else:
                c.release(1)
                _, failure_event = c.event("invalid_candidate" if case in {"malformed_backup", "corrupt_backup"} else "attempt_failed", 0)
                if case == "corrupt_backup":
                    assert "checksum/size mismatch" in failure_event[5]
                    candidate, = (root / "worker-1").glob("*/0/t0-a2/candidate.txt")
                    header, identity, *rows = candidate.read_text().splitlines()
                    assert header == "STRAGGLERMR1" and identity.endswith(" 0 t0-a2")
                    pairs = [row.split("\t") for row in rows]
                    assert [word for word, count in pairs] == sorted({word for word, count in pairs})
                    assert all(word.isascii() and word.islower() and word.isalpha() and int(count) > 0 for word, count in pairs)
                    assert sum(int(count) for word, count in pairs) <= inputs[0].stat().st_size
                # A failed backup must neither fail the job nor launch a third task-0 attempt while the original runs.
                assert not any(e[1] == "submitted" and e[3] == "t0-a3" for e in c.events())
                c.release(0)
        metrics = c.finish(success=not failure)
        events = c.events()
        assert metrics["speculative_outstanding_peak"] <= 2
        assert all(w["peak_active"] <= w["capacity"] for w in metrics["workers"])
        backups = collections.Counter(a["task"] for a in metrics["attempt_records"] if a["reason"] == "speculation")
        assert max(backups.values(), default=0) <= 1
        assert all(a["reason"] in {"normal", "failure_retry", "deadline_retry", "speculation"} for a in metrics["attempt_records"])
        if failure:
            assert not (root / "job/result.tsv").exists()
            assert metrics["speculative_attempts"] == 1
            assert len([a for a in metrics["attempt_records"] if a["task"] == 0]) == 2
        else:
            assert metrics["success"] and metrics["accepted"] == number
            assert (root / "job/result.tsv").read_bytes() == expected
            accepted = [e for e in events if e[1] == "accepted"]
            assert len(accepted) == number and len({e[2] for e in accepted}) == number
        if case in {"backup_wins", "original_wins", "pending_priority", "lost_reply"}:
            assert metrics["speculative_attempts"] == 1 and metrics["late_rejected"] == 1
            assert metrics["backup_wins"] == int(case != "original_wins")
        if case in {"backup_wins", "pending_priority", "lost_reply"}:
            assert {p.name: p.read_bytes() for p in (root / "job/accepted").iterdir()} == before
            for candidate in (root / "worker-0").rglob("candidate.txt"):
                candidate.write_text("late corruption\n")
            assert {p.name: p.read_bytes() for p in (root / "job/accepted").iterdir()} == before
            assert (root / "job/result.tsv").read_bytes() == expected
        if case in {"idle_unavailable", "no_spare", "attempt_budget", "insufficient_peers", "incomparable_peers"}:
            assert metrics["speculative_attempts"] == 0
        if case == "concurrent_limit":
            assert metrics["speculative_attempts"] == 3 and metrics["speculative_outstanding_peak"] == 2
        if case in {"malformed_backup", "corrupt_backup"}:
            assert metrics["invalid_candidates"] == 1 and metrics["backup_wins"] == 0
        if case == "lost_reply":
            assert metrics["uncertain_submissions"] == 1
        if case == "cleanup":
            assert metrics["unresolved_attempts"] == 1 and metrics["backup_wins"] == 0
    cleanup = json.loads((root / "cleanup.json").read_text())
    assert all(item["returncode"] is not None for item in cleanup)
    print(f"PASS speculation/{case}: output, policy bounds, attempt ownership and cleanup")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin-dir", type=pathlib.Path, required=True)
    parser.add_argument("--case", choices=CASES, required=True)
    parser.add_argument("--keep", type=pathlib.Path)
    args = parser.parse_args()
    if args.keep:
        args.keep.mkdir(parents=True, exist_ok=False)
        exercise(args.bin_dir.resolve(), args.keep.resolve(), args.case)
    else:
        with tempfile.TemporaryDirectory(prefix=f"stragglermr-spec-{args.case}-") as directory:
            exercise(args.bin_dir.resolve(), pathlib.Path(directory), args.case)


if __name__ == "__main__":
    main()
