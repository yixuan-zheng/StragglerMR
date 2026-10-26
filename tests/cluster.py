"""Local process ownership and deterministic gate support for Phase 2."""
import json
import pathlib
import subprocess
import time


class Cluster:
    def __init__(self, binaries, root):
        self.binaries = pathlib.Path(binaries).resolve()
        self.root = pathlib.Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.workers, self.endpoints, self.processes, self.logs = [], [], [], []
        self.coordinator = None

    def __enter__(self):
        return self

    def spawn(self, name, command):
        log = open(self.root / f"{name}.log", "w")
        self.logs.append(log)
        process = subprocess.Popen(list(map(str, command)), stdout=log, stderr=subprocess.STDOUT)
        self.processes.append((name, process))
        return process

    def wait(self, predicate, description, seconds=10):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            if self.coordinator and self.coordinator.poll() is not None:
                raise AssertionError(f"coordinator exited waiting for {description}: " +
                                     (self.root / "coordinator.log").read_text())
            time.sleep(0.005)
        raise AssertionError(f"timeout waiting for {description}; events={self.events()}")

    def worker(self, **options):
        index = len(self.workers)
        ready = self.root / f"worker-{index}.ready"
        command = [self.binaries / "mr_worker", "--root", self.root / f"worker-{index}",
                   "--listen", "127.0.0.1:0", "--ready-file", ready,
                   "--event-file", self.root / f"gate-{index}.ready",
                   "--release-file", self.root / f"gate-{index}.release"]
        for key, value in options.items():
            command += ["--" + key.replace("_", "-"), str(value)]
        process = self.spawn(f"worker-{index}", command)
        self.workers.append(process)
        self.wait(lambda: ready.exists() or process.poll() is not None, "worker readiness")
        assert ready.exists(), (self.root / f"worker-{index}.log").read_text()
        self.endpoints.append(ready.read_text().strip())
        return process

    def start(self, inputs, **options):
        command = [self.binaries / "mr_coordinator", "--run-dir", self.root / "job",
                   "--workers", ",".join(self.endpoints)]
        for key, value in options.items():
            command += ["--" + key.replace("_", "-"), str(value)]
        self.coordinator = self.spawn("coordinator", command + ["--", *inputs])
        return self.coordinator

    def gate(self, worker):
        self.wait(lambda: (self.root / f"gate-{worker}.ready").exists(), f"worker {worker} gate")

    def release(self, worker):
        (self.root / f"gate-{worker}.release").touch()

    def events(self):
        path = self.root / "job/events.tsv"
        if not path.exists():
            return []
        # Ignore an incomplete final line while the coordinator is writing.
        return [line.split("\t") for line in path.read_text().splitlines() if len(line.split("\t")) == 6]

    def event(self, kind, task=None, detail=None, after=-1):
        def find():
            for index, row in enumerate(self.events()):
                if index > after and row[1] == kind and (task is None or row[2] == str(task)) and (detail is None or detail in row[5]):
                    return index + 1, row
            return None
        index, row = self.wait(find, f"event {kind}/{task}/{detail}")
        return index - 1, row

    def finish(self, success=True):
        self.coordinator.wait(timeout=20)
        assert (self.coordinator.returncode == 0) == success, (self.root / "coordinator.log").read_text()
        return json.loads((self.root / "job/metrics.json").read_text())

    def __exit__(self, *exc):
        cleanup = []
        for name, process in reversed(self.processes):
            terminated = process.poll() is None
            if terminated:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
            assert process.poll() is not None, f"leaked process {name}"
            cleanup.append(dict(name=name, pid=process.pid, returncode=process.returncode, launcher_terminated=terminated))
        (self.root / "cleanup.json").write_text(json.dumps(cleanup, indent=2) + "\n")
        for log in self.logs:
            log.close()
