"""Launch a two-worker demonstration and retain all run artifacts."""
import argparse
import pathlib
import subprocess
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin-dir", type=pathlib.Path, default=pathlib.Path("build"))
    parser.add_argument("--run-dir", type=pathlib.Path, default=pathlib.Path("runs/demo"))
    args = parser.parse_args()
    binaries, root = args.bin_dir.resolve(), args.run_dir.resolve()
    source = pathlib.Path(__file__).resolve().parents[1]
    root.mkdir(parents=True, exist_ok=False)
    workers, logs = [], []
    try:
        addresses = []
        for index in range(2):
            ready = root / f"ready-{index}"
            log = open(root / f"worker-{index}.log", "w")
            logs.append(log)
            process = subprocess.Popen([str(binaries / "mr_worker"), "--root", str(root / f"worker-{index}"),
                                        "--listen", "127.0.0.1:0", "--ready-file", str(ready)],
                                       stdout=log, stderr=subprocess.STDOUT)
            workers.append(process)
            deadline = time.monotonic() + 10
            while not ready.exists():
                if process.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError(f"worker {index} failed to start; see {log.name}")
                time.sleep(0.01)
            addresses.append(ready.read_text().strip())
        subprocess.run([str(binaries / "mr_coordinator"), "--run-dir", str(root / "job"),
                        "--workers", ",".join(addresses), "--", str(source / "examples/a.txt"),
                        str(source / "examples/b.txt")], check=True, timeout=35)
        print((root / "job/result.tsv").read_text(), end="")
        print(f"Artifacts: {root / 'job'}")
    finally:
        for process in workers:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
        for log in logs:
            log.close()


if __name__ == "__main__":
    main()
