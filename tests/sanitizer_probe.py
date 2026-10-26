"""Bounded environment probes, deliberately independent of the runtime/gRPC."""
import json
import os
import pathlib
import subprocess
import sys


def main():
    root = pathlib.Path(sys.argv[1]).resolve()
    root.mkdir(parents=True, exist_ok=False)
    empty = root / "empty.cpp"
    empty.write_text("int main(){return 0;}\n")
    threaded = root / "threaded.cpp"
    threaded.write_text("#include <thread>\n#include <atomic>\nint main(){std::atomic<int> n{0};std::jthread t([&]{n=42;});t.join();return n==42?0:1;}\n")
    results = []
    for name, source, sanitizer, options in [
        ("empty-asan-ubsan", empty, "address,undefined", {}),
        ("threaded-asan-ubsan", threaded, "address,undefined", {}),
        ("threaded-asan-no-leaks", threaded, "address,undefined", {"ASAN_OPTIONS": "detect_leaks=0"}),
        ("threaded-ubsan", threaded, "undefined", {"UBSAN_OPTIONS": "halt_on_error=1:print_stacktrace=1"}),
    ]:
        executable = root / name
        command = ["clang++", "-std=c++20", "-pthread", "-fno-omit-frame-pointer", "-fsanitize=" + sanitizer,
                   str(source), "-o", str(executable)]
        row = dict(name=name, sanitizer=sanitizer, options=options, run_timeout_seconds=5)
        try:
            build = subprocess.run(command, capture_output=True, text=True, timeout=20)
            row.update(compile_returncode=build.returncode, compile_stderr=build.stderr)
            if build.returncode == 0:
                try:
                    result = subprocess.run([str(executable)], capture_output=True, text=True,
                                            env={**os.environ, **options}, timeout=5)
                    row.update(status="passed" if result.returncode == 0 else "failed",
                               returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)
                except subprocess.TimeoutExpired as error:
                    row.update(status="timeout_killed_and_waited", stdout=str(error.stdout), stderr=str(error.stderr))
            else:
                row["status"] = "compile_failed"
        except subprocess.TimeoutExpired:
            row["status"] = "compile_timeout_killed_and_waited"
        results.append(row)
        print(name, row["status"], flush=True)
        (root / "results.json").write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
