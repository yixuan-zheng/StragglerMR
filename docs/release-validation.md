# Fresh release-candidate validation

The source-only candidate was copied to a new clean directory for macOS builds. All build outputs, caches, full logs and raw trace workspaces stayed outside the candidate. Linux builds used a read-only candidate mount, private output storage and no runtime network access beyond container-local loopback. Dependencies were installed during a separately bounded local image build. Tested C++/proto/test files and CMake content were hash-checked against the final candidate afterward.

| Environment | Debug | Release | UBSan only | ASan+UBSan |
| --- | --- | --- | --- | --- |
| macOS 27.0.1, arm64, Apple Clang 17 | 34/34; 9.55 s | 34/34; 9.19 s | 34/34; 9.55 s | Tiny probes stalled; no passing macOS suite |
| Debian 13.7, arm64, GCC 14.2, local Linux container | 34/34; 10.15 s | 34/34; 9.82 s | 34/34; 10.68 s | **34/34; 11.18 s** |

All suites include the unchanged 18 Phase 1 regressions and 16 deterministic speculation cases. Expected injected errors/crashes/exhaustion return explicit failure and are tested as passing negative cases. Project-owned code uses warnings-as-errors. No runtime correctness defect was uncovered, and no benchmark campaign was rerun.

macOS dependencies: CMake 4.4.3, gRPC 1.84.0, Protobuf 36.2, OpenSSL 3.6.4, deployment target 27.0. Linux: CMake 3.31.6, gRPC 1.51.1, Protobuf 3.21.12, OpenSSL 3.5.7; Abseil 20240722.0, c-ares 1.34.5, RE2 20240702 and zlib 1.3.1 from Debian packages. Python 3.11 was used. Container resources were bounded at three CPUs and 3 GiB memory; each configure/build/test stage had an external time bound. The validation container was removed after completion. The optional Dockerfile describes a dependency environment, not a binary/container release or a bit-reproducible package lock.

**Sanitizer statuses:** UBSan passed independently on both platforms with halt-on-error enabled. The Linux ASan+UBSan empty-program probe exited successfully, followed by the genuine passing 34-case combined suite. macOS empty-main and threaded combined probes, including leak detection disabled, compiled but exceeded five seconds and were killed/waited for without diagnostics; this remains a local toolchain/environment issue of unidentified exact cause. No sanitizer diagnostic failures were recorded. These macOS stalls are not counted as passing. **TSan is untested on both platforms.** Installed third-party libraries were not rebuilt with instrumentation.

Launchers terminate worker processes rather than exercising a cooperative graceful-shutdown interface. Signal termination and injected abrupt exits do not provide complete worker-teardown leak coverage; the sanitizer passes are not a claim of exhaustive leak/lifecycle verification.

The [representative packaged-source trace](execution-trace.md) reproduced proactive backup admission, backup acceptance, original late rejection and unchanged accepted/final bytes after a further old-workspace overwrite. It matched the independent serial oracle. Launcher cleanup records confirm all children exited and were waited for. The curated trace excludes original paths, job UUID and process IDs.

[Machine-readable results](validation-results.json) preserve counts/statuses without internal build paths. The evidence verifier and analyzer also reproduced the original Phase 2 CSV, pair JSON and both plots byte-for-byte.
