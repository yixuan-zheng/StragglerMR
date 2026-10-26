# Third-party dependency notices

StragglerMR is MIT licensed. It links installed dependencies and generates bindings at build time. Dependencies retain their own licenses; the corresponding license texts and notices are in `third_party/licenses/`.

| Component | Role / tested development version | Primary license / included text |
| --- | --- | --- |
| gRPC | RPC, code-generation plugin; macOS 1.84.0 / Debian package 1.51.1 | Apache-2.0; `third_party/licenses/grpc-LICENSE.txt` and `grpc-NOTICE.txt` |
| Protocol Buffers | Schema compiler/runtime; macOS 36.2 / Debian 3.21.12 | BSD-3-Clause; `protobuf-LICENSE.txt` |
| OpenSSL | SHA-256 and gRPC dependency; macOS 3.6.4 / Debian 3.5.7 | Apache-2.0 for these releases; `openssl-LICENSE.txt` |
| Abseil | Transitive gRPC/Protobuf dependency; macOS 20260817.0 / Debian 20240722.0 | Apache-2.0; `abseil-LICENSE.txt` |
| RE2 | Transitive gRPC dependency; macOS 2025-11-05 / Debian 20240702 | BSD-3-Clause; `re2-LICENSE.txt` |
| c-ares | Transitive DNS dependency; macOS 1.34.8 / Debian 1.34.5 | MIT; `c-ares-LICENSE.txt` |
| utf8_range / utf8_validity | Transitive Protobuf validation support | MIT; `utf8_range-LICENSE.txt` from the Protobuf v36.2 source tree |
| zlib | Transitive compression dependency; macOS system 1.2.12 / Debian 1.3.1 | zlib license; `zlib-LICENSE.txt` preserves the installed 1.3.1 upstream notice |

gRPC's installed aggregate LICENSE also includes BSD-3-Clause terms for specified Objective-C packaging files and MPL-2.0 terms for `etc/roots.pem`. The complete aggregate is included unchanged; those files/certificates are not shipped in this candidate. upb support is part of the linked gRPC/Protobuf distribution, not imported project source. Any future binary/certificate redistribution must review the precise bundled components and applicable notices rather than treating the entire dependency graph as one license.

The installed package license texts were copied verbatim. utf8_range's text was retrieved from its [upstream versioned location](https://github.com/protocolbuffers/protobuf/blob/v36.2/third_party/utf8_range/LICENSE). Relevant upstream projects are [gRPC](https://github.com/grpc/grpc), [Protobuf](https://github.com/protocolbuffers/protobuf), [OpenSSL](https://github.com/openssl/openssl), [Abseil](https://github.com/abseil/abseil-cpp), [RE2](https://github.com/google/re2), [c-ares](https://github.com/c-ares/c-ares) and [zlib](https://zlib.net/). Exact packaged license-file hashes are listed in the release inventory.

Build/test tools are external: CMake (BSD-3-Clause), Python (PSF licensing and bundled component notices), Apple Clang/SDK or GCC/libstdc++ and their platform licenses/runtime exceptions, and Docker with its base OS packages if that optional workflow is used. These tools/images are not redistributed here. The local Linux validation image was built only for testing; its package versions are recorded in validation evidence. Debian package copyright/patch notices and compiler-runtime obligations would need a version-specific review before distributing a built image or binary.
