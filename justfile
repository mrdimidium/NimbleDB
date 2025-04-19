set unstable
set script-interpreter  := ["bash", "-euo", "pipefail"]

help:
  @echo "Welcome to NimbleDB. Please refer to the README for more information on building"
  @just --list

[script]
multitest:
  export CMAKE_GENERATOR="Ninja Multi-Config"
  export CMAKE_CONFIGURATION_TYPES="Release;MinSizeRel;RelWithDebInfo;Debug"

  for shared in 'OFF' 'ON'; do
    for sanitizer in '' 'asan' 'ubsan'; do
      rm -rf "build/"

      cmake -S. -B"build/" -DCMAKE_VERBOSE_MAKEFILE=ON \
        -DNIMBLEDB_FAIL_ON_WARNINGS=ON -DNIMBLEDB_WITH_TESTS=ON -DNIMBLEDB_WITH_CLANG_TIDY=ON \
        -D"BUILD_SHARED_LIBS=$shared" -D"NIMBLEDB_SANITIZER=$sanitizer"

      for build_type in "Debug" "Release" "RelWithDebInfo" "MinSizeRel"; do
        cmake --build "build/" --config "$build_type"
        ctest --test-dir "build/" -C "$build_type"
      done
    done
  done

format:
  clang-format --dry-run -Werror $(git ls-files | grep -E -i '\.(h|c|cc)$')
