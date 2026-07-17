#!/usr/bin/env bash
# Emit native Go benchmark samples for guard selectors that do not require the
# Callgrind scalar-fallback supporting metric.
set -euo pipefail

if [[ $# -eq 1 && $1 == --build ]]; then
	exit 0
fi

if [[ $# -ne 2 ]]; then
	echo "usage: $0 --build | <benchmark-regexp> <go-benchmark-name>" >&2
	exit 64
fi

benchmark_regexp=$1
benchmark_name=$2

if [[ ${PERFLOOP_DISABLE_AVX512:-} == 1 ]]; then
	GODEBUG=cpu.avx512vl=off go test -run '^$' -bench="$benchmark_regexp" -benchmem -count=1 . |
		perfloop-go-bench-json "$benchmark_name" 'ns/op'
else
	go test -run '^$' -bench="$benchmark_regexp" -benchmem -count=1 . |
		perfloop-go-bench-json "$benchmark_name" 'ns/op'
fi
