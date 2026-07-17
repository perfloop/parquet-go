#!/usr/bin/env bash
# The native Go benchmark supplies ns/op. Callgrind supplies a separately
# labeled instruction reference count on the scalar feature-disabled route,
# where its dynamic call graph is supported even though Valgrind cannot emulate
# the AVX-512 instructions used by the native path.
set -euo pipefail

valgrind_root=${PERFLOOP_CALLGRIND_ROOT:-/workspace/deps/tools/valgrind}
valgrind=$valgrind_root/usr/bin/valgrind
go_cache=${PERFLOOP_GO_CACHE:-"$PWD/.perfloop-go-cache"}

ensure_callgrind() {
	if [[ -x $valgrind ]]; then
		return
	fi

	mkdir -p /workspace/deps/apt/lists/partial /workspace/deps/tools/debs
	package=$(find /workspace/deps/tools/debs -maxdepth 1 -type f -name 'valgrind_*_amd64.deb' -print -quit)
	if [[ -z $package ]]; then
		apt-get -o Dir::State::Lists=/workspace/deps/apt/lists update >&2
		(
			cd /workspace/deps/tools/debs
			apt-get -o Dir::State::Lists=/workspace/deps/apt/lists download valgrind >&2
		)
		package=$(find /workspace/deps/tools/debs -maxdepth 1 -type f -name 'valgrind_*_amd64.deb' -print -quit)
	fi
	test -n "$package"
	rm -rf "$valgrind_root"
	mkdir -p "$valgrind_root"
	dpkg-deb -x "$package" "$valgrind_root"
}

if [[ $# -eq 1 && $1 == --build ]]; then
	: "${PERFLOOP_BENCH_BIN:?PERFLOOP_BENCH_BIN is required in --build mode}"
	ensure_callgrind
	GOCACHE="$go_cache" go test -c -o "$PERFLOOP_BENCH_BIN" .
	exit
fi

if [[ $# -ne 2 ]]; then
	echo "usage: $0 --build | <benchmark-regexp> <selector>" >&2
	exit 64
fi

benchmark_regexp=$1
selector=$2
binary=${PERFLOOP_BENCH_BIN:?PERFLOOP_BENCH_BIN is required for benchmark samples}

case "$selector" in
	BenchmarkBoundsInt32/*) primitive_type=Int32 ;;
	BenchmarkBoundsInt64/* | BenchmarkBoundsInt64CombinedCutoff/* | BenchmarkBoundsInt64WriterDefaultPage) primitive_type=Int64 ;;
	BenchmarkBoundsUint32/*) primitive_type=Uint32 ;;
	BenchmarkBoundsUint64/*) primitive_type=Uint64 ;;
	BenchmarkBoundsFloat32/*) primitive_type=Float32 ;;
	BenchmarkBoundsFloat64/*) primitive_type=Float64 ;;
	*)
		echo "unsupported bounds selector: $selector" >&2
		exit 64
		;;
esac

if [[ ! -x $valgrind || ! -x $binary ]]; then
	echo "callgrind or the compiled bounds test binary is unavailable" >&2
	exit 1
fi

GOCACHE="$go_cache" go test -run '^$' -bench="$benchmark_regexp" -benchmem -count=1 . |
	perfloop-go-bench-json "$selector" 'ns/op'

temporary_directory=$(mktemp -d)
trap 'rm -rf "$temporary_directory"' EXIT
trace=$temporary_directory/callgrind.out

GOMAXPROCS=1 GODEBUG=asyncpreemptoff=1,cpu.avx512vl=off \
	VALGRIND_LIB=$valgrind_root/usr/libexec/valgrind \
	"$valgrind" --tool=callgrind --callgrind-out-file="$trace" \
	"$binary" -test.run '^$' -test.bench="$benchmark_regexp" \
	-test.benchtime=1x -test.count=1 \
	>/dev/null 2>&1

instructions=$(awk -v primitive="$primitive_type" '
function isKernel(symbol) {
	return symbol == "github.com/parquet-go/parquet-go.min" primitive ".abi0" ||
		symbol == "github.com/parquet-go/parquet-go.max" primitive ".abi0" ||
		symbol == "github.com/parquet-go/parquet-go.combinedBounds" primitive ".abi0"
}
FNR == NR {
	if ($0 ~ /^(fn|cfn)=\([0-9][0-9]*\)[[:space:]]+/) {
		id = $0
		sub(/^(fn|cfn)=\(/, "", id)
		sub(/\).*/, "", id)
		symbol = $0
		sub(/^(fn|cfn)=\([0-9][0-9]*\)[[:space:]]+/, "", symbol)
		name[id] = symbol
	}
	next
}
{
	if ($0 ~ /^cfn=\([0-9][0-9]*\)/) {
		id = $0
		sub(/^cfn=\(/, "", id)
		sub(/\).*/, "", id)
		selected = (id in name && isKernel(name[id]))
		awaitingCost = 0
		next
	}
	if (selected && /^calls=/) {
		awaitingCost = 1
		next
	}
	if (selected && awaitingCost && /^\* [0-9][0-9]*$/) {
		total += $2
		found = 1
		selected = 0
		awaitingCost = 0
	}
}
END {
	if (!found || total == 0) exit 1
	print total
}
' "$trace" "$trace")
printf '{"metric":"CPU instructions (Callgrind scalar fallback)","value":%s}\n' "$instructions"
