# Native audit performance comparison — 2026-09-07

The proposed MPSC copy-before-release API costs **6.2% more time per record**
in the longer, race-free serial workload (107.03 → 113.69 ns/record).
Concurrent copy runs processed **72 million records** without payload, sequence,
or skip failures. Legacy concurrent reads failed payload validation in 35 of 36
runs; their timings are not valid correctness-preserving throughput comparisons.

The existing market processing fixture also shows a substantial slowdown in the
full native changeset: **38.4% at 128 subscribers and 29.4% at 1,024 subscribers**
for its blended metric. Performance acceptance remains open. These results do
not justify describing the audit changes as performance-neutral.

## Scope and provenance

- Baseline: archived, unmodified native HEAD
  `3a62b7fdc1ba522e0c03c07f7313caf7bb98a433`.
- Candidate: the sibling VeloxMatch worktree at that HEAD, with the 14 previously
  transferred audit source/header changes. Native source patch SHA-256:
  `d19b81fa97ae6f74d41de820ed45005b3a8d850bee5f9327a10cd3944c311076`.
  The local patch is `build_release/perf-audit-20260907/native-source.patch`.
- Both built in this project's own `build_release` tree, with GCC 16.2.1,
  Release `-O3 -DNDEBUG`, ASan/MSan/UBSan off, prefetch off and khashl off.
- Host: Intel Core i7 860, four physical cores/eight threads. Serial/existing
  fixtures and MPSC consumer pinned to CPU 6; producers pinned to CPUs 0, 2, 4,
  on separate physical cores. This is an active desktop, not an isolated host;
  frequency scaling and scheduling noise limit precision.
- Nine measured rounds after an excluded warm-up, sequential workloads,
  alternating baseline/candidate order and rotating MPSC variants. No benchmarks
  competed with one another or with our builds/tests.
- No further production MPSC edits were made for this investigation. Queue
  reservation, commit, sequencing, memory orders and skip policy remain as in the
  already-proposed changeset. The finance submodule's tracked source remains clean.
- Full worktree, dependency, compiler, CPU and file-hash evidence:
  [provenance.json](perf-audit-20260907/provenance.json).

## MPSC measurements

The repository's existing bus benchmark exercises broadcast SHM/TCP, not MPSC.
The added `tests/bench_bus_mp_perf.c` measures the existing MPSC implementation
without replacing it. Each record carries 336 bytes in a 512-byte slot; capacity
is 4,096 slots, maximum producers three, skip timeout 10 ms. It checks every
payload field, global dequeue sequence, each producer's record index, published
and dequeued totals, and zero skipped sequences. `payload_errors` counts failed
payload checks, not corrupted commands.

Both modes copy into owned storage and validate it. `poll` copies immediately
after the legacy API returns, when the slot has already been released; `copy`
copies before release. Serial mode publishes and consumes on the same thread,
preventing concurrent slot reuse and giving a valid comparison of processing cost.

Long serial confirmation: nine rotated trials, 20 million records per variant
per trial; all 540 million records passed validation. Values below are medians
and observed min–max, not confidence intervals.

| Serial variant | ns/record | Range | Change from baseline median |
|---|---:|---:|---:|
| Baseline `poll` + caller copy | 107.03 | 102.32–110.97 | — |
| Candidate `poll` + caller copy | 106.10 | 101.91–109.76 | −0.9% |
| Candidate `poll_copy` | 113.69 | 106.39–124.58 | +6.2% |

Copy-before-release is +7.2% relative to the candidate's legacy API in this
workload. The shorter initial serial runs showed +14.1% versus baseline, with
much wider ranges (baseline 98.58–137.18, copy 102.09–154.87 ns/record).
The longer result is the primary cost estimate; the difference between the two
experiments illustrates the host's variability. Neither isolates memcpy cycles
from publication, polling and validation overhead.

Concurrent trials used two million records per producer per round:

| Variant | Producers | Failed runs / 9 | Valid median throughput | ns/record range |
|---|---:|---:|---:|---:|
| Baseline legacy `poll` | 1 | 9 | Invalid payloads | — |
| Candidate legacy `poll` | 1 | 8 | Invalid payloads across the series | — |
| Candidate `poll_copy` | 1 | 0 | 7.08 million/s (141.28 ns/record) | 135.33–154.66 |
| Baseline legacy `poll` | 3 | 9 | Invalid payloads | — |
| Candidate legacy `poll` | 3 | 9 | Invalid payloads | — |
| Candidate `poll_copy` | 3 | 0 | 7.68 million/s (130.22 ns/record) | 107.49–165.66 |

Legacy failures were payload mismatches, with no observed sequence assertion or
skip failures. This is consistent with reading storage after producer reuse;
it is not evidence of queue sequence reordering. The unsafe concurrent read can
race a producer write, so its timing cannot establish the cost of an equivalent
correct implementation. Successful finite stress runs do not prove all possible
interleavings or strict price-time behavior of the complete matcher.

## Existing native fixtures

Nine rounds, medians. Percentage changes compare medians, with positive meaning
more time. Broadcast: ten million SHM records and 20,000 loopback TCP records per
run. Market: 64 configured products, 3,000 timed iterations and 300 warm-up
iterations per profile, with 128 and 1,024 subscriber organizations. All these
fixture executions exited successfully.

| Workload | Baseline ns/record or operation | Candidate | Time change |
|---|---:|---:|---:|
| Broadcast SHM | 62.04 | 65.47 | +5.5% |
| Broadcast SHM mixed, batch 32 | 23.92 | 24.29 | +1.5% |
| TCP loopback | 11,858.41 | 12,347.82 | +4.1% |
| Market 128 orgs: insert | 2,337.28 | 3,343.87 | +43.1% |
| Market 128 orgs: match | 2,491.90 | 3,568.44 | +43.2% |
| Market 128 orgs: cancel | 2,867.76 | 3,327.34 | +16.0% |
| Market 128 orgs: blended | 2,573.90 | 3,563.38 | +38.4% |
| Market 1,024 orgs: insert | 24,776.70 | 32,101.94 | +29.6% |
| Market 1,024 orgs: match | 33,021.07 | 39,126.73 | +18.5% |
| Market 1,024 orgs: cancel | 38,072.08 | 43,397.42 | +14.0% |
| Market 1,024 orgs: blended | 28,704.39 | 37,138.30 | +29.4% |

Broadcast differences are small relative to observed variation: SHM ranges were
55.05–75.28 vs 55.34–76.76 ns/record. The median within-round SHM change was +1.6%,
so the +5.5% ratio of medians should not be treated as a precise regression.
Market blended within-round median changes were +37.3% and +31.6%, respectively,
supporting the observed slowdown despite noise. Full ranges and samples are in
[summary.json](perf-audit-20260907/summary.json).

The market fixture calls `om_market_worker_process`; it does not measure
publication/flush or use the MPSC ingress. This comparison includes the entire
native changeset and does not isolate which change causes the market slowdown.
No claim is made that this cost is required by correctness. The fixture's worker
count extrapolation is not a validated deployment sizing result.

## Reproduction and verification

Candidate source build and native tests, from this project's build directory:

```sh
cd ../veloxmatch/build_release
cmake --build . --parallel 4
ctest --output-on-failure -V
```

Final result: **146 checks, zero failures/errors**, including the price-time
execution guardrail added earlier. The new CMake benchmark target built and
passed serial and three-producer copy smoke runs. It is Linux/x86-specific and
requires CPUs 0, 2, 4 and 6 to be available. No container images were built.

The baseline source was extracted using `git archive HEAD` into
`build_release/perf-baseline-source`, with its `deps/klib` and `deps/check`
pointing to this project's same dependencies, and built with:

```sh
cmake -S perf-baseline-source -B perf-baseline -DCMAKE_BUILD_TYPE=Release \
  -DENABLE_ASAN=OFF -DENABLE_MSAN=OFF -DENABLE_UBSAN=OFF -DBUILD_TESTING=ON
cmake --build perf-baseline --parallel 4
```

The same added MPSC source was manually compiled for both timed variants
(commands below run from `build_release`). The baseline macro disables the
unavailable copy API; poll workloads are otherwise identical.

```sh
cc -O3 -DNDEBUG -Wall -Wextra -Werror -std=gnu11 -I../include \
  ../tests/bench_bus_mp_perf.c -Lsrc -Wl,-rpath,"$PWD/src" \
  -lvmbusmp -pthread -o tests/bench_bus_mp_perf
cc -O3 -DNDEBUG -Wall -Wextra -Werror -std=gnu11 -DBENCH_HAS_COPY=0 \
  -Iperf-baseline-source/include ../tests/bench_bus_mp_perf.c \
  -Lperf-baseline/src -Wl,-rpath,"$PWD/perf-baseline/src" \
  -lvmbusmp -pthread -o perf-baseline/tests/bench_bus_mp_perf
bun ../tests/run_perf_audit.js --baseline perf-baseline --candidate . \
  --output perf-audit-rerun --rounds 9
```

The Bun runner retains failed results, completes the experiment, and exits
nonzero if any run failed. Inspect each row's `exit_code` and payload checks.
The initial measurements used the subsequently removed Python runner, which
continued after failures and exited zero; its exit status was not a passing gate.
The recorded measurements and historical provenance are preserved. The original [results.jsonl](perf-audit-20260907/results.jsonl) includes
the excluded warm-up (`trial: -1`) and all nine measured rounds. Longer serial
confirmation used each MPSC binary with `poll 0 20000000` or `copy 0 20000000`,
rotating the three variants over nine rounds; exact commands, binary hashes,
return codes and outputs are in
[serial-long.jsonl](perf-audit-20260907/serial-long.jsonl).

All ns/record values are aggregate elapsed time divided by records, including
fixture validation; they are not queue residence latency or p99. These fixtures
do not establish end-to-end matcher throughput, WAL/fsync latency, price-index
performance, gateway performance or release acceptance. The market regression
requires investigation, and strict matcher ordering remains a correctness gate
independent of throughput.
