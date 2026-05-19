# Message Bus Performance Plan

This document provides a practical performance model for `libvmbus` and a roadmap
for additional optimization work.

Scope:

- SHM transport (`OmBusStream` / `OmBusEndpoint`)
- TCP transport (`OmBusTcpServer` / `OmBusTcpClient`)
- WAL record fan-out for local and remote consumers

## Baseline Assumptions

Reference defaults from current implementation:

- SHM ring: `capacity=4096`, `slot_size=256`, `max_consumers=8`
- CRC: optional (`OM_BUS_FLAG_CRC`)
- TCP buffers: server send 256KB per client, client recv 256KB
- Single producer for SHM stream

Baseline latency targets from `docs/msg_bus.md`:

- SHM publish: ~50-80ns/record
- SHM poll: ~30-50ns/record
- TCP client poll (network path): ~10-50us/record

## SHM Performance Estimate

### Throughput Envelope

For single-producer steady state:

```text
publish_rps ~= 1 / publish_ns
```

Range:

- 50ns -> ~20.0M rec/s
- 80ns -> ~12.5M rec/s

Practical planning range (with real payload mix, atomics, cache pressure,
consumer interaction):

- **5M-12M rec/s per stream**

### Backpressure Sensitivity

Producer must satisfy:

```text
head - min_tail < capacity
```

With stale-consumer skipping enabled (`staleness_ns > 0`), dead/idle consumers
stop dominating `min_tail`, reducing producer stalls under partial failures.

### SHM Memory Footprint

```text
memory ~= 4096 (header page)
       + max_consumers * 64
       + capacity * slot_size
```

Default:

- `4096 + 8*64 + 4096*256 = 1,053,184 bytes` (~1.05MB/stream)

## TCP Performance Estimate

TCP is relay-mediated and client-count dependent.

### Per-client Record Rate

If end-to-end per-record latency is 10-50us:

- 10us -> ~100K rec/s per client
- 50us -> ~20K rec/s per client

With loopback/co-located relay and small payloads, practical rates can be
substantially higher. Existing project target remains:

- **~500K-1M rec/s aggregate (deployment dependent)**

### Server Capacity Shape

Broadcast CPU/network cost scales with connected clients and payload size:

```text
server_bytes_per_sec ~= rec_rate * (frame_header + payload) * client_count
```

Example (64-byte payload, 16-byte frame header, 200K rec/s, 16 clients):

- `200,000 * 80 * 16 = 256,000,000 B/s` (~244MB/s user-space egress before kernel overhead)

This is generally CPU-copy and socket-buffer bound before protocol-bound.

## Current Optimization Status

Implemented high-impact items:

- Batch publish API (`om_bus_stream_publish_batch`)
- Phased backpressure loop + optional callback
- Deferred recv-buffer compaction in TCP client
- Fast-path append in TCP server send path
- Thread-safe CRC table init + hardware CRC32C paths
- Stale consumer skipping (`_om_bus_min_tail_live`)
- Auto-reconnect client and slow-client warning path
- Relay burst-drain optimization (batch SHM polls before TCP `poll_io` flush)
- Relay SHM batch polling (`om_bus_endpoint_poll_batch`) for lower per-record overhead
- Relay adaptive burst sizing (dynamic 16-256 batch window)
- SHM `publish_batch` chunked capacity writes (amortized backpressure checks)
- TCP server batch broadcast API (`om_bus_tcp_server_broadcast_batch`)
- Relay stats hooks with histogram-based loop-latency percentile estimation

Correctness/perf interaction fixed recently:

- Mixed `poll_batch` + `poll` sequence tracking now consistent in SHM endpoint

## Measured Snapshot (2026-02-11)

Environment:

- Build: `Release` (`-DENABLE_ASAN=OFF -DENABLE_UBSAN=OFF`)
- Host: local dev machine, loopback for TCP, single client
- Tool: `build_release/tests/bench_bus_perf`

Run set:

- SHM: 5 runs, `--mode shm --shm-iters 100000`
- SHM mixed: 5 runs, `--mode shm-mixed --shm-iters 100000 --shm-batch 32`
- TCP: 5 runs, `--mode tcp --tcp-iters 20000`

Observed medians:

| Path | Median ns/rec | Median rec/s | Run min-max (ns/rec) |
|---|---:|---:|---:|
| SHM (publish+poll loop) | 75.06ns | 13.32M rec/s | 62.47-83.98 |
| SHM mixed (publish_batch+poll_batch, batch=32) | 29.10ns | 34.37M rec/s | 24.11-42.64 |
| TCP loopback (1 client) | 14,027.61ns | 71.3K rec/s | 13,745.61-15,032.30 |

Capacity note:

- SHM measurements align with model range (`~12.5M rec/s @ 80ns`).
- Mixed SHM batch path shows materially higher throughput than single-record
  path on this host (~2.6x median rec/s improvement).
- TCP loopback single-client throughput is currently in the `~50K-73K rec/s`
  envelope under this benchmark style (broadcast+poll cadence bound).

## Roadmap: Performance Improvement Fixes

Priority is ordered by expected impact and implementation risk.

### Remaining P0 (Near-term, low risk)

1. Wire relay percentile stats into production telemetry pipeline/log sink.

Expected gain: better stability and fast regression detection (operational gain,
not raw throughput).

### Remaining P1 (High impact)

1. Per-client bounded queue policy tuning (warning threshold before hard drop)
   to reduce unnecessary disconnect churn for transient spikes.

Expected gain: **15-35% TCP aggregate throughput** in bursty workloads,
improved p99 relay latency.

### P2 (Medium impact)

1. SHM huge-page option for very large rings (`MADV_HUGEPAGE` where available).
2. Tune slot-size classes (e.g., 128/256/512) with profile-guided default
   selection per WAL payload distribution.
3. Add optional producer-side prefetch for slot header/payload write path.

Expected gain: **5-15% SHM throughput** depending on payload mix and hardware.

### P3 (Advanced / optional)

1. io_uring-based TCP server mode (Linux-only optional backend).
2. NUMA-aware relay placement and CPU pinning for high core-count machines.
3. Multi-relay sharding by stream/product domain for horizontal scaling.

Expected gain: deployment-specific; can be significant at high client counts,
but higher complexity.

## Capacity Planning Quick Formulas

SHM producer headroom:

```text
publish_budget_ns = 1e9 / target_rec_rate
```

TCP aggregate bandwidth check:

```text
bw_bytes_sec = rec_rate * (16 + avg_payload_bytes) * clients
```

Use measured p99 latency and sustained 5-10 minute soak runs, not only
microbench medians, before final capacity decisions.

## Validation Checklist

1. SHM microbench (publish only, poll only, mixed) in Release build.
2. Relay + TCP soak (target client count, realistic payload mix).
3. Tail-latency and disconnect-rate tracking under induced slow-client events.
4. Recompute deployment SLO capacity from observed p99, not theoretical best case.

## Benchmark Tool

An executable benchmark harness is available:

- Source: `tests/bench_bus_perf.c`
- Binary: `build/tests/bench_bus_perf`

Example usage:

```bash
cd build
cmake ..
make -j$(nproc)

# SHM benchmark
ASAN_OPTIONS=verify_asan_link_order=0 ./tests/bench_bus_perf --mode shm --shm-iters 100000

# SHM mixed benchmark (publish_batch + poll_batch)
ASAN_OPTIONS=verify_asan_link_order=0 ./tests/bench_bus_perf --mode shm-mixed --shm-iters 100000 --shm-batch 32

# TCP loopback benchmark
ASAN_OPTIONS=verify_asan_link_order=0 ./tests/bench_bus_perf --mode tcp --tcp-iters 20000
```

Treat sanitizer-enabled numbers as relative-only. Use Release-like builds for
capacity planning.
