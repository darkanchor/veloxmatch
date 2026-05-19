# VeloxMarket Performance Improvement Plan

This note captures practical performance improvements for VeloxMarket and estimates
their impact for the reference workload:

- **5,000 orgs**
- **10,000 products per org**
- **1,000,000 WAL records/sec**
- **0.5s aggregation budget per 1.0s publish window**

## Baseline (Current)

Using the current model in `docs/market_data.md`:

- Product-level fixed cost per record: **~65ns**
- Blended per-org fan-out cost: **~16ns/org**
  - (60% INSERT @ 15ns, 30% MATCH @ 15ns, 10% CANCEL @ 25ns)
- Constraint:

```text
65ns + (5000 / W) * 16ns <= 1000ns
```

- Required private workers: **~86**

## Suggested Improvements

Priority is ordered by expected impact on throughput/latency and implementation risk.

1. **Cache ladder index per product subscriber (remove repeated find path)**
   - Today fan-out loops call `om_market_worker_find_ladder()` for each org.
   - Store a direct ladder index array alongside `product_orgs`.
   - Main effect: lower per-org fan-out overhead and fewer branches.

2. **Pre-initialize and pre-size all delta maps**
   - Avoid lazy `kh_init` on first update in hot paths.
   - Reduces latency spikes and improves p99 consistency.

3. **Reuse scratch map in private `copy_full`**
   - Keep a per-worker scratch `price->qty` map and `kh_clear` each cycle.
   - Removes frequent alloc/free from publish path.

4. **Add insert-position hints for Q1 ladder insertion**
   - Current insertion for new price levels is O(L) linked-list scan.
   - Add finger hint (`last_insert`) and head/tail fast-path heuristics.

5. **Ring buffer backoff and less expensive pressure handling**
   - Add CPU relax/backoff in spin loops.
   - Reduce repeated full min-tail rescans during contention.

6. **Targeted wakeup instead of broad wakeup when possible**
   - Replace frequent `pthread_cond_broadcast` behavior for larger consumer sets.
   - Main effect: lower wake contention and CPU waste.

7. **Optional: compact org lookup structure when org cardinality per worker is small**
   - Improves cache locality over wide sparse lookups.

## Implementation Status

Completed in code:

- Cached per-subscription ladder indices in fan-out loops
- Pre-initialized private/public delta maps (no lazy hot-path `kh_init`)
- Reused private `copy_full` scratch qty map (`kh_clear` instead of alloc/free)
- Added ring enqueue spin backoff (`pause` + periodic `sched_yield`)
- Added consumer-side `min_tail` refresh to reduce producer full rescans
- Added ladder insertion hint fields and hint-guided insertion search

Planned but not implemented yet:

- Replace/augment `pthread_cond_broadcast` with targeted wake strategy for very high consumer counts

Remaining optional work:

- Compact org lookup layout for sparse org-id distributions
- Further wake strategy tuning for very high consumer counts

## Test Coverage Status

Market test coverage was expanded to protect recent performance-path changes and
sharding behavior in `tests/test_market.c`:

- lifecycle/idempotency checks for `DEACTIVATE`/`ACTIVATE` and repeated transitions
- ring API error-path coverage (invalid params, invalid consumer ids)
- multi-worker and multi-public-worker sharding behavior
- delta copy truncation and bid/ask side-isolation checks
- private `copy_full` correctness under mixed ownership + match/cancel sequence

## Estimated Impact by Component

The table below estimates realistic deltas for the implemented core optimizations
(with additional upside if wake strategy and org-index compaction are also applied).
Numbers are directional planning estimates, not benchmark guarantees.

| Component | Baseline | Projected | Rationale |
|---|---:|---:|---|
| Product fixed cost per record | 65ns | 55ns | Better insert hint behavior + fewer occasional path penalties |
| Per-org fan-out blended cost | 16ns/org | 10ns/org | Cached ladder index + pre-init delta maps + tighter hot loop |
| Publish-path alloc overhead | non-zero | near-zero | Scratch-map reuse |

## 5000x10000 Scenario Estimate

### New worker requirement

With projected costs:

```text
55ns + (5000 / W) * 10ns <= 1000ns
W >= 53
```

Estimated private workers after all improvements: **~53**.

### Improvement vs current baseline

- Current estimate: **~86 workers**
- Projected estimate: **~53 workers**
- Reduction: **~33 workers (~38%)**

### Headroom at current worker count

If you keep **86 workers** after optimization:

- Current per-record time: `65 + (5000/86)*16 ~= 995ns`
- Projected per-record time: `55 + (5000/86)*10 ~= 636ns`

Approximate ingest headroom improvement at same worker count:

- `995ns -> 636ns` => **~36% lower per-record cost**
- Equivalent throughput headroom: **~1.56x**

## Conservative/Optimistic Range

To account for cache pressure, callback variability, and record mix drift:

| Case | Fixed cost | Per-org cost | Required workers |
|---|---:|---:|---:|
| Conservative | 60ns | 12ns | 64 |
| Target (recommended planning) | 55ns | 10ns | 53 |
| Optimistic | 50ns | 8ns | 43 |

Recommended capacity planning target: **53-64 private workers** (target at 53,
operate with safety margin toward 64 until perf tests confirm production behavior).

## Validation Plan

Before locking capacity numbers, benchmark these four micro-paths:

1. INSERT fan-out loop (per-org ns)
2. MATCH fan-out loop (per-org ns)
3. Q1 new-price insertion (tail-heavy and mid-book distributions)
4. Ring enqueue/dequeue under sustained backpressure

Then recompute:

```text
W >= (O * per_org_ns) / (1000ns - fixed_ns)
```

with measured `per_org_ns` and `fixed_ns`.

## Benchmark Harness

A standalone benchmark tool is available to measure `fixed_ns` and `per_org_ns`
from the current implementation:

- Source: `tests/bench_market_perf.c`
- Binary: `build/tests/bench_market_perf`

### Build

```bash
cd build
cmake ..
make -j$(nproc)
```

### Run (example)

```bash
cd build
ASAN_OPTIONS=verify_asan_link_order=0 \
  ./tests/bench_market_perf \
  --orgs 1024 \
  --products 10000 \
  --iters 20000 \
  --warmup 2000 \
  --total-orgs 5000
```

### Output

The tool prints:

- low/high org profile timings for INSERT/MATCH/CANCEL
- fitted `fixed_ns` and `per_org_ns`
- computed worker estimate with:

```text
W >= (O * per_org_ns) / (1000 - fixed_ns)
```

### Important Notes

- Use a Release-like build for capacity planning (sanitizers materially increase
  timings and can make worker estimates pessimistic or unavailable).
- Keep `iters` large enough for stable numbers; compare multiple runs and use
  the median.

## Revision History

- 2026-02-07: Initial perf plan and 5000x10000 worker-capacity estimate added.
- 2026-02-07: Added implementation-status updates after core optimizations
  (cached ladder indices, pre-init deltas, scratch-map reuse, ring backoff,
  consumer-side min-tail refresh, insertion hints).
- 2026-02-07: Added benchmark harness documentation for
  `tests/bench_market_perf.c`.
- 2026-02-07: Added test-coverage status summary for market lifecycle,
  idempotency, ring error paths, sharding, and delta/copy_full correctness.
