# Market Data Aggregation (VeloxMarket)

This document describes how VeloxMarket consumes WAL records and builds
**public** and **private** market data per product/price/side.

For optimization planning and capacity-impact estimates, see
[`docs/perf_market_data.md`](perf_market_data.md).

## Definitions

- **Public ladder**: total remaining quantity at each price level for a product.
- **Private ladder**: dealable remaining quantity per org at each price level.
- **Top-N**: the top `N` price levels published per side (bid/ask).
- **Dealable callback**: `uint64_t dealable(const OmWalInsert *rec, uint16_t viewer_org, void *ctx)`
  returns the maximum dealable quantity for an org (0 = not dealable). Called both during
  WAL ingest (for delta tracking) and at query/publish time (for computing per-org qty).

## Data Ownership & Sharding

- **Private workers** are **sharded by org**.
- **Public workers** are **sharded by product** (via `product_to_public_worker`).
- Private and public workers can consume the **same** WAL stream or different streams.
- Each worker maintains its own slab, ladders, and order maps (no cross-worker writes).

## Memory Architecture: Slab + Intrusive Queue

VeloxMarket uses the same architectural pattern as VeloxMatch:
- **Slab allocator** for fixed-size price level slots
- **Intrusive queues** linking slots together (no external node allocation)
- **uint32_t indices** instead of pointers (fits more in cache line)
- **Hash map** for O(1) price → slot lookup

### Price Level Slot (32 bytes = 2 slots per cache line)

```c
#define OM_MARKET_SLOT_NULL UINT32_MAX

typedef struct OmMarketLevelSlot {
    // Q0: free list links (8 bytes)
    uint32_t q0_next;           // next free slot
    uint32_t q0_prev;           // prev free slot

    // Q1: price ladder links (8 bytes)
    uint32_t q1_next;           // next slot (worse price)
    uint32_t q1_prev;           // prev slot (better price)

    // Data (16 bytes)
    uint64_t price;
    uint64_t qty;
} OmMarketLevelSlot;            // 32 bytes exactly
```

### Queue Definitions

| Queue | Purpose | Links Used | When Active |
|-------|---------|------------|-------------|
| **Q0** | Free list (slab internal) | q0_next, q0_prev | Slot is free |
| **Q1** | Price ladder (sorted) | q1_next, q1_prev | Slot is active |

A slot is either in Q0 (free) or Q1 (active), never both. The links could be
unioned, but keeping them separate allows future expansion (e.g., additional
queues for LRU eviction or time-ordered tracking).

### Slab Structure

```c
typedef struct OmMarketLevelSlab {
    OmMarketLevelSlot *slots;   // contiguous array, cache-line aligned allocation
    uint32_t capacity;          // total slots
    uint32_t q0_head;           // free list head
    uint32_t q0_tail;           // free list tail
    uint32_t free_count;        // slots available
} OmMarketLevelSlab;
```

### Ladder Structure (Q1 Queue Heads)

```c
KHASH_MAP_INIT_INT64(om_level_map, uint32_t)  // price → slot_idx

typedef struct OmMarketLadder {
    // Q1 heads/tails for bid side
    uint32_t bid_head;          // best bid (highest price)
    uint32_t bid_tail;          // worst bid (lowest price)
    uint32_t bid_count;         // active bid levels

    // Q1 heads/tails for ask side
    uint32_t ask_head;          // best ask (lowest price)
    uint32_t ask_tail;          // worst ask (highest price)
    uint32_t ask_count;         // active ask levels

    // O(1) price lookup
    khash_t(om_level_map) *price_to_slot;
} OmMarketLadder;
```

## Worker Memory Layout

**Each worker owns its own slab** - no sharing between workers.

### Private Worker (Compute-on-Publish)

The private worker uses a **product-level slab+Q1 ladder** (like the public worker)
for sorted price structure, plus a single `global_orders` map storing per-order state
(including `org`, `flags`, `vol_remain`). **No per-org state** — per-org dealable qty
is computed on demand from global state + the dealable callback:

```
per_org_qty = max(0, min(vol_remain, dealable(rec, viewer)) - (vol_remain - remaining))
```

Fan-out during ingest is kept only for delta/dirty tracking.

```
┌─────────────────────────────────────────────────────────────────┐
│ OmMarketWorker                                                  │
├─────────────────────────────────────────────────────────────────┤
│ product_slab: OmMarketLevelSlab                                 │
│   └─ slots[0..capacity-1]  (contiguous, cache-line aligned alloc)│
│                                                                 │
│ product_ladders[prod0]:                                         │
│   ├─ bid: head ──→ slot[5] ──→ slot[12] ──→ slot[3] ──→ NULL   │
│   │        (100)      (95)        (90)                          │
│   ├─ ask: head ──→ slot[8] ──→ slot[2] ──→ NULL                │
│   │        (101)      (105)                                     │
│   └─ price_to_slot: {100→5, 95→12, 90→3, 101→8, 105→2}         │
│                                                                 │
│ product_ladders[prod1]:                                         │
│   └─ ...                                                        │
│                                                                 │
│ global_orders: khash order_id → OmMarketOrderState              │
│   {order_id → (product_id, side, active, org, flags,            │
│                price, remaining, vol_remain)}                    │
│                                                                 │
│ product_order_sets[prod_id]: khash_set of order_ids             │
│   (per-product index for O(k) queries instead of O(K))          │
│                                                                 │
│ ladder_dirty[]: 64-byte aligned flags                           │
│ ladder_deltas[]: delta tracking hash maps                       │
└─────────────────────────────────────────────────────────────────┘
```

### Public Worker

```
┌─────────────────────────────────────────────────────────────────┐
│ OmMarketPublicWorker                                            │
├─────────────────────────────────────────────────────────────────┤
│ slab: OmMarketLevelSlab                                         │
│   └─ slots[0..capacity-1]  (contiguous, cache-line aligned alloc)│
│                                                                 │
│ ladders[prod0]:                                                 │
│   ├─ bid: head ──→ slot[1] ──→ slot[7] ──→ ...                 │
│   ├─ ask: head ──→ slot[4] ──→ slot[9] ──→ ...                 │
│   └─ price_to_slot: {...}                                       │
│                                                                 │
│ ladders[prod1]:                                                 │
│   └─ ...                                                        │
│                                                                 │
│ orders: khash order_id → OmMarketOrderState                     │
│ dirty[]: 64-byte aligned flags                                  │
│ deltas[]: delta tracking hash maps                              │
└─────────────────────────────────────────────────────────────────┘
```

## Operations

### Slab Operations

| Operation | Steps | Cost |
|-----------|-------|------|
| **Alloc** | Pop from Q0 head | O(1) |
| **Free** | Push to Q0 head | O(1) |
| **Grow** | Realloc slots, link new slots to Q0 tail | O(new_capacity) |

**Slab Growth**: When a slot allocation fails (slab full), the slab doubles in capacity
via `realloc`. Since we use uint32_t indices (not pointers), all existing indices remain
valid after growth. New slots are initialized and linked to the Q0 free list tail.
This is a rare event - initial sizing accounts for expected load - but ensures we never
lose WAL records due to capacity limits.

### Ladder Operations

| Operation | Steps | Cost |
|-----------|-------|------|
| **Add qty at price** | 1. Hash lookup price→slot<br>2. If found: `slot.qty += qty`<br>3. If not: alloc slot, find position in Q1, link | O(1) lookup, O(L) insert position |
| **Sub qty at price** | 1. Hash lookup price→slot<br>2. `slot.qty -= qty`<br>3. If qty==0: unlink from Q1, free to slab, remove from hash | O(1) |
| **Get qty at price** | Hash lookup price→slot, return qty | O(1) |
| **Publish top-N** | Walk Q1 from head, copy N entries | O(N), cache-friendly |

### Finding Insert Position (Q1)

For bids (descending by price): walk from head until `slot.price < new_price`.
For asks (ascending by price): walk from head until `slot.price > new_price`.

Optimization: If new price is likely near best (common case), start from head.
If new price is likely worse than current worst, start from tail.

## Record Flow

1. The matcher writes a WAL record to disk.
2. The record pointer is enqueued to the 1P-NC ring buffer.
3. Workers dequeue in batches and aggregate for their orgs/products.

## Per-Record Aggregation Behavior

### INSERT (OmWalInsert)

**Public (product-level)**

- For the public worker assigned to the product:
  - Hash lookup price in ladder
  - If found: add `vol_remain` to existing slot
  - If not found: alloc slot from slab, insert into Q1 at correct position

**Private (org-level)**

1. Update product ladder (same as public).
2. Store `org`, `flags`, `vol_remain` in `global_orders` alongside existing fields.
3. Fan-out to subscriber orgs: call dealable per viewer, compute qty via formula,
   record delta + mark dirty. **No per-org order state** is stored.

### CANCEL / DEACTIVATE (OmWalCancel / OmWalDeactivate)

**Public**

- Lookup `order_id` in the public order map.
- If present and active, subtract remaining quantity from the ladder.
- If qty reaches 0: unlink slot from Q1, free to slab, remove from hash.

**Private**

1. Lookup global order.
2. **Fan-out FIRST** (needs pre-cancel remaining): iterate product subscribers,
   compute `pre_qty` via formula, record delta = `-pre_qty`, mark dirty.
3. **THEN** update product ladder + mark global inactive.

### ACTIVATE (OmWalActivate)

**Public**

- Lookup in public order map; if inactive and remaining > 0, re-add remaining
  to ladder (alloc slot if price level was removed).

**Private**

1. Lookup global order (inactive, remaining > 0).
2. Mark active + update product ladder.
3. Fan-out: compute per-org qty via formula, record delta = `+qty`.

### MATCH (OmWalMatch)

**Public**

- Lookup maker order in public order map, subtract matched quantity from ladder.

**Private**

1. Lookup global order, compute `global_match`.
2. **Fan-out FIRST** (needs pre/post remaining): compute `pre_qty` with current
   remaining, `post_qty` with `remaining - global_match`, delta = `post_qty - pre_qty`.
3. **THEN** update product ladder + global remaining.

## Top-N Publishing

- **All price levels** are tracked in the ladder (Q1 linked list).
- Top-N filtering happens **at publish time** by walking Q1 from head.
- Since Q1 is sorted (best first), the first N slots are the top-N.

```c
// Publish top-N bids
uint32_t slot_idx = ladder->bid_head;
for (int i = 0; i < top_levels && slot_idx != OM_MARKET_SLOT_NULL; i++) {
    OmMarketLevelSlot *slot = &slab->slots[slot_idx];
    out[i].price = slot->price;
    out[i].qty = slot->qty;
    slot_idx = slot->q1_next;
}
```

## Publish Cadence

- Market snapshots are published at a fixed interval (e.g., 1s).
- If no WAL records affect a ladder, the **same snapshot** is still published.
- Dirty flags are used to skip re-serialization work when unchanged.

**Concrete publish loop (public):**

1. For each product assigned to the public worker, check `om_market_public_is_dirty()`.
2. If dirty, publish **delta** or **full** (see below) and then clear.
3. If not dirty, publish cached data (or skip) based on your cadence requirements.

**Concrete publish loop (private):**

1. For each subscribed `(org_id, product_id)` pair in the worker, check
   `om_market_worker_is_dirty()`.
2. If dirty, publish **delta** or **full** (see below) and then clear.
3. If not dirty, publish cached data (or skip) based on your cadence requirements.

## Delta vs Full Snapshot Publishing

- **Delta publishing:**
  - Only changed price levels are published each tick.
  - Backed by per-ladder delta maps (price -> delta).
  - Lowest bandwidth when changes are sparse.

- **Full snapshot publishing:**
  - Publishes top-N price levels every tick.
  - Walk Q1 from head, copy first N slots.
  - **Cache optimal**: sequential slot access if slots are allocated contiguously.

## Performance Characteristics

### Slab + Intrusive Queue Performance

| Operation | Cost | Notes |
|-----------|------|-------|
| Alloc/Free slot | O(1) | Q0 push/pop |
| Add qty (existing price) | O(1) | Hash lookup + increment |
| Add qty (new price) | O(1) + O(L) | Hash miss + Q1 insert position |
| Sub qty | O(1) | Hash lookup + decrement (+ free if zero) |
| Publish top-N | O(N) | Walk Q1, sequential memory access |
| Get qty at price | O(1) | Hash lookup |

Where L = number of price levels in the ladder.

### Comparison with Previous Designs

| Design | Insert | Remove | Publish | Memory |
|--------|--------|--------|---------|--------|
| Hash map only | O(1) | O(1) | O(P log P) sort | Unbounded |
| Dynamic sorted array | O(L) memmove | O(L) memmove | O(N) copy | Unbounded |
| **Slab + intrusive queue** | O(1) + O(L) link | O(1) | O(N) walk | **Bounded** |

Key advantages of slab + intrusive queue:
1. **Bounded memory**: Preallocated slab, no dynamic allocation during operation.
2. **No memmove**: Insert/remove just update a few indices.
3. **Cache-friendly publish**: Walk contiguous slots (if allocated sequentially).
4. **O(1) price lookup**: Hash map for direct access.

### Insert Position Optimization

Finding insert position is O(L) worst case, but can be optimized:
- **Best price heuristic**: Most inserts are near the best price; start from head.
- **Worst price check**: If new price is worse than tail, insert at tail (O(1)).
- **Skip list**: For very deep ladders (L > 100), add skip pointers.

## Slab Sizing

**Public worker** slab:
```
capacity = max_products * expected_levels_per_side * 2 (bid + ask) * safety_factor
```

**Private worker** product slab (same formula — no per-org slabs):
```
capacity = max_products * expected_levels_per_side * 2 (bid + ask) * safety_factor
```

Example:
- Private worker with 10,000 products
- Expected 50 price levels per side
- Safety factor 1.5

```
capacity = 10,000 * 50 * 2 * 1.5 = 1,500,000 slots
memory = 1,500,000 * 32 bytes = 48 MB per private worker
```

Per-org qty is computed on demand from `global_orders` + dealable callback (no per-org storage).

## False Sharing Prevention

- Each worker has its **own slab** (no sharing).
- Dirty flags are 64-byte aligned.
- No shared counters in the hot path.

## Optimization Suggestions

1. **Size slab appropriately**: Use `expected_price_levels * num_ladders * 2 * 1.5`.
2. **Make dealable fast**: simple bit checks, precomputed org/product rules, no I/O.
   The callback is invoked at both ingest and query time — keep it under ~5ns.
3. **Batch WAL processing**: process 256-1024 records per batch.
4. **Preallocate hash maps**: size `global_orders` and `price_to_slot` maps to avoid rehash.
5. **Right-size workers**: balance load across workers to avoid hotspots.
6. **Prefer full snapshot over delta** when most levels change each tick.
7. **Insert position hint**: track whether new orders tend to be at best or spread out.
8. **Per-product order sets**: `get_qty()` and `copy_full()` iterate only orders
   for the queried product via `product_order_sets`, giving O(k) cost where
   k = active orders for that product (typically ~100x smaller than total orders).

## Capacity Planning

### Reference Scenario

| Parameter | Value |
|-----------|-------|
| Orgs (O) | 5,000 |
| Products per org (P) | 10,000 |
| Total subscriptions (S = O × P) | 50,000,000 |
| Price levels per side (L) | 20 |
| WAL throughput (R) | 1,000,000/s |
| Publish window | 1.0s |
| Aggregation budget (T) | 0.5s (remaining 0.5s for serialization) |

### Per-Record Cost Model

**Public worker** — no fan-out, 1 product operation per WAL record:

| Operation | Cost |
|-----------|------|
| `product_has_subs` check | ~1ns |
| `kh_put(order_map)` | ~35ns |
| `kh_get(level_map, price)` (hit) | ~25ns |
| `slot.qty += qty` | ~5ns |
| Delta map update | ~25ns |
| **Total per record** | **~90ns** |

**Private worker** — fans out to O/W orgs per record (W = worker count):

Phase 1 (product-level, once per record):

| Operation | Cost |
|-----------|------|
| `om_ladder_add_qty(product_slab)` | ~30ns |
| `kh_put(global_orders)` | ~35ns |
| **Total per record** | **~65ns** |

Phase 2 (per-org fan-out, delta/dirty tracking only):

| Operation | INSERT | MATCH | CANCEL |
|-----------|--------|-------|--------|
| `find_ladder()` (array indexed) | ~10ns | ~10ns | ~10ns |
| `dealable()` callback | ~5ns (direct, no fake) | ~5ns (1 call, reused) | ~5ns |
| `_om_market_qty_from_dq()` | — | ~2ns (×2 pre/post) | — |
| Fake `OmWalInsert` construct | — | ~5ns (1×, was 2×) | ~5ns |
| Delta map update | ~5ns | ~5ns | ~5ns |
| Dirty flag | ~1ns | ~1ns | ~1ns |
| **Total per org** | **~15ns** | **~15ns** | **~25ns** |

INSERT skips fake `OmWalInsert` construction (uses original `rec` directly).
MATCH uses a single `dealable()` call + `_om_market_qty_from_dq()` for pre/post qty
(was 2× `dealable()` + 2× fake construct).

With L2/L3 cache pressure from large working sets: **15-30ns per org realistic**.
No per-org order maps or qty maps — dealable qty is computed on demand at query time.

### Public Worker Sizing

```
records_per_window = R × T = 1,000,000 × 0.5 = 500,000
time_needed = 500,000 × 90ns = 45ms
```

**1-2 public workers** is sufficient.

Memory per public worker:
```
slab = P × L × 2 sides × 32 bytes = 10,000 × 20 × 2 × 32 = 12.8 MB
hash + orders overhead ≈ 2× slab ≈ 26 MB
total ≈ 40 MB per public worker
```

### Private Worker Sizing (Compute-on-Publish)

Each private worker reads ALL WAL records, updates a product-level ladder (once),
then fans out delta/dirty tracking to subscriber orgs (~15-25ns/org depending on
record type). Per-org qty is computed on demand at query/publish time from global
state + dealable callback.

```
records_in_window = R × T = 1,000,000 × 0.5 = 500,000
time_budget_per_record = T / (R × T) = 1/R = 1μs = 1000ns
```

Per record, each worker fans out to O/W orgs. Blended cost depends on WAL record
mix (typical: ~60% INSERT, ~30% MATCH, ~10% CANCEL):
```
blended_cost = 0.6 × 15ns + 0.3 × 15ns + 0.1 × 25ns = 16ns/org

constraint: 65ns + (O / W) × cost_per_org ≤ 1000ns

Optimistic  (15ns/org):  W ≥ 5,000 × 15 / 935 =  80
Blended     (16ns/org):  W ≥ 5,000 × 16 / 935 =  86
Realistic   (20ns/org):  W ≥ 5,000 × 20 / 935 = 107
Pessimistic (30ns/org):  W ≥ 5,000 × 30 / 935 = 160
```

Example: 10 workers, 500 orgs each:
```
per record:  65ns + 500 × 16ns = 8μs
per window:  500,000 × 8μs = 4.0s  (8× over budget → need ~86 workers)
```

Memory (constant regardless of W, total across all workers):
```
product_slab: P × L × 2 × 32 bytes = 10K × 20 × 2 × 32 = ~13 MB per worker
global_orders: avg_active_orders × ~56 bytes               ≈  few hundred MB
────────────────────────────────────────────────────────────────
total private                                             ≈   4 GB
```

Query cost at publish time: `get_qty()` and `copy_full()` iterate per-product order
sets and call `dealable()` per matching order, so are O(k) per call where k = active
orders for the queried product (not total orders across all products).

### Scaling Improvement vs Old Design

The old design used per-org slabs + Q1 ladders at ~150ns/org, requiring ~750
workers and ~250 GB RAM. The current design eliminates ALL per-org state:
- **Workers**: ~86-107 (down from ~750, ~7-9x reduction)
- **Memory**: ~4 GB (down from ~250 GB, ~60x reduction)
- **Per-org cost**: ~15-30ns blended (down from ~150-200ns, ~7-10x reduction)

### Comparison

The current implementation uses compute-on-publish: product-level slab+Q1 ladder
for sorted price structure, with per-org qty computed on demand from `global_orders`
+ dealable callback. This avoids all per-org state at ingest time.

| | Old (Per-org Slabs) | Hybrid (Product Slab + OrgPriceQty) | **Current (Compute-on-Publish)** |
|---|---|---|---|
| WAL workers | ~750 | ~270 | ~86-107 |
| Publish workers | 0 (pre-computed) | 0 (pre-computed) | 0 (computed on query) |
| **Total workers** | **~750** | **~270** | **~86-107** |
| **Memory** | **~250 GB** | **~10 GB** | **~4 GB** |
| Per-org ingest cost | ~150ns | ~40ns | ~15-25ns (delta/dirty only) |
| Private read | O(1) hash | O(1) hash | O(k) per-product compute |
| Publish | O(N) Q1 walk | O(N) Q1 walk + filter | O(k) build + O(N) walk |
| Per-org state | slab+Q1+orders | qty maps + orders | **none** |

### Quick Sizing Formula

For current design (compute-on-publish, blended ~16ns/org):
```
W_private ≥ (65ns + O/W × 16ns) × R / 1e9

Example: 50 orgs, 1M/s WAL, 0.5s budget
  per record: 65 + 50 × 16 = 865ns
  W ≥ 865ns × 1M / 0.5s ≈ 2 workers

Example: 5000 orgs, 1M/s WAL, 0.5s budget
  W ≥ 5000 × 16 / (1000 - 65) ≈ 86 workers
```

Query-time cost (`get_qty`, `copy_full`) is O(k) per call where k = active orders
for the queried product, using per-product order sets for efficient iteration.

## Two-Phase Coordination (Private Workers)

Private workers process WAL records in two phases within a single thread:

**Phase 1 — Ingest** (once per WAL record):
1. Update product-level ladder (`product_slab` + `product_ladders[product_id]`)
2. Record order state in `global_orders` (order_id -> product/side/price/remaining/org/flags/vol_remain)

**Phase 2 — Fan-out** (per subscriber org, delta/dirty tracking only):
1. Compute per-org qty via `om_market_compute_org_qty()` using the formula:
   `per_org_qty = max(0, min(vol_remain, dealable(rec, viewer)) - (vol_remain - remaining))`
2. Record delta, mark dirty
3. **No per-org state is stored** — qty is computed from global state

**Critical ordering for CANCEL/MATCH**: fan-out must happen BEFORE updating global
state, because the formula needs the pre-operation `remaining` value.

**At publish time** — query computes per-org qty on demand:
- `get_qty()`: iterates per-product order set, sums computed per-org qty for matching orders — O(k)
- `copy_full()`: builds temp price->qty map from per-product order set, then walks Q1 ladder — O(k) + O(N)

Within a single worker, the two phases are implicit (sequential in the same thread).
Cross-worker coordination is not needed because each worker owns its own data
structures (no sharing).

## Summary

The slab + intrusive queue design mirrors VeloxMatch's architecture:
- Fixed-size slots in a contiguous slab (cache-line aligned)
- Intrusive queues with uint32_t indices (not pointers)
- Q0 for free list, Q1 for sorted price ladder
- Hash map for O(1) price → slot lookup
- Each worker owns its slab (no cross-worker sharing)
- Bounded memory, no memmove, O(N) publish
- Slab growth via realloc when capacity exceeded (indices survive)

The current design uses compute-on-publish for private ladders: per-org dealable
qty is computed on demand from global order state + dealable callback, eliminating
all per-org storage. Fan-out during ingest is kept only for delta/dirty tracking
(~25ns/org vs ~40ns/org with per-org hash maps). Trade-off: query-time cost is
O(active_orders) per call instead of O(1) hash lookup.
