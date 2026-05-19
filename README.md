# VeloxMatch

Three C11 libraries for building low-latency trading systems. Small,
embeddable, and designed for HFT-style workloads on a single thread per core.

## Three Artifacts

### VeloxMatch — Matching Engine (`libveloxmatch`)

Callback-driven matching core with a cache-friendly dual slab allocator,
intrusive-queue orderbook, and durable write-ahead log.

- **Dual slab allocator** — hot fields (64 B slots) + separate aux (cold) data
- **Orderbook** — price ladder (Q1), time FIFO (Q2), org queue (Q3) per product
- **O(1) cancel** via order-ID hashmap (khash or khashl backend)
- **WAL** — append-only log with CRC32, multi-file rotation, and full replay/recovery
- **Engine callbacks** — `can_match`, `on_deal`, `on_booked`, `on_filled`, `on_cancel`, `pre_booked`
- **Perf presets** — HFT (~2-6 M/sec), durable (~0.2-0.8 M/sec), and more

### VeloxMarket — Market Data Aggregation (`libveloxmarket`)

Consumes WAL records and builds publishable price ladders. Two worker types:

- **Public workers** (product-sharded) — total remaining qty at each price level (~90 ns/record)
- **Private workers** (org-sharded) — per-org dealable qty via `dealable()` callback, compute-on-publish with no per-org state (~15-25 ns/org fan-out)
- Delta or full-snapshot publishing, top-N enforcement, dirty tracking

### VmBus — WAL Distribution Bus (`libvmbus`)

Distributes WAL records across process and machine boundaries via two
transports: shared memory (same host) and TCP (cross-machine).

**SHM transport** (same host, ~50-80ns publish, ~30-50ns poll):

- **Producer** (`OmBusStream`) — creates SHM ring, publishes records inline (not pointers)
- **Consumer** (`OmBusEndpoint`) — attaches to SHM, non-blocking poll with zero-copy option
- Batch publish/poll, CRC32C validation (HW-accelerated on x86/ARM), gap detection
- Phased backpressure (spin → yield → callback), stale consumer detection
- Producer restart detection (epoch), cursor persistence for resume after restart

**TCP transport** (cross-machine, ~10-50us):

- **Server** (`OmBusTcpServer`) — binds TCP port, broadcasts frames to all clients
- **Client** (`OmBusTcpClient`) — connects, polls frames into `OmBusRecord`
- **Auto-reconnect** (`OmBusTcpAutoClient`) — transparent reconnection with exponential backoff
- Slow client warning before disconnect, reorder detection, server-side stats
- OS-managed TCP keep-alive on all sockets

**Glue headers** (header-only, no link dependencies):

- `om_bus_attach_wal(wal, stream)` — wires WAL post-write callback to bus publish
- `om_bus_poll_worker(ep, w)` / `om_bus_tcp_poll_worker(client, w)` — feed into market workers
- `om_bus_relay_run()` — reference SHM → TCP relay loop
- `om_bus_replay_gap()` — WAL replay for gap recovery

## Project Layout

```
.
├── include/
│   ├── openmatch/            # Matching engine headers
│   │   ├── om_slab.h         # Dual slab allocator + slot layout
│   │   ├── orderbook.h       # Orderbook API
│   │   ├── om_hash.h         # Hashmap (khash/khashl)
│   │   ├── om_wal.h          # WAL API + replay + post_write hook
│   │   ├── om_perf.h         # Performance presets
│   │   ├── om_engine.h       # Matching engine API
│   │   └── om_wal_mock.h     # WAL mock (prints to stderr)
│   ├── openmarket/           # Market data headers
│   │   ├── om_market.h       # Public/private ladder aggregation
│   │   └── om_worker.h       # Lock-free ring for WAL distribution
│   └── ombus/                # WAL distribution bus headers
│       ├── om_bus.h           # SHM stream (producer) + endpoint (consumer)
│       ├── om_bus_tcp.h       # TCP server + client + auto-reconnect
│       ├── om_bus_error.h     # Bus error codes (-800 to -823)
│       ├── om_bus_wal.h       # Header-only: WAL → bus glue
│       ├── om_bus_market.h    # Header-only: SHM bus → market worker
│       ├── om_bus_tcp_market.h # Header-only: TCP bus → market worker
│       ├── om_bus_relay.h     # Header-only: SHM → TCP relay loop
│       └── om_bus_replay.h    # Header-only: WAL replay gap recovery
├── src/                      # Implementations
│   ├── om_engine.c           # Matching engine
│   ├── om_market.c           # Market data aggregation
│   ├── om_bus_shm.c          # SHM bus transport
│   └── om_bus_tcp.c          # TCP bus transport (server + client)
├── tests/                    # check-based unit tests
├── tools/                    # Utility binaries + awk helpers
│   ├── wal_reader.c           # WAL dump with filters (-s/-r), CRC (-c), SHM replay (-p)
│   ├── wal_maker.c            # Generate random WAL files for testing (-e for corruption)
│   ├── wal_trace_oid.awk
│   ├── wal_match_by_maker.awk
│   └── wal_sum_qty_by_maker.awk
└── deps/                     # submodules (klib, check)
```

## Build

```bash
git clone --recursive <repo>
cmake -S . -B build
cmake --build build -j$(nproc)
```

### Release Build (recommended for perf runs)

Use a separate `build_release/` directory so debug/sanitizer and release
artifacts do not mix:

```bash
cmake -S . -B build_release -DCMAKE_BUILD_TYPE=Release -DENABLE_ASAN=OFF -DENABLE_UBSAN=OFF
cmake --build build_release -j$(nproc)
```

### Sanitizers (default on)

```bash
cmake -S . -B build -DENABLE_ASAN=ON -DENABLE_UBSAN=ON
cmake --build build -j$(nproc)
```

## Tests

All tests run from a chosen build directory (for example `build/` or
`build_release/`).

```bash
ctest --test-dir build --output-on-failure
ctest --test-dir build_release --output-on-failure
```

If ASan complains about preload order, run:

```bash
ASAN_OPTIONS=verify_asan_link_order=0 ctest --test-dir build --output-on-failure
```

## Core Concepts

### VeloxMatch

#### Slab Allocator (`om_slab`)

Each order lives in a fixed-size slab slot (`OmSlabSlot`):

- **Hot fields** (price, volume, flags, org, order_id)
- **4 intrusive queue nodes** per slot
- **Flexible user data** for secondary hot payload
- **Aux slab** for cold data (separate allocation)

Queues:

- **Q0** internal free list
- **Q1** price ladder
- **Q2** time FIFO at price
- **Q3** org queue

#### Orderbook (`orderbook`)

Per product:

- **Q1** is a sorted price ladder (best price at head)
- **Q2** is FIFO list for orders at each price level
- **Q3** is org queue per product (for batch cancel)
- **Hashmap** maps `order_id → (slot_idx, product_id)` for O(1) cancel

API highlights:

- `om_orderbook_init()` / `om_orderbook_destroy()`
- `om_orderbook_insert()`
- `om_orderbook_cancel()`
- `om_orderbook_cancel_org_product()` / `om_orderbook_cancel_org_all()`
- `om_orderbook_get_best_bid()` / `om_orderbook_get_best_ask()`

#### WAL (`om_wal`)

Append-only log with replay support. Record types:

- `OM_WAL_INSERT` (variable length: fixed fields + user + aux data)
- `OM_WAL_CANCEL`
- `OM_WAL_MATCH`
- `OM_WAL_DEACTIVATE` / `OM_WAL_ACTIVATE`

Post-write hook: a generic `post_write(seq, type, data, len, ctx)` callback
fires after every WAL write, allowing downstream systems (e.g. VmBus) to
observe records without any link dependency from libveloxmatch.

Replay API:

- `om_wal_replay_init_with_config()`
- `om_wal_replay_next()` returns `-2` on CRC mismatch
- `om_orderbook_recover_from_wal()` reconstructs slab + orderbook

Custom records:

- use `om_wal_append_custom()` with types `>= OM_WAL_USER_BASE`
- register a replay handler with `om_wal_replay_set_user_handler()`

Custom record output:

- `wal_reader` prints `user[len]` for custom records
- `wal_mock` prints `type[USER] ut[<type>] len[<len>]`

Multi-file WAL:

- Set `OmWalConfig.filename_pattern` (e.g. `/tmp/openmatch_%06u.wal`) with `file_index` start
- Set `wal_max_file_size` to roll to the next file on flush
- Replay will scan sequential files in increasing index until a file is missing

#### Performance Presets (`om_perf`)

Presets include `OM_PERF_DEFAULT`, `OM_PERF_HFT`, `OM_PERF_DURABLE`,
`OM_PERF_RECOVERY`, `OM_PERF_MINIMAL`. Use `om_perf_validate()` and
`om_perf_autotune()` to verify/tune.

**Approximate throughput (single-thread, light callbacks, in-memory):**

1. **OM_PERF_HFT**: ~2-6M matches/sec/core
2. **OM_PERF_RECOVERY**: ~1.5-4M matches/sec/core
3. **OM_PERF_DEFAULT**: ~1-3M matches/sec/core
4. **OM_PERF_MINIMAL**: ~0.5-1.5M matches/sec/core
5. **OM_PERF_DURABLE**: ~0.2-0.8M matches/sec/core

Engine can apply a preset via `OmEngineConfig.perf` or `om_engine_init_perf()`.

#### Engine (`om_engine`)

Callback-driven matching core. Supports:

- `can_match` (per maker/taker; returns max match volume)
- `on_match` (per order, post-deduction)
- `on_deal` (per trade)
- `on_booked` / `on_filled` / `on_cancel`
- `pre_booked` (decide whether remainder rests)

Order deactivation/activation:

- `om_engine_deactivate(order_id)` (remove from book, keep slot)
- `om_engine_activate(order_id)` (reattempt match as taker)

### VeloxMarket

Aggregates WAL records into publishable market data ladders. Two worker types:

- **Public workers** (product-sharded): total remaining qty at each price level.
  Simple hash-lookup aggregation, ~90 ns per WAL record.
- **Private workers** (org-sharded): per-org dealable qty via a `dealable()` callback.
  Compute-on-publish design -- no per-org state stored, qty derived on demand from
  global order state. Fan-out cost ~15-25 ns/org depending on record type.

Key data structures per private worker:

- `product_slab` + `product_ladders[]`: sorted price levels (32-byte slots, Q1 queue)
- `global_orders`: order_id -> state (product, side, price, remaining, org, flags)
- `product_order_sets[]`: per-product order_id sets for O(k) queries
- Delta maps + dirty flags for incremental publish

Publishing modes: **delta** (only changed levels) or **full snapshot** (top-N walk).

See [docs/market_data.md](docs/market_data.md) for detailed aggregation flow,
capacity planning, and per-record cost model.

### VmBus

Two transports for WAL record distribution: SHM for same-host workers,
TCP for remote hosts. No serialization -- records are copied inline.

```
Engine -> WAL -> post_write() -> OmBusStream (SHM ring)
                                       |
                        +--------------+--------------+
                        |                             |
                 OmBusEndpoint             TCP Relay (om_bus_relay)
                        |                       |
                 OmMarketWorker          OmBusTcpServer
                  (local)                  |    |    |
                                    OmBusTcpClient (remote hosts)
                                           |
                                    OmMarketWorker
```

**SHM** (`OmBusStream` / `OmBusEndpoint`): ~50-80ns publish, ~30-50ns poll.
Inline payload, zero-copy option, CRC32C, batch publish/poll, backpressure.

**TCP** (`OmBusTcpServer` / `OmBusTcpClient`): ~10-50us. Non-blocking poll-based
I/O, auto-reconnect wrapper, slow client warning, server stats, keep-alive.

See [docs/msg_bus.md](docs/msg_bus.md) for wire protocol, memory layout, and
full API reference.

## Example (Minimal)

```c
#include "openmatch/orderbook.h"
#include "openmatch/om_wal.h"

OmSlabConfig slab_cfg = { .user_data_size = 64, .aux_data_size = 128, .total_slots = 100000 };
OmWalConfig wal_cfg = {
    .filename = "/tmp/openmatch.wal",
    .buffer_size = 1024 * 1024,
    .sync_interval_ms = 10,
    .use_direct_io = true,
    .enable_crc32 = true,
    .user_data_size = slab_cfg.user_data_size,
    .aux_data_size = slab_cfg.aux_data_size
};

OmWal wal;
om_wal_init(&wal, &wal_cfg);

OmOrderbookContext ctx;
om_orderbook_init(&ctx, &slab_cfg, &wal, 1024, 1024, 0);

OmSlabSlot *slot = om_slab_alloc(&ctx.slab);
om_slot_set_order_id(slot, om_slab_next_order_id(&ctx.slab));
om_slot_set_price(slot, 10000);
om_slot_set_volume(slot, 100);
om_slot_set_volume_remain(slot, 100);
om_slot_set_flags(slot, OM_SIDE_BID | OM_TYPE_LIMIT);
om_slot_set_org(slot, 1);

om_orderbook_insert(&ctx, 0, slot);

om_wal_flush(&wal);
om_wal_fsync(&wal);
om_orderbook_destroy(&ctx);
om_wal_close(&wal);
```

## Roadmap / Missing Parts

1. **Snapshotting** (state dump + WAL checkpointing).
2. **Async I/O** in WAL (flag exists, not implemented).
3. **More validation** (parameter checks, error codes, and recovery guarantees):
   - bounds checks for product/org ids
   - enforce volume/price invariants (non‑zero, monotonic rules)
   - sanity checks on callbacks (e.g., can_match > 0 implies matchable)
   - WAL replay strictness options (drop/stop on corrupt record)

## Tools

### wal_reader

Reads WAL files, prints records, and optionally replays them to an SHM bus.
Accepts multiple files (shell glob works: `/tmp/wal_*.log`).

```
wal_reader [options] <wal_file> [wal_file ...]

options:
  -t                Format timestamps as human-readable
  -c                Strict CRC: stop on first corruption
                    (without -c, CRC errors are warned but skipped)
  -s from-to        Sequence range filter (inclusive, repeatable)
  -r from-to        Time range filter (inclusive, repeatable)
                    Format: YYYYMMDDHHMMSS-YYYYMMDDHHMMSS
  -p stream_name    Replay matching records to SHM bus stream
```

Output format uses short bracketed fields (easy to parse with AWK):

```
seq[12] type[MATCH] len[40] m[100] t[200] p[10000] q[5] pid[0] ts[1700000000]
```

#### Filtering

Multiple `-s` and `-r` filters form OR logic — a record is included if it
falls within any specified range. Ranges are inclusive.

```bash
# Sequences 1-100 and 500-600
./build/tools/wal_reader -s 1-100 -s 500-600 /tmp/openmatch.wal

# Records between 12:00 and 13:00 on Jan 15 2025
./build/tools/wal_reader -t -r 20250115120000-20250115130000 /tmp/openmatch.wal

# Combine: sequences 1-50 OR anything in the time window
./build/tools/wal_reader -s 1-50 -r 20250115120000-20250115130000 /tmp/openmatch.wal
```

#### Replay to SHM bus

With `-p`, matching records are published to a named SHM bus stream for
downstream consumers (market workers, relay, etc.). Output goes to stderr.

```bash
# Replay all records to /om-replay stream
./build/tools/wal_reader -p /om-replay /tmp/openmatch.wal

# Selective replay: only sequences 100-200
./build/tools/wal_reader -s 100-200 -p /om-replay /tmp/openmatch.wal
```

#### AWK examples

Trace a single order id across all records:

```
./build/tools/wal_reader /tmp/openmatch.wal \
  | awk -F'[][]' '{for (i=2;i<=NF;i+=2) if ($i=="oid" && $(i+1)==42) print $0}'
```

Extract all matches for a maker id (m[]):

```
./build/tools/wal_reader /tmp/openmatch.wal \
  | awk -F'[][]' '{for (i=2;i<=NF;i+=2) if ($i=="m" && $(i+1)==42) print $0}'
```

Compute total traded quantity per maker id:

```
./build/tools/wal_reader /tmp/openmatch.wal \
  | awk -F'[][]' '
    {m=""; q=""; for (i=2;i<=NF;i+=2) {if ($i=="m") m=$(i+1); if ($i=="q") q=$(i+1)}
     if (m!="" && q!="") sum[m]+=q}
    END {for (id in sum) print id, sum[id]}
  '
```

List all activates/deactivates:

```
./build/tools/wal_reader /tmp/openmatch.wal \
  | awk -F'[][]' '{for (i=2;i<=NF;i+=2) if ($i=="type" && ($(i+1)=="DEACTIVATE" || $(i+1)=="ACTIVATE")) print $0}'
```

#### CRC validation

CRC32 is always validated. Corrupted records are warned on stderr and skipped.
With `-c` (strict mode), the reader stops at the first CRC error:

```bash
# Default: warn and skip corrupted records, continue reading
$ wal_reader /tmp/broken.wal
# stderr: CRC MISMATCH warnings for each bad record
# stdout: all good records (bad ones skipped)

# Strict: stop at first corruption
$ wal_reader -c /tmp/broken.wal
```

CRC mismatch output includes repair instructions:

```
CRC MISMATCH in /tmp/broken.wal at seq 33
  file offset:  1784 (0x6f8)
  stored CRC:   0x5ce0680f (bad)
  computed CRC: 0x24b1cac5 (good)
  record type:  INSERT  len: 56

To repair, patch 4 bytes at offset 1784 + 8 + 56 = 1848 (0x738) with the good CRC value.
```

### wal_maker

Generates WAL files with random records for testing.

```
wal_maker [options] <output_wal>

options:
  -n count      Number of records (default 100)
  -e count      Number of records to corrupt (default 0)
  -C            Disable CRC32 (CRC is on by default)
  -p products   Number of product IDs (default 4)
  -S seed       RNG seed (default: from clock)
```

Record mix: ~50% INSERT, ~15% CANCEL, ~15% MATCH, ~10% DEACTIVATE, ~10% ACTIVATE.

```bash
# Generate a clean 1000-record WAL (CRC on by default)
./build/tools/wal_maker -n 1000 /tmp/test.wal

# Generate a WAL with 3 corrupted records (for testing CRC validation)
./build/tools/wal_maker -n 500 -e 3 /tmp/broken.wal

# Verify the broken file (strict mode: stop on first error)
./build/tools/wal_reader -c /tmp/broken.wal
```

### wal_query (SQLite extension)

Builds a loadable SQLite extension that exposes WAL records as a virtual table.
CRC32 is validated by default — corrupted records are included with `crc_ok=0`.

```
cmake -S . -B build
cmake --build build
```

Load in sqlite3:

```sql
.load ./build/tools/wal_query
CREATE VIRTUAL TABLE walv USING wal_query(
  file=/tmp/openmatch.wal,
  user_data=64,
  aux_data=128
);
SELECT seq, type_name, order_id, price, volume, product_id, timestamp_ns FROM walv;
```

Virtual table options:

| Option | Description |
|--------|-------------|
| `file=PATH` | WAL file path (required unless `pattern` is used) |
| `pattern=FMT` | Multi-file pattern, e.g. `/tmp/wal_%06u.wal` |
| `index=N` | Starting file index for multi-file (default 0) |
| `user_data=N` | User data size in bytes (default 0) |
| `aux_data=N` | Aux data size in bytes (default 0) |
| `crc32=0` | Disable CRC validation (for legacy CRC-free WALs) |

Virtual table columns:

```
seq INTEGER          -- WAL sequence number
type INTEGER         -- Record type enum
type_name TEXT       -- INSERT, CANCEL, MATCH, DEACTIVATE, ACTIVATE, USER
data_len INTEGER     -- Payload length in bytes
order_id INTEGER     -- Order ID (INSERT, CANCEL, DEACTIVATE, ACTIVATE)
price INTEGER        -- Price (INSERT, MATCH)
volume INTEGER       -- Volume (INSERT)
vol_remain INTEGER   -- Volume remaining (INSERT)
org INTEGER          -- Organization ID (INSERT)
flags INTEGER        -- Flags (INSERT)
product_id INTEGER   -- Product ID (all types)
timestamp_ns INTEGER -- Timestamp in nanoseconds (all types)
slot_idx INTEGER     -- Slab slot index (CANCEL, DEACTIVATE, ACTIVATE)
maker_id INTEGER     -- Maker order ID (MATCH)
taker_id INTEGER     -- Taker order ID (MATCH)
match_price INTEGER  -- Deal price (MATCH)
match_volume INTEGER -- Deal volume (MATCH)
user_type INTEGER    -- Custom record type (USER)
stored_crc INTEGER   -- CRC32 value read from disk
computed_crc INTEGER -- CRC32 value computed over header+payload
crc_ok INTEGER       -- 1 if CRC matches, 0 if corrupted
file_offset INTEGER  -- Byte offset of record header in WAL file
```

#### CRC integrity queries

```sql
-- Find all corrupted records
SELECT seq, type_name, printf('0x%08x', stored_crc) AS bad_crc,
       printf('0x%08x', computed_crc) AS good_crc, file_offset
FROM walv WHERE crc_ok = 0;

-- Count good vs bad records
SELECT crc_ok, count(*) FROM walv GROUP BY crc_ok;

-- Analyze only valid records
SELECT type_name, count(*), sum(volume) FROM walv
WHERE crc_ok = 1 GROUP BY type_name;
```

#### Multi-file WAL

```sql
CREATE VIRTUAL TABLE walv USING wal_query(
  pattern=/tmp/openmatch_%06u.wal,
  index=0,
  user_data=64,
  aux_data=128
);
```

#### Optional indexes

```
.read ./tools/wal_query.sql
```

This materializes the virtual table into `wal` and creates indexes there.

### wal_mock (compile-time)

Build the library with WAL mock (prints to stderr, no file I/O):

```
cmake -S . -B build -DOM_USE_WAL_MOCK=ON
cmake --build build
```

Mock output uses the same bracketed format as `wal_reader`, for example:

```
seq[12] type[MATCH] m[100] t[200] p[10000] q[5] pid[0] ts[1700000000]
```
