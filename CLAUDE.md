# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Standard build (sanitizers enabled by default)
mkdir -p build && cd build && cmake .. && make -j$(nproc)

# Release without sanitizers
cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_ASAN=OFF -DENABLE_UBSAN=OFF ..

# Debug build
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_FLAGS="-g -O0" ..

# Use khashl instead of khash
cmake -DUSE_KHASHL=ON ..

# WAL mock (prints to stderr, no file I/O)
cmake -DOM_USE_WAL_MOCK=ON ..
```

## Testing

All test commands run from `build/` directory:

```bash
ctest --output-on-failure          # Run all tests
ctest -R <test_name> --output-on-failure  # Single test suite
./tests/test_runner                # Direct execution
ctest -N                           # List available tests
```

If ASan complains about preload order:
```bash
LD_PRELOAD=/usr/lib/libasan.so make test
```

## Lint/Format

```bash
clang-tidy src/*.c -- -Iinclude
cd build && cmake --build . --target format        # Apply formatting
cd build && cmake --build . --target format-check  # Check formatting
cppcheck --enable=all --suppress=missingIncludeSystem src/
```

## Architecture

Two artifacts built as shared (.so) and static (.a) libraries:

### VeloxMatch (Matching Engine Core)
- **om_slab**: Dual slab allocator - hot fields (64B slots) vs cold aux data, cache-optimized
- **om_hash**: Pluggable hashmap (khash or khashl backend) for O(1) order lookup
- **orderbook**: Per-product order books with 4 intrusive queues per slot:
  - Q0: internal free list
  - Q1: price ladder (sorted)
  - Q2: time FIFO at price level
  - Q3: org queue (for batch cancel by org)
- **om_wal**: Write-ahead log with CRC32, replay, multi-file rotation, custom records
- **om_engine**: Callback-driven matching (can_match, on_match, on_deal, on_booked, on_filled, on_cancel, pre_booked)
- **om_perf**: Performance presets (HFT ~2-6M/sec, DURABLE ~0.2-0.8M/sec)

### VeloxMarket (Market Data Aggregation)
- **om_worker**: Lock-free 1P-NC ring buffer for WAL record distribution
- **om_market**: Aggregates WAL into public (product-sharded) and private (org-sharded) ladders
  - Public ladder: total qty at price level
  - Private ladder: dealable qty per org (via `dealable()` callback)
  - Delta or full snapshot publishing modes
  - Top-N enforcement (only top N price levels aggregated)

## Code Style

- C11 with POSIX extensions
- Cross compile for MacOS 10.12+ too
- 4-space indent, 100-char lines, K&R braces
- Types: `Om<Component><Type>` (e.g., `OmSlabSlot`, `OmMarketWorker`)
- Functions: `om_<module>_<verb>()` (e.g., `om_slab_alloc()`)
- Private/internal: prefix with `_` (e.g., `_om_market_pair_key()`)
- Macros: `OM_<COMPONENT>_<NAME>` (e.g., `OM_SIDE_BID`)
- Error codes: 0 = success, negative = error
- Use `<stdint.h>` types, `bool` from `<stdbool.h>`, `const` for read-only params
- Include order: system → library → local headers

## Dependencies

- **klib** (git submodule): khash.h, khashl.h for hash tables
- **check** (git submodule): unit testing framework

Initialize submodules: `git submodule update --init --recursive`
