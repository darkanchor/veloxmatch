# AGENTS.md - C11 CMake Project Guidelines

## Project Overview
- **Language**: C11
- **Build System**: CMake (3.15+)
- **Outputs**: Shared library (.so/.dll/.dylib) and Static library (.a/.lib)
- **Standard**: ISO C11 with POSIX extensions where needed
- **Dependencies**: klib (git submodule)

## Git Submodules

This project uses **klib** as a git submodule for data structures and utilities.

### Cloning with Submodules
```bash
# Clone with all submodules
git clone --recursive https://github.com/darkanchor/veloxmatch.git

# Or if already cloned without --recursive
git submodule update --init --recursive
```

### Updating Submodules
```bash
# Update klib to latest version
cd deps/klib && git pull origin master && cd ../..
git add deps/klib
git commit -m "Update klib to latest"
```

### Check Testing Framework

The project uses **check** as a git submodule for unit testing.

```bash
# The check submodule is automatically initialized with:
git submodule update --init --recursive

# Or manually:
git submodule add https://github.com/libcheck/check.git deps/check
```

## Build Commands

### Full Build
```bash
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

### Clean Build with Tests
```bash
rm -rf build && mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
make test  # Run tests from build directory
```

### Debug Build
```bash
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_FLAGS="-g -O0" ..
make -j$(nproc)
```

### Sanitizer Builds (enabled by default)

By default, Address Sanitizer (ASan) and Undefined Behavior Sanitizer (UBSan) are enabled. Use the options below to disable them or add Memory Sanitizer.

#### Default Build (with sanitizers)
```bash
cd build
cmake ..
make -j$(nproc)
make test
```

#### Address Sanitizer (detects memory errors)
```bash
cd build
cmake -DENABLE_ASAN=OFF ..
make -j$(nproc)
make test
```

#### Memory Sanitizer (detects uninitialized reads) - Requires Clang
```bash
cd build
CC=clang cmake -DENABLE_MSAN=ON ..
make -j$(nproc)
make test
```

#### Undefined Behavior Sanitizer
```bash
cd build
cmake -DENABLE_UBSAN=OFF ..
make -j$(nproc)
make test
```

#### Combined Sanitizers (ASan + UBSan)
```bash
cd build
cmake -DENABLE_ASAN=OFF -DENABLE_UBSAN=OFF ..
make -j$(nproc)
make test
```

## Test Commands

**IMPORTANT: All test commands must be run from the `build/` directory.**

The tests use the **check** testing framework.

### Run All Tests
```bash
cd build && make test
# or
cd build && ctest --output-on-failure
```

### Run Single Test Suite
```bash
cd build && ctest -R <test_name> --output-on-failure
```

### Run Test Executable Directly
```bash
cd build && ./tests/test_runner
```

### List Available Tests
```bash
cd build && ctest -N
```

### Run with Valgrind (if available)
```bash
cd build && ctest -T memcheck
```

### Test Files Location
- Tests are in `tests/` directory
- Test files follow pattern: `test_*.c`
- Each test file creates a standalone executable
- Tests are organized in suites (slab_suite, engine_suite)
- ctest runs the test_runner which executes all test suites
- Current tests:
  - `tests/test_slab.c` - Slab allocator tests (intrusive queues, dual slabs)
  - `tests/test_engine.c` - Matching engine tests (orderbook, products)

## Code Style Guidelines

### Formatting
- **Indent**: 4 spaces (no tabs)
- **Line Length**: 100 characters max
- **Braces**: K&R style - opening brace on same line
- Use `clang-format` with project `.clang-format` file

### Naming Conventions
- **Files**: lowercase_with_underscores.c/h
- **Types**: PascalCase (e.g., `OpenMatchContext`)
- **Functions**: snake_case with module prefix (e.g., `openmatch_init()`)
- **Macros/Constants**: UPPER_SNAKE_CASE with prefix (e.g., `OM_MAX_BUFFER`)
- **Variables**: snake_case
- **Private/internal**: prefix with underscore (e.g., `_internal_helper()`)

### Includes
- Order: system headers → library headers → local headers
- Use angle brackets for external headers: `#include <stdio.h>`
- Use quotes for local headers: `#include "om_core.h"`
- One header per line
- Alphabetize within groups
- Guard all headers with `#ifndef OM_FILENAME_H`

### Types
- Use `<stdint.h>` types: `uint32_t`, `int64_t`, `size_t`
- Avoid bare `int` for sizes, use `size_t`
- Use `bool` from `<stdbool.h>`
- Explicitly mark `const` for read-only parameters

### Error Handling
- Use return codes (0 = success, negative = error)
- Define error codes in `om_error.h`
- Set `errno` pattern: function returns bool, takes `int *err` param
- Check all malloc/calloc return values
- Use `goto cleanup` pattern for resource cleanup

### Memory Management
- All allocations must have corresponding frees
- Use `calloc()` for zero-initialization
- Never cast malloc return in C
- Document ownership in function documentation

### Documentation
- Use Doxygen-style comments: `/** ... */`
- Document all public API functions
- Mark @param, @return, @note
- Include usage example for complex functions

### Safety
- Check bounds on all array accesses
- Validate all pointer parameters (non-NULL)
- Use `static_assert` for compile-time checks
- Prefer `snprintf` over `sprintf`
- Use `memcpy_s` or bounds-checked alternatives where available

## Lint/Analysis Commands

```bash
# Static analysis with clang-tidy
clang-tidy src/*.c -- -Iinclude

# Format check
cd build && cmake --build . --target format-check

# Apply formatting
cd build && cmake --build . --target format

# Static analysis with cppcheck
cppcheck --enable=all --suppress=missingIncludeSystem src/
```

## Project Structure
```
.
├── AGENTS.md              # This file - coding guidelines for AI agents
├── CMakeLists.txt
├── .gitmodules            # Git submodule configuration
├── deps/
│   ├── klib/              # Git submodule - klib library
│   │   ├── khash.h       # Hash map
│   │   ├── khashl.h      # Lightweight hash map
│   │   ├── kbtree.h      # B-tree
│   │   ├── kavl.h        # AVL tree
│   │   ├── klist.h       # Linked list
│   │   ├── kdq.h         # Queue
│   │   └── ...           # 30+ headers
│   └── check/             # Git submodule - check testing framework
├── include/
│   └── openmatch/
│       ├── om_slab.h     # Slab allocator public API
│       ├── om_hash.h     # Hashmap interface
│       └── om_engine.h   # Matching engine API
├── src/
│   ├── CMakeLists.txt
│   ├── om_slab.c         # Slab allocator implementation
│   ├── om_hash_khash.c   # khash backend (default)
│   ├── om_hash_khashl.c  # khashl backend (opt-in)
│   └── om_engine.c       # Matching engine implementation
├── tests/
│   ├── CMakeLists.txt
│   ├── test_slab.c       # Slab allocator unit tests
│   └── test_engine.c     # Matching engine tests
└── .clang-format
```
