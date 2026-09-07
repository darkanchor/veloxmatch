find_package(Threads REQUIRED)

set(TEST_SOURCES
    test_runner.c
    test_slab.c
    test_orderbook.c
    test_wal.c
    test_engine.c
    test_market.c
    test_bus.c
    test_bus_mp.c
)

add_executable(test_runner ${TEST_SOURCES})
target_link_libraries(test_runner openmatch openmarket ombus ombusmp check Threads::Threads)
if(NOT APPLE)
    target_link_libraries(test_runner rt)
endif()
target_include_directories(test_runner PRIVATE ${CMAKE_SOURCE_DIR}/deps/check/src ${CMAKE_BINARY_DIR}/deps/check)

add_test(NAME test_runner COMMAND test_runner)

add_executable(bench_market_perf bench_market_perf.c)
target_link_libraries(bench_market_perf openmatch openmarket Threads::Threads m)

add_executable(bench_bus_perf bench_bus_perf.c)
target_link_libraries(bench_bus_perf ombus Threads::Threads m)
if(NOT APPLE)
    target_link_libraries(bench_bus_perf rt)
endif()

# This measurement fixture uses Linux affinity and x86 pause instructions.
# Baseline builds may compile the same source with BENCH_HAS_COPY=0.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64|i.86)$")
    add_executable(bench_bus_mp_perf bench_bus_mp_perf.c)
    target_link_libraries(bench_bus_mp_perf ombusmp Threads::Threads)
endif()
