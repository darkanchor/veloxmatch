set(OPENMATCH_SOURCES
    om_slab.c
    orderbook.c
    om_perf.c
    om_engine.c
)

option(OM_USE_WAL_MOCK "Use WAL mock implementation" OFF)

set(OPENMATCH_WAL_SOURCES om_wal.c)
if(OM_USE_WAL_MOCK)
    set(OPENMATCH_WAL_SOURCES om_wal_mock.c)
    add_compile_definitions(OM_USE_WAL_MOCK)
endif()

add_library(openmatch_shared SHARED ${OPENMATCH_SOURCES} ${OPENMATCH_WAL_SOURCES})
add_library(openmatch_static STATIC ${OPENMATCH_SOURCES} ${OPENMATCH_WAL_SOURCES})

add_library(openmatch ALIAS openmatch_shared)

set(OPENMARKET_SOURCES
    om_market.c
    om_worker.c
)

add_library(openmarket_shared SHARED ${OPENMARKET_SOURCES})
add_library(openmarket_static STATIC ${OPENMARKET_SOURCES})

add_library(openmarket ALIAS openmarket_shared)

if(USE_KHASHL)
    target_compile_definitions(openmatch_shared PRIVATE OM_USE_KHASHL)
    target_compile_definitions(openmatch_static PRIVATE OM_USE_KHASHL)
    target_sources(openmatch_shared PRIVATE om_hash_khashl.c)
    target_sources(openmatch_static PRIVATE om_hash_khashl.c)
else()
    target_sources(openmatch_shared PRIVATE om_hash_khash.c)
    target_sources(openmatch_static PRIVATE om_hash_khash.c)
endif()

target_include_directories(openmatch_shared
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_include_directories(openmatch_static
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_include_directories(openmarket_shared
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_include_directories(openmarket_static
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

set_target_properties(openmatch_shared PROPERTIES
    OUTPUT_NAME veloxmatch
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
    PUBLIC_HEADER "${CMAKE_SOURCE_DIR}/include/openmatch/om_slab.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_hash.h;${CMAKE_SOURCE_DIR}/include/openmatch/orderbook.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_wal.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_perf.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_engine.h"
)

set_target_properties(openmatch_static PROPERTIES
    OUTPUT_NAME veloxmatch
    PUBLIC_HEADER "${CMAKE_SOURCE_DIR}/include/openmatch/om_slab.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_hash.h;${CMAKE_SOURCE_DIR}/include/openmatch/orderbook.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_wal.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_perf.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_engine.h"
)

set_target_properties(openmarket_shared PROPERTIES
    OUTPUT_NAME veloxmarket
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
    PUBLIC_HEADER "${CMAKE_SOURCE_DIR}/include/openmarket/om_market.h;${CMAKE_SOURCE_DIR}/include/openmarket/om_worker.h"
)

set_target_properties(openmarket_static PROPERTIES
    OUTPUT_NAME veloxmarket
    PUBLIC_HEADER "${CMAKE_SOURCE_DIR}/include/openmarket/om_market.h;${CMAKE_SOURCE_DIR}/include/openmarket/om_worker.h"
)

install(TARGETS openmatch_shared openmatch_static openmarket_shared openmarket_static
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    PUBLIC_HEADER DESTINATION include
)

# ---------- ombus (message bus) ----------
set(OMBUS_SOURCES om_bus_shm.c om_bus_tcp.c)

add_library(ombus_shared SHARED ${OMBUS_SOURCES})
add_library(ombus_static STATIC ${OMBUS_SOURCES})
add_library(ombus ALIAS ombus_shared)

if(NOT APPLE)
    target_link_libraries(ombus_shared rt)
    target_link_libraries(ombus_static rt)
endif()

# Enable hardware CRC32C on x86_64 (SSE4.2) and ARM (CRC extension)
# x86_64: add -msse4.2 to enable __SSE4_2__ macro + intrinsics
# ARM: Apple Silicon defines __ARM_FEATURE_CRC32 by default (baseline ARMv8.1+),
#      Linux aarch64 may need -march=armv8-a+crc. Use -mcrc to avoid overriding -march.
include(CheckCCompilerFlag)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
    check_c_compiler_flag("-msse4.2" HAS_SSE42)
    if(HAS_SSE42)
        set_source_files_properties(om_bus_shm.c PROPERTIES COMPILE_FLAGS "-msse4.2")
    endif()
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
    # Apple Silicon: CRC is baseline (ARMv8.1+), no flag needed.
    # Linux aarch64: may need -mcrc (GCC) or -march=armv8-a+crc.
    if(NOT APPLE)
        check_c_compiler_flag("-mcrc" HAS_MCRC)
        if(HAS_MCRC)
            set_source_files_properties(om_bus_shm.c PROPERTIES COMPILE_FLAGS "-mcrc")
        else()
            check_c_compiler_flag("-march=armv8-a+crc" HAS_ARM_CRC)
            if(HAS_ARM_CRC)
                set_source_files_properties(om_bus_shm.c PROPERTIES COMPILE_FLAGS "-march=armv8-a+crc")
            endif()
        endif()
    endif()
endif()

target_include_directories(ombus_shared
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_include_directories(ombus_static
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

set_target_properties(ombus_shared PROPERTIES
    OUTPUT_NAME vmbus
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
    PUBLIC_HEADER "${CMAKE_SOURCE_DIR}/include/ombus/om_bus.h;${CMAKE_SOURCE_DIR}/include/ombus/om_bus_error.h;${CMAKE_SOURCE_DIR}/include/ombus/om_bus_tcp.h"
)

set_target_properties(ombus_static PROPERTIES
    OUTPUT_NAME vmbus
    PUBLIC_HEADER "${CMAKE_SOURCE_DIR}/include/ombus/om_bus.h;${CMAKE_SOURCE_DIR}/include/ombus/om_bus_error.h;${CMAKE_SOURCE_DIR}/include/ombus/om_bus_tcp.h"
)

install(TARGETS ombus_shared ombus_static
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    PUBLIC_HEADER DESTINATION include/ombus
)