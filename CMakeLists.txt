cmake_minimum_required(VERSION 3.15)
project(VeloxMatch VERSION 1.0.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)

option(BUILD_SHARED_LIBS "Build shared libraries" ON)
option(BUILD_TESTING "Build tests (requires deps/check submodule)" ON)
option(USE_KHASHL "Use khashl hash map backend instead of khash" OFF)
option(ENABLE_ASAN "Enable Address Sanitizer" ON)
option(ENABLE_MSAN "Enable Memory Sanitizer (requires Clang)" OFF)
option(ENABLE_UBSAN "Enable Undefined Behavior Sanitizer" ON)
option(ENABLE_PREFETCH "Enable unconditional prefetch in hot path" OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Wpedantic")
set(CMAKE_C_FLAGS_DEBUG "-g -O0")
set(CMAKE_C_FLAGS_RELEASE "-O3 -DNDEBUG")

# Sanitizers
if(ENABLE_ASAN)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
endif()

if(ENABLE_MSAN)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=memory -fno-omit-frame-pointer -fsanitize-memory-track-origins")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=memory")
    else()
        message(WARNING "Memory Sanitizer requires Clang. Current compiler: ${CMAKE_C_COMPILER_ID}")
    endif()
endif()

if(ENABLE_UBSAN)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=undefined -fno-omit-frame-pointer")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=undefined")
endif()

if(ENABLE_PREFETCH)
    add_compile_definitions(OM_ENABLE_PREFETCH)
endif()

include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_SOURCE_DIR}/include/openmatch)
include_directories(${CMAKE_SOURCE_DIR}/deps/klib)

if(BUILD_TESTING)
    add_subdirectory(deps/check)
endif()
add_subdirectory(src)
add_subdirectory(tools)

if(BUILD_TESTING)
    enable_testing()
    add_subdirectory(tests)
endif()

find_program(CLANG_FORMAT clang-format)
if(CLANG_FORMAT)
    file(GLOB_RECURSE ALL_SOURCE_FILES src/*.c include/*.h tests/*.c)
    add_custom_target(format
        COMMAND ${CLANG_FORMAT} -i ${ALL_SOURCE_FILES}
        COMMENT "Running clang-format"
    )
    add_custom_target(format-check
        COMMAND ${CLANG_FORMAT} --dry-run --Werror ${ALL_SOURCE_FILES}
        COMMENT "Checking clang-format"
    )
endif()
