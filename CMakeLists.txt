# Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.10)
project(ycsb-cpp LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 20)

# Enable verbose compilation output
set(CMAKE_VERBOSE_MAKEFILE OFF)

# Set general optimization level to -O3 for GCC only in Release mode
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic -g")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Wpedantic -g")
    if(CMAKE_BUILD_TYPE STREQUAL "Release")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3")
    endif()
endif()

# set(CMAKE_BUILD_TYPE Release)
set(CompilerFlags
        CMAKE_CXX_FLAGS
        CMAKE_CXX_FLAGS_DEBUG
        CMAKE_CXX_FLAGS_RELEASE
        CMAKE_C_FLAGS
        CMAKE_C_FLAGS_DEBUG
        CMAKE_C_FLAGS_RELEASE
        )

if(MSVC)
    foreach(CompilerFlag ${CompilerFlags})
        string(REPLACE "/MD" "/MT" ${CompilerFlag} "${${CompilerFlag}}")
    endforeach()
endif()

option(BIND_ROCKSDB "build with rocksdb" OFF)
option(BIND_LMDB "build with lmdb" OFF)
option(BIND_LEVELDB "build with leveldb" OFF)
option(BIND_WIREDTIGER "build with wiredtiger" OFF)
option(BIND_LEAVES "build with leaves" OFF)
option(BIND_SQLITE "build with sqlite" OFF)
option(BIND_REDIS "build with redis" OFF)
option(BIND_AEROSPIKE "build with aerospike" OFF)
option(BIND_BADGER "build with badger" OFF)

option(WITH_ZLIB "linking YCSB with zlib; needed by HdrHISTOGRAM, DO NOT TURN OFF" ON)
option(WITH_LZ4 "linking YCSB with lz4" OFF)
option(WITH_SNAPPY "linking YCSB with snappy" OFF)
option(WITH_ZSTD "linking YCSB with zstd" OFF)
option(WITH_BZ2 "linking YCSB with bzip2" OFF)

file(GLOB_RECURSE YCSB_CORE_SRC "core/*.cc")
list(APPEND YCSB_CORE_SRC "${CMAKE_CURRENT_SOURCE_DIR}/utils/sha256.cpp")
file(GLOB_RECURSE YCSB_NULL_SRC "adapters/null/*.cc")

include(FindThreads)

add_executable(ycsb ${YCSB_CORE_SRC} ${YCSB_NULL_SRC})

if(NOT MSVC)
    set_source_files_properties(${YCSB_CORE_SRC} ${YCSB_NULL_SRC}
        PROPERTIES COMPILE_OPTIONS "-Wno-unused-parameter")
endif()
target_include_directories(ycsb PRIVATE ${PROJECT_SOURCE_DIR})

if (BIND_ROCKSDB)
    message(STATUS "BIND_ROCKSDB - ON")
    set(WITH_ZLIB ON)
    set(WITH_BZ2 ON)
    file(GLOB_RECURSE YCSB_ROCKSDB_SRC "adapters/rocksdb/*.cc")
    target_sources(ycsb PRIVATE ${YCSB_ROCKSDB_SRC})
    if(NOT MSVC)
        set_source_files_properties(${YCSB_ROCKSDB_SRC}
            PROPERTIES COMPILE_OPTIONS "-Wno-unused-parameter")
    endif()

    find_package(RocksDB CONFIG)
    if(RocksDB_FOUND)
        if(MSVC)
            target_link_libraries(ycsb PRIVATE RocksDB::rocksdb rpcrt4.lib)
        else()
            target_link_libraries(ycsb PRIVATE RocksDB::rocksdb)
        endif()
    else()
        message(STATUS "Try to find rocksdb library using find_library")
        find_library(ROCKSDB_LIB rocksdb REQUIRED)
        find_path(ROCKSDB_INCLUDE_DIR "rocksdb/db.h" REQUIRED)
        target_link_libraries(ycsb PRIVATE ${ROCKSDB_LIB})
        target_include_directories(ycsb PRIVATE ${ROCKSDB_INCLUDE_DIR})
    endif()
    message(STATUS "Found RocksDB library")
    if(NOT MSVC)
        target_link_libraries(ycsb PRIVATE dl)
    endif()
else()
    message(STATUS "BIND_ROCKSDB - OFF")
endif()

if (BIND_LMDB)
    message(STATUS "BIND_LMDB - ON")
    file(GLOB_RECURSE YCSB_LMDB_SRC "adapters/lmdb/*.cc")
    target_sources(ycsb PRIVATE ${YCSB_LMDB_SRC})
    if(NOT MSVC)
        set_source_files_properties(${YCSB_LMDB_SRC}
            PROPERTIES COMPILE_OPTIONS "-Wno-unused-parameter;-Wno-type-limits")
    endif()

    find_package(lmdb CONFIG)
    if(lmdb_FOUND)
        target_link_libraries(ycsb PRIVATE lmdb)
    else()
        message(STATUS "Try to find lmdb library using find_library")
        find_library(LMDB_LIB lmdb REQUIRED)
        find_path(LMDB_INCLUDE_DIR "lmdb.h" REQUIRED)
        target_link_libraries(ycsb PRIVATE ${LMDB_LIB})
        target_include_directories(ycsb PRIVATE ${LMDB_INCLUDE_DIR})
    endif()
    message(STATUS "Found LMDB library")
else()
    message(STATUS "BIND_LMDB - OFF")
endif()

if (BIND_LEVELDB)
    message(STATUS "BIND_LEVELDB - ON")
    file(GLOB_RECURSE YCSB_LEVELDB_SRC "adapters/leveldb/*.cc")
    target_sources(ycsb PRIVATE ${YCSB_LEVELDB_SRC})
    if(NOT MSVC)
        set_source_files_properties(${YCSB_LEVELDB_SRC}
            PROPERTIES COMPILE_OPTIONS "-Wno-unused-parameter")
    endif()

    find_package(leveldb CONFIG)
    if(leveldb_FOUND)
        target_link_libraries(ycsb PRIVATE leveldb::leveldb)
    else()
        message(STATUS "Try to find leveldb library using find_library")
        find_library(LEVELDB_LIB leveldb REQUIRED)
        find_path(LEVELDB_INCLUDE_DIR "leveldb/db.h" REQUIRED)
        target_link_libraries(ycsb PRIVATE ${LEVELDB_LIB})
        target_include_directories(ycsb PRIVATE ${LEVELDB_INCLUDE_DIR})
    endif()
    message(STATUS "Found leveldb library")
else()
    message(STATUS "BIND_LEVELDB - OFF")
endif ()

if (BIND_WIREDTIGER)
    message(STATUS "BIND_WIREDTIGER - ON")
    file(GLOB_RECURSE YCSB_WT_SRC "adapters/wiredtiger/*.cc")
    target_sources(ycsb PRIVATE ${YCSB_WT_SRC})
    if(NOT MSVC)
        set_source_files_properties(${YCSB_WT_SRC}
            PROPERTIES COMPILE_OPTIONS "-Wno-unused-parameter;-Wno-missing-field-initializers")
    endif()
    find_library(WIREDTIGER_LIB wiredtiger REQUIRED)
    find_path(WIREDTIGER_INCLUDE_DIR "wiredtiger.h" REQUIRED)
    target_link_libraries(ycsb PRIVATE ${WIREDTIGER_LIB})
    target_include_directories(ycsb PRIVATE ${WIREDTIGER_INCLUDE_DIR})
else()
    message(STATUS "BIND_WIREDTIGER - OFF")
endif()

if (BIND_LEAVES)
    message(STATUS "BIND_LEAVES - ON")
    file(GLOB_RECURSE YCSB_LEAVES_SRC "adapters/leaves/*.cc")
    target_sources(ycsb PRIVATE ${YCSB_LEAVES_SRC})

    # ── Priority 1: Use leaves submodule if it exists
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/external/leaves/CMakeLists.txt")
        message(STATUS "Configuring Leaves (via submodule at external/leaves)")
        add_subdirectory(external/leaves)
        target_link_libraries(ycsb PRIVATE leaves::leaves)
    # ── Priority 2: Use explicit leaves_DIR if provided
    elseif(DEFINED leaves_DIR AND EXISTS "${leaves_DIR}/leaves-targets.cmake")
        message(STATUS "Configuring Leaves (via leaves_DIR package)")
        find_package(leaves CONFIG REQUIRED)
        target_link_libraries(ycsb PRIVATE leaves::leaves)
    # ── Priority 3: Fallback to include path from peer directory (backward compatibility)
    else()
        message(STATUS "Configuring Leaves (via peer directory ../leaves - fallback)")
        target_include_directories(ycsb PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../leaves/include)
    endif()

    # Suppress pedantic and unused-parameter warnings for leaves headers/adapter
    if(NOT MSVC)
        set_source_files_properties(${YCSB_LEAVES_SRC}
            PROPERTIES COMPILE_OPTIONS "-Wno-pedantic;-Wno-unused-parameter")
    endif()

    # Add specific compiler optimizations for Leaves files with GCC only in Release mode
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_BUILD_TYPE STREQUAL "Release")
        foreach(LEAVES_FILE ${YCSB_LEAVES_SRC})
            set_source_files_properties(${LEAVES_FILE} PROPERTIES
                COMPILE_FLAGS "-DNDEBUG -DLEAVES_SINGLE_PROCESS -g -march=native -O3 -funroll-loops -flto=auto"
            )
        endforeach()
        message(STATUS "Applied GCC optimizations to Leaves files: -DNDEBUG -DLEAVES_SINGLE_PROCESS -g -march=native -O3 -funroll-loops -flto=auto")
    endif()

    # Add LEAVES_SINGLE_PROCESS for Leaves files in Debug mode
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        foreach(LEAVES_FILE ${YCSB_LEAVES_SRC})
            set_source_files_properties(${LEAVES_FILE} PROPERTIES
                COMPILE_FLAGS "-DLEAVES_SINGLE_PROCESS -g -O0"
            )
        endforeach()
        message(STATUS "Applied Debug flags to Leaves files: -DLEAVES_SINGLE_PROCESS -g -O0")
    endif()
    
    message(STATUS "Configured Leaves binding")
else()
    message(STATUS "BIND_LEAVES - OFF")
endif()

if (BIND_SQLITE)
    message(STATUS "BIND_SQLITE - ON")
    file(GLOB_RECURSE YCSB_SQLITE_SRC "adapters/sqlite/*.cc")
    target_sources(ycsb PRIVATE ${YCSB_SQLITE_SRC})
    if(NOT MSVC)
        set_source_files_properties(${YCSB_SQLITE_SRC}
            PROPERTIES COMPILE_OPTIONS "-Wno-unused-parameter;-Wno-unused-variable")
    endif()

    find_package(SQLite3)
    if(SQLite3_FOUND)
        target_link_libraries(ycsb PRIVATE SQLite::SQLite3)
        target_include_directories(ycsb PRIVATE ${SQLite3_INCLUDE_DIRS})
    else()
        message(STATUS "Try to find sqlite3 library using find_library")
        find_library(SQLITE3_LIB sqlite3 REQUIRED)
        find_path(SQLITE3_INCLUDE_DIR "sqlite3.h" REQUIRED)
        target_link_libraries(ycsb PRIVATE ${SQLITE3_LIB})
        target_include_directories(ycsb PRIVATE ${SQLITE3_INCLUDE_DIR})
    endif()
    message(STATUS "Found SQLite3 library")
else()
    message(STATUS "BIND_SQLITE - OFF")
endif()

if (BIND_REDIS)
    message(STATUS "BIND_REDIS - ON")
    file(GLOB_RECURSE YCSB_REDIS_SRC "adapters/redis/*.cc")
    target_sources(ycsb PRIVATE ${YCSB_REDIS_SRC})
    
    find_library(HIREDIS_LIB hiredis REQUIRED)
    find_path(HIREDIS_INCLUDE_DIR "hiredis/hiredis.h" REQUIRED)
    target_link_libraries(ycsb PRIVATE ${HIREDIS_LIB})
    target_include_directories(ycsb PRIVATE ${HIREDIS_INCLUDE_DIR})
    message(STATUS "Found hiredis library")
else()
    message(STATUS "BIND_REDIS - OFF")
endif()

if (BIND_AEROSPIKE)
    message(STATUS "BIND_AEROSPIKE - ON")
    file(GLOB_RECURSE YCSB_AEROSPIKE_SRC "adapters/aerospike/*.cc")
    target_sources(ycsb PRIVATE ${YCSB_AEROSPIKE_SRC})
    
    find_library(AEROSPIKE_LIB aerospike REQUIRED)
    find_path(AEROSPIKE_INCLUDE_DIR "aerospike/aerospike.h" REQUIRED)
    target_link_libraries(ycsb PRIVATE ${AEROSPIKE_LIB})
    target_include_directories(ycsb PRIVATE ${AEROSPIKE_INCLUDE_DIR})
    # Aerospike requires pthread, ssl, crypto, yaml, and libuv for async support
    target_link_libraries(ycsb PRIVATE pthread ssl crypto yaml uv)
    message(STATUS "Found Aerospike library")
else()
    message(STATUS "BIND_AEROSPIKE - OFF")
endif()

if (BIND_BADGER)
    message(STATUS "BIND_BADGER - ON")
    file(GLOB_RECURSE YCSB_BADGER_SRC "adapters/badger/*.cc")
    target_sources(ycsb PRIVATE ${YCSB_BADGER_SRC})

    set(CBADGER_DIR ${PROJECT_SOURCE_DIR}/adapters/badger/cbadger)
    set(CBADGER_LIB_PATH ${CBADGER_DIR}/libcbadger.a)

    # Add a custom command to build the Go library
    add_custom_command(
        OUTPUT ${CBADGER_LIB_PATH}
        COMMAND go build -buildmode=c-archive -o ${CBADGER_LIB_PATH}
        WORKING_DIRECTORY ${CBADGER_DIR}
        COMMENT "Building cbadger Go library"
        VERBATIM
    )

    # Create a custom target for the Go library
    add_custom_target(cbadger_build DEPENDS ${CBADGER_LIB_PATH})

    # Add a dependency to ensure cbadger is built before ycsb
    add_dependencies(ycsb cbadger_build)

    # Link the library that will be built by the custom command
    target_link_libraries(ycsb PRIVATE ${CBADGER_LIB_PATH})
    target_include_directories(ycsb PRIVATE ${PROJECT_SOURCE_DIR}/adapters/badger)
    message(STATUS "Configured BadgerDB CGo library build")
else()
    message(STATUS "BIND_BADGER - OFF")
endif()

if(WITH_ZLIB)
    message(STATUS "WITH_ZLIB - ON")
    find_package(ZLIB)
    if(ZLIB_FOUND)
        target_link_libraries(ycsb PRIVATE ZLIB::ZLIB)
    else()
        message(STATUS "Try to find zlib library using find_library")
        find_library(ZLIB_LIB zlib)
        if(ZLIB_LIB-NOTFOUND)
            find_library(ZLIB_LIB z REQUIRED)
        endif()
        target_link_libraries(ycsb PRIVATE ${ZLIB_LIB})
    endif()
    message(STATUS "Found ZLIB library")
else()
    message(STATUS "WITH_ZLIB - OFF")
endif()

if(WITH_SNAPPY)
    message(STATUS "WITH_SNAPPY - ON")
    find_package(Snappy CONFIG)
    if(Snappy_FOUND)
        target_link_libraries(ycsb PRIVATE Snappy::snappy)
    else()
        message(STATUS "Try to find Snappy library using find_library")
        find_library(SNAPPY_LIB snappy REQUIRED)
        target_link_libraries(ycsb PRIVATE ${SNAPPY_LIB})
    endif()
    message(STATUS "Found Snappy library")
else()
    message(STATUS "WITH_SNAPPY - OFF")
endif()

if(WITH_LZ4)
    message(STATUS "WITH_LZ4 - ON")
    find_package(lz4 CONFIG)
    if(lz4_FOUND)
        target_link_libraries(ycsb PRIVATE lz4::lz4)
    else()
        message(STATUS "Try to find LZ4 library using find_library")
        find_library(LZ4_LIB lz4 REQUIRED)
        target_link_libraries(ycsb PRIVATE ${LZ4_LIB})
    endif()
    message(STATUS "Found LZ4 library")
else()
    message(STATUS "WITH_LZ4 - OFF")
endif()

if(WITH_ZSTD)
    message(STATUS "WITH_ZSTD - ON")
    find_package(zstd CONFIG)
    if(zstd_FOUND)
        target_link_libraries(ycsb PRIVATE $<IF:$<TARGET_EXISTS:zstd::libzstd_shared>,zstd::libzstd_shared,zstd::libzstd_static>)
    else()
        message(STATUS "Try to find zstd library using find_library")
        find_library(ZSTD_LIB zstd REQUIRED)
        target_link_libraries(ycsb PRIVATE ${ZSTD_LIB})
    endif()
    message(STATUS "Found zstd library")
else()
    message(STATUS "WITH_ZSTD - OFF")
endif()

if(WITH_BZ2)
    message(STATUS "WITH_BZ2 - ON")
    find_package(BZip2)
    if(BZip2_FOUND)
        target_link_libraries(ycsb PRIVATE BZip2::BZip2)
    else()
        message(STATUS "Try to find BZip2 library using find_library")
        find_library(BZ2_LIB bz2 REQUIRED)
        target_link_libraries(ycsb PRIVATE ${BZ2_LIB})
    endif()
    message(STATUS "Found BZip2 library")
else()
    message(STATUS "WITH_BZ2 - OFF")
endif()

add_subdirectory(HdrHistogram_c)
include_directories(HdrHistogram_c/include)
add_compile_definitions(HDRMEASUREMENT)
add_dependencies(ycsb hdr_histogram_static)
target_link_libraries(ycsb PRIVATE hdr_histogram_static)