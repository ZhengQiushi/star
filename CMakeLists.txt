cmake_minimum_required(VERSION 3.2)
project(star)

set(CMAKE_CXX_COMPILER "/usr/bin/g++")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -Wall -Wno-long-long -Wno-unused-variable -Wno-variadic-macros -pedantic -O2 -std=c++14")
set(CMAKE_CXX_STANDARD 14)

# set(CMAKE_EXE_LINKER_FLAGS "-static")
# set(CMAKE_EXE_LINKER_FLAGS "-static-libgcc -static-libstdc++")

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0")

# find_library(jemalloc_lib jemalloc) # jemalloc 5.0
set(jemalloc_lib, "/usr/lib64/libjemalloc.so.1")

# additional target to perform clang-format run, requires clang-format

# get all project files
file(GLOB_RECURSE ALL_SOURCE_FILES benchmark/*.h common/*.h core/*.h protocol/*.h bench*.cpp)

add_custom_target(
        format
        COMMAND clang-format
        -style=LLVM
        -i
        -sort-includes
        ${ALL_SOURCE_FILES}
)

include_directories(${CMAKE_SOURCE_DIR})

file(GLOB_RECURSE COMMON_SOURCE_FILES common/*.cpp)
add_library(common STATIC ${COMMON_SOURCE_FILES})

include_directories(absl_DIR /usr/local/include)

# find_package(absl REQUIRED)

if(APPLE)
    find_package(glog REQUIRED)
    find_package(gflags REQUIRED)
    target_link_libraries(common ${jemalloc_lib} glog::glog gflags)# absl::flat_hash_map)
else()
    # find_package(glog REQUIRED)
    # set(gflags_NAMES libgflags.a)
    # find_package(gflags REQUIRED)
    set(glog "/usr/local/lib/libglog.a")
    set(gflags "/usr/local/lib/libgflags.a")

    target_link_libraries(common ${jemalloc_lib} ${glog} ${gflags}) # absl::flat_hash_map  -static)
    # target_link_libraries(common libgflags.a)
    
endif()

# include_directories(absl_DIR /usr/local/include) 

# Process Abseil's CMake build system
# add_subdirectory(3rd/abseil-cpp)

add_executable(bench_tpcc bench_tpcc.cpp)
target_link_libraries(bench_tpcc common)

add_executable(bench_ycsb bench_ycsb.cpp)
target_link_libraries(bench_ycsb common) # -static

# target_link_libraries(bench_ycsb absl::flat_hash_map)
