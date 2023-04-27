cmake_minimum_required(VERSION 3.2)
project(star)

set(CMAKE_CXX_COMPILER "/usr/bin/g++")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -Wall -O0  -g -ggdb3 -pthread -Wall -Wno-long-long -Wno-unused-variable -Wno-address-of-packed-member -Wno-variadic-macros -pedantic -std=c++14 -Wno-unknown-pragmas") #

set(CMAKE_CXX_STANDARD 14)

# set(CMAKE_EXE_LINKER_FLAGS "-static")
# set(CMAKE_EXE_LINKER_FLAGS "-static-libgcc -static-libstdc++")

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0")

# additional target to perform clang-format run, requires clang-format

include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/third_party)

# get all project files
file(GLOB_RECURSE ALL_SOURCE_FILES benchmark/*.h common/*.h core/*.h core/*.cpp protocol/*.h bench*.cpp)

include_directories(${CMAKE_SOURCE_DIR})

file(GLOB_RECURSE COMMON_SOURCE_FILES common/*.cpp)
add_library(common STATIC ${COMMON_SOURCE_FILES} ${ALL_SOURCE_FILES})


# if(APPLE)
#     find_package(glog REQUIRED)
#     find_package(gflags REQUIRED)
#     target_link_libraries(common ${jemalloc_lib} glog::glog gflags)# absl::flat_hash_map)
# else()
    # find_package(glog REQUIRED)
    # set(gflags_NAMES libgflags.a)
    # find_package(gflags REQUIRED)
    set(glog "/usr/local/lib/libglog.a")
    set(gflags "/usr/local/lib/libgflags.so")
    set(TFlowC_LIBRARIES "/usr/local/lib/libtensorflow.so")
    set(TFlowC_FRAMEWORK_LIBRARIES "/usr/local/lib/libtensorflow_framework.so")
    set(GMOCK_LIBRARIES "/usr/local/lib/libgmock.a")
    set(GTEST_LIBRARIES "/usr/local/lib/libgtest.a")
    set(METIS_LIBRARIES "/usr/local/lib/libmetis.a")

    # target_link_libraries(common libgflags.a)
    # --[ Eigen3
    find_package(Eigen3 REQUIRED)
    include_directories(SYSTEM ${EIGEN3_INCLUDE_DIR})

# endif()


# add_executable(bench_tpcc bench_tpcc.cpp)
# target_link_libraries(bench_tpcc common)

add_executable(zqs_laji bench_ycsb.cpp)
target_link_libraries(zqs_laji common 
                    ${jemalloc_lib} ${glog} ${gflags} 
                    ${TFlowC_LIBRARIES} ${TFlowC_FRAMEWORK_LIBRARIES}
                    ${GMOCK_LIBRARIES} ${GTEST_LIBRARIES} 
                    ${METIS_LIBRARIES}) # -static


add_executable(zqs_laji_metis metis_test.cpp)
target_link_libraries(zqs_laji_metis common 
                    ${jemalloc_lib} ${glog} ${gflags} 
                    ${TFlowC_LIBRARIES} ${TFlowC_FRAMEWORK_LIBRARIES}
                    ${GMOCK_LIBRARIES} ${GTEST_LIBRARIES} 
                    ${METIS_LIBRARIES}) # -static

# 
add_subdirectory(test)
