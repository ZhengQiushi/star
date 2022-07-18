##################################################################################
# TEST CMAKELISTS
##################################################################################

##################################################################################

# --[ Tests lists

# creates 'tests' lists
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -Wall -Wno-long-long -Wno-unused-variable -Wno-variadic-macros -pedantic -std=c++14 -g -ggdb3")

message("---start build test---")
# peloton_pickup_peloton_sources(${PROJECT_SOURCE_DIR})
file(GLOB_RECURSE ALL_SOURCE_FILES benchmark/*.h common/*.h core/*.h core/*.cpp protocol/*.h bench*.cpp)

set(common ${PROJECT_SOURCE_DIR}/build/libcommon.a)

file(GLOB test_hdrs ${PROJECT_SOURCE_DIR}/test/include/*/*.h*)
file(GLOB test_srcs_temp ${PROJECT_SOURCE_DIR}/test/*/*test.cpp)
file(GLOB perf_srcs ${PROJECT_SOURCE_DIR}/test/performance/*.cpp)

# ---[ Includes
set(Peloton_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
set(Peloton_TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test/include)
set(Peloton_THIRD_PARTY_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/third_party)
include_directories(${Peloton_SRC_INCLUDE_DIR} ${Peloton_TEST_INCLUDE_DIR} ${Peloton_THIRD_PARTY_INCLUDE_DIR} ${PROJECT_BINARY_DIR})


set(test_srcs "")

foreach(test_src_temp ${test_srcs_temp} )
    string(REPLACE "//" "/" test_src ${test_src_temp})
    list(APPEND test_srcs ${test_src})
endforeach(test_src_temp ${test_srcs_temp})

# For each performance test source file
# First remove them from the test source list since they should not be run as
# part of the correctness test
foreach(perf_src ${perf_srcs} )
    list(REMOVE_ITEM test_srcs ${perf_src})
endforeach(perf_src ${perf_srcs})

##################################################################################

# --[ Gmock

set(GMOCK_DIR  "${PROJECT_SOURCE_DIR}/third_party/gmock")
file(GLOB gmock_srcs  ${GMOCK_DIR}/*.cc)
include_directories(SYSTEM ${GMOCK_DIR})

##################################################################################

# --[ Tests Common library

set(TESTING_UTIL_FORECAST   ${PROJECT_SOURCE_DIR}/test/brain/testing_forecast_util.cpp)
set(TESTING_UTIL_AUGMENTEDNN ${PROJECT_SOURCE_DIR}/test/brain/testing_augmented_nn_util.cpp)

add_library(peloton-test-common EXCLUDE_FROM_ALL ${gmock_srcs} ${HARNESS}
            ${TESTING_UTIL_EXECUTOR}
            ${TESTING_UTIL_LOGGING}
            ${TESTING_UTIL_JOIN}
            ${TESTING_UTIL_TXN}
            ${TESTING_UTIL_STATS}
            ${TESTING_UTIL_INDEX}
            ${TESTING_UTIL_SQL}
            ${TESTING_UTIL_CODEGEN}
            ${TESTING_UTIL_FORECAST}
            ${TESTING_UTIL_OPTIMIZER}
            ${TESTING_UTIL_AUGMENTEDNN}
)

# --[ Add "make check" target

set(CTEST_FLAGS "")
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} ${CTEST_FLAGS} --verbose)

##################################################################################

# --[ Functionality Tests

foreach(test_src ${test_srcs} )
    # message("test_src = " ${test_src})
    
    # get test file name
	get_filename_component(test_bare_name ${test_src} NAME)	
	string(REPLACE ".cpp" "" test_bare_name_without_extension ${test_bare_name})
    string(REPLACE "\"" "" test_name ${test_bare_name_without_extension})
     
    message("${test_name}")
    # create executable
    add_executable(${test_name} EXCLUDE_FROM_ALL ${test_src})
    add_dependencies(check ${test_name})
    
    #message("Correctness test: "  ${test_name})
    
    # link libraries
    target_link_libraries(${test_name} common peloton-test-common)    

	# set target properties    
	set_target_properties(${test_name}
	    PROPERTIES
	    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
	    COMMAND ${test_name}
	)   
	
	# add test
  add_test(${test_name} ${CMAKE_BINARY_DIR}/test/${test_name} --gtest_color=yes
    --gtest_output=xml:${CMAKE_BINARY_DIR}/test/${test_name}.xml)
  
  # leak suppression / whitelist
  set_property(TEST ${test_name}
	PROPERTY ENVIRONMENT
	"LSAN_OPTIONS=suppressions=${PROJECT_SOURCE_DIR}/test/leak_suppr.txt")
	 
endforeach(test_src ${test_srcs})

##################################################################################

# --[ Performance Tests

# Compile and run performance tests separately

foreach(perf_src ${perf_srcs} )
    list(REMOVE_ITEM test_srcs ${perf_src})
    
    #message("test_srcs = " ${test_srcs})
    #message("perf_src = " ${perf_src})
    
    get_filename_component(perf_bare_name ${perf_src} NAME)	
	string(REPLACE ".cpp" "" perf_bare_name_without_extension ${perf_bare_name})
    string(REPLACE "\"" "" perf_name ${perf_bare_name_without_extension})
     
    # create executable
    add_executable(${perf_name} EXCLUDE_FROM_ALL ${perf_src})
    add_dependencies(check ${perf_name})
    
    #message("Performance test: "  ${perf_name})
    
    # link libraries
    target_link_libraries(${perf_name} common peloton-test-common)    

	# set target properties    
	set_target_properties(${perf_name}
	    PROPERTIES
	    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
	    COMMAND ${perf_name}
	)   
	
	# add test
	add_test(${perf_name} ${CMAKE_BINARY_DIR}/test/${perf_name} --gtest_color=yes
           --gtest_output=xml:${CMAKE_BINARY_DIR}/test/${perf_name}.xml)
	
endforeach(perf_src ${perf_srcs})
