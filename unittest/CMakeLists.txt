include(CTest)
project(unittest)
set(CMAKE_BUILD_TYPE "Debug")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wno-long-long -Wno-unused-variable -Wno-variadic-macros" )
message("compile flags in unitest : " ${CMAKE_CXX_FLAGS})

find_package(GTest REQUIRED PATHS ${PACKAGE_DIR})

# file(GLOB_RECURSE test_files "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
set(test_files 
    # ${CMAKE_CURRENT_SOURCE_DIR}/api/libfolly.cc
    # ${CMAKE_CURRENT_SOURCE_DIR}/api/skiplist.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/slice.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/memtable.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/block.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/sstable.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/iterator.cc
)

message("header path: ${SOURCE_H_DIR}")
message("source file: ${SOURCE_CPP_FILES}")

set(PROJECT_NAME "MiniLSM")
set(PROJECT_ROOT_PATH ${CMAKE_SOURCE_DIR})

configure_file(
    "${CMAKE_SOURCE_DIR}/src/defs.h.in"
    "${CMAKE_SOURCE_DIR}/src/defs.h"
)

foreach(test_file ${test_files})
    message("test case : ${test_file}")
    cmake_path(GET test_file STEM test_case)
 
    set(target_name "${test_case}_test")
    set(test_name "TEST_${test_case}")

    add_executable(${target_name} ${test_file} ${SOURCE_CPP_FILES})
    message("build target : ${target_name}")
    target_include_directories(${target_name} PRIVATE ${PACKAGE_DIR}/include/ ${SOURCE_H_DIR}/)
    target_link_directories(${target_name} PRIVATE ${PACKAGE_DIR}/lib/)
    target_link_libraries(${target_name} PRIVATE folly glog::glog gtest fmt double-conversion dl iberty pthread)

    add_test(
        NAME ${test_name}
        COMMAND $<TARGET_FILE:${target_name}>
    )
endforeach()