cmake_minimum_required(VERSION 3.29)
set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake")
project(minilsm)

set(CMAKE_CXX_STANDARD 17)
set(BUILD_TYPE "Debug")
set(CMAKE_BUILD_TYPE ${BUILD_TYPE})

set(PACKAGE_DIR ${CMAKE_SOURCE_DIR}/vcpkg_installed/x64-linux/)

find_package(folly REQUIRED REQUIRED PATHS ${PACKAGE_DIR})
find_package(glog REQUIRED REQUIRED PATHS ${PACKAGE_DIR})
find_package(fmt REQUIRED REQUIRED PATHS ${PACKAGE_DIR})
find_package(double-conversion REQUIRED PATHS ${PACKAGE_DIR})

set(SOURCE_H_DIR ${CMAKE_SOURCE_DIR}/src/ ${CMAKE_SOURCE_DIR}/include/minilsm/)
set(SOURCE_CPP_FILES
    ${CMAKE_SOURCE_DIR}/src/memtable/iterator.cc
    ${CMAKE_SOURCE_DIR}/src/memtable/memtable.cc
    ${CMAKE_SOURCE_DIR}/src/block/iterator.cc 
    ${CMAKE_SOURCE_DIR}/src/block/block.cc
    ${CMAKE_SOURCE_DIR}/src/sstable/sstable.cc
    ${CMAKE_SOURCE_DIR}/src/sstable/iterator.cc
    ${CMAKE_SOURCE_DIR}/src/mvcc/key.cc
    ${CMAKE_SOURCE_DIR}/src/util/skiplist.cc
)
# file(GLOB_RECURSE SOURCE_CC_PATH ${CMAKE_SOURCE_DIR}/src/*.cc)

enable_testing()
add_subdirectory(unittest/)

add_library(minilsm)
target_include_directories(minilsm PRIVATE ${PACKAGE_DIR}/include/ ${SOURCE_H_DIR}/)
target_link_directories(minilsm PRIVATE ${PACKAGE_DIR}/lib/)
target_sources(minilsm PRIVATE ${SOURCE_CPP_FILES})
target_link_libraries(minilsm PRIVATE folly glog::glog fmt double-conversion dl iberty pthread)
