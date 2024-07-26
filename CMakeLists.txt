cmake_minimum_required(VERSION 3.29)
set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake")
project(minilsm)

add_subdirectory(test/)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_BUILD_TYPE "Debug")

set(PACKAGE_DIR ${CMAKE_SOURCE_DIR}/vcpkg_installed/x64-linux/)

find_package(folly REQUIRED REQUIRED PATHS ${PACKAGE_DIR})
find_package(glog REQUIRED REQUIRED PATHS ${PACKAGE_DIR})
find_package(fmt REQUIRED REQUIRED PATHS ${PACKAGE_DIR})
find_package(double-conversion REQUIRED PATHS ${PACKAGE_DIR})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wno-long-long -Wno-unused-variable -Wno-variadic-macros -Wl,--trace")

set(SOURCE_H_DIR ${CMAKE_SOURCE_DIR}/src/ ${CMAKE_SOURCE_DIR}/include/minilsm/)
file(GLOB_RECURSE SOURCE_CC_PATH ${CMAKE_SOURCE_DIR}/src/*.cc)

add_library(minilsm ${SOURCE_CC_PATH})
target_include_directories(minilsm PRIVATE ${PACKAGE_DIR}/include/ ${SOURCE_H_DIR}/)
target_link_directories(minilsm PRIVATE ${PACKAGE_DIR}/lib/)
target_link_libraries(minilsm PRIVATE folly glog::glog fmt double-conversion dl iberty pthread)
