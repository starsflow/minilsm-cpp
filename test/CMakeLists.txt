cmake_minimum_required(VERSION 3.12)
project(minilsm)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_BUILD_TYPE "Debug")

set(FOLLY /usr/local/folly)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wno-long-long -Wno-unused-variable -Wno-variadic-macros" )
include_directories(${FOLLY}/include/ include/minilsm/) 
link_directories(${FOLLY}/lib/)

add_executable(test_api test_api.cc)
target_link_libraries(test_api glog 
    folly gflags fmt double-conversion dl iberty pthread)