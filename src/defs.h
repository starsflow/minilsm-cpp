/*
 * @Author: lxc
 * @Date: 2024-04-08 22:08:44
 * @Description: Global headers, marcos and definitions...
 */
#ifndef DEFS_H
#define DEFS_H

#include "folly/ConcurrentSkipList.h"

#include <cstddef>
#include <vector>
#include <array>
#include <shared_mutex>
#include <mutex>
#include <memory>
#include <iterator>
#include <variant>
#include <algorithm>
#include <fstream>
#include <queue>
#include <random>
#include <set>
#include <filesystem>

#define u8 uint8_t
#define u16 uint16_t
#define u32 uint32_t
#define u64 uint64_t
#define i8 int8_t
#define i16 int16_t
#define i32 int32_t
#define i64 int64_t
#define f32 float
#define f64 double

#define PROJECT_NAME "MiniLSM"
#define PROJECT_ROOT_PATH "/home/lxc/storage/MiniLSM"

#define Debug

#endif
