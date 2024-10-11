/*
 * @Author: lxc
 * @Date: 2024-10-09 10:02:11
 * @Description: util library to get current timestamp
 */
#ifndef TIME_H
#define TIME_H

#include "defs.h"
#include <bits/chrono.h>
#include <chrono>
#include <ratio>

namespace minilsm {
    
class Time {
public:
    static u64 now() {
        auto now_time = std::chrono::high_resolution_clock::now();
        auto duration = now_time.time_since_epoch();
        return std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
    }
};

}

#endif