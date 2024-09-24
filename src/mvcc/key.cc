/*
 * @Author: lxc
 * @Date: 2024-08-07 10:34:27
 * @Description: implementation for KeySlice
 */

#include "key.h"

namespace minilsm {

u64 KeySlice::get_ts() const {
    return this->ts_;
}

void KeySlice::set_ts(u64 ts) {
    this->ts_ = ts;
}

}