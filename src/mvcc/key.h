/*
 * @Author: lxc
 * @Date: 2024-08-07 10:34:22
 * @Description: slice wrapper for mvcc with timestamp
 */
#ifndef KEY_H
#define KEY_H

#include "slice.h"
#include "defs.h"

namespace minilsm {

class KeySlice : public Slice {
private:
    u64 ts_;

public:
    using Slice::Slice;

    KeySlice(const Slice& slice) : Slice(slice) {}
 
    u64 get_ts() const;

    void set_ts(u64 ts);
};

}

#endif