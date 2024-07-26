/*
 * @Author: lxc
 * @Date: 2024-07-26 16:29:52
 * @Description: iterator interface
 */
#ifndef ITERATOR_ITERATOR_H
#define ITERATOR_ITERATOR_H

#include "defs.h"
#include "slice.h"

namespace minilsm {

using std::unique_ptr;

class Iterator {
public:
    virtual Slice value() = 0;

    virtual Slice key() = 0;

    virtual bool is_valid() = 0;

    virtual unique_ptr<Iterator> next() = 0;

    virtual u64 num_active_iterators() { return 1; }
};
}

#endif
