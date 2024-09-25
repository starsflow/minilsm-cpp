/*
 * @Author: lxc
 * @Date: 2024-07-26 16:29:52
 * @Description: iterator interface
 */
#ifndef ITERATOR_ITERATOR_H
#define ITERATOR_ITERATOR_H

#include "defs.h"
#include "mvcc/key.h"

namespace minilsm {

using std::shared_ptr;

class Iterator {
public:
    virtual Slice value() const = 0;

    virtual KeySlice key() const = 0;

    // mark whether the iter reach the end
    virtual bool is_valid() const = 0;

    // find next valid position or the end
    virtual void next() = 0;

    virtual u64 num_active_iterators() { return 1; }
};
}

#endif
