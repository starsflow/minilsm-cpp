/*
 * @Author: lxc
 * @Date: 2024-04-09 23：26
 * @Description: iterator for memtable
 */
#ifndef MEMTABLE_ITERATE_H
#define MEMTABLE_ITERATE_H

#include "defs.h"
#include "memtable/memtable.h"
#include "slice.h"
#include "iterator/iterator.h"
#include <utility>

namespace minilsm {

using folly::ConcurrentSkipList;
using std::shared_ptr;

using SkipListType = ConcurrentSkipList<KVPair>;
using SkipListAccessor = SkipListType::Accessor;
using SkipListIterator = SkipListType::iterator;

class MemTableIterator : public Iterator {
private:
    const SkipListAccessor acer_;
    SkipListIterator iterator_;
    const Bound end_;

public:
    MemTableIterator(const SkipListAccessor& acer, 
            SkipListIterator& start, 
            const Bound& end = Bound(true)) : 
        acer_(acer),
        iterator_(start),
        end_(end) {}
    
    KeySlice key() const override;

    Slice value() const override;

    void next() override;

    bool is_valid() const override;

    size_t num_active_iterators() override;
};

}

#endif