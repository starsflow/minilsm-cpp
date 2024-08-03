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

namespace minilsm {

using folly::ConcurrentSkipList;
using std::shared_ptr;

using SkipListType = ConcurrentSkipList<SlicePair>;
using SkipListAccessor = SkipListType::Accessor;
using SkipListIterator = SkipListType::iterator;

class MemTableIterator : public Iterator {
private:
    SkipListIterator iterator_;
    SkipListIterator end_;
    // shared_lock<shared_mutex>& lock_; // bind lock with iterator

public:
    template<class SkipListIteratorType>
    MemTableIterator(SkipListIteratorType&& start, SkipListIteratorType&& end) : 
        iterator_(std::forward<SkipListIteratorType>(start)),
        end_(end) {}

    // MemTableIterator(shared_lock<shared_mutex>& mtx, ) : 
    //     iterator_(accessor_.begin()),
    //     end_(accessor_.end()),
    //     lock_(mtx) {}

    // template<class SkipListIteratorType>
    // MemTableIterator(shared_lock<shared_mutex>& mtx, SkipListIteratorType&& start) : 
    //     iterator_(std::forward<SkipListIteratorType>(start)),
    //     end_(accessor_.end()),
    //     lock_(mtx) {}
    
    // template<class SkipListIteratorType>
    // MemTableIterator(shared_lock<shared_mutex>& mtx, SkipListIteratorType&& start, SkipListIteratorType&& end) : 
    //     iterator_(std::forward<SkipListIteratorType>(start)),
    //     end_(std::forward<SkipListIteratorType>(end)),
    //     lock_(mtx) {}

    Slice key() override;

    Slice value() override;

    shared_ptr<Iterator> next() override;

    bool is_valid() override;
};

}

#endif