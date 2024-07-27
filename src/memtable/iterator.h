/*
 * @Author: lxc
 * @Date: 2024-04-09 23：26
 * @Description: iterator for memtable
 */
#ifndef MEMTABLE_ITERATE_H
#define MEMTABLE_ITERATE_H

#include "defs.h"
#include "slice.h"
#include "iterator/iterator.h"
#include <memory>
#include <shared_mutex>

namespace minilsm {

using folly::ConcurrentSkipList;
using std::shared_ptr;
using std::shared_lock;
using std::shared_mutex;
using std::unique_ptr;

typedef ConcurrentSkipList<array<Slice, 2>, SliceArrayComparator> SkipListType;
typedef SkipListType::Accessor SkipListAccessor;
typedef SkipListType::iterator SkipListIterator;

class MemTableIterator : public Iterator {
private:
    shared_ptr<SkipListType> map_;
    SkipListAccessor accessor_;
    SkipListIterator iterator_;
    SkipListIterator end_;
    shared_lock<shared_mutex>& lock_; // bind lock with iterator

public:
    MemTableIterator(shared_ptr<SkipListType> sl_ptr, shared_lock<shared_mutex>& mtx) : 
        map_(sl_ptr), 
        accessor_(sl_ptr), 
        iterator_(accessor_.begin()),
        end_(accessor_.end()),
        lock_(mtx) {}

    template<class SkipListIteratorType>
    MemTableIterator(shared_ptr<SkipListType> sl_ptr, shared_lock<shared_mutex>& mtx, SkipListIteratorType&& start) : 
        map_(sl_ptr), 
        accessor_(sl_ptr), 
        iterator_(std::forward<SkipListIteratorType>(start)),
        end_(accessor_.end()),
        lock_(mtx) {}
    
    template<class SkipListIteratorType>
    MemTableIterator(shared_ptr<SkipListType> sl_ptr, shared_lock<shared_mutex>& mtx, SkipListIteratorType&& start, SkipListIteratorType&& end) : 
        map_(sl_ptr), 
        accessor_(sl_ptr), 
        iterator_(std::forward<SkipListIteratorType>(start)),
        end_(std::forward<SkipListIteratorType>(end)),
        lock_(mtx) {}

    Slice key() override;

    Slice value() override;

    unique_ptr<Iterator> next() override;

    unique_ptr<Iterator> end();

    bool is_valid() override;
};

}

#endif