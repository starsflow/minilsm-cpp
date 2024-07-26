/*
 * @Author: lxc
 * @Date: 2024-04-09 23：26
 * @Description: iterators for memtable, sstable
 */
#ifndef ITERATE_H
#define ITERATE_H

#include "defs.h"
#include "slice.h"
#include <shared_mutex>

using folly::ConcurrentSkipList;
using std::shared_ptr;
using std::make_shared;
using std::shared_lock;
using std::shared_mutex;

namespace minilsm {
typedef ConcurrentSkipList<array<Slice, 2>, SliceArrayComparator> SkipListType;
typedef SkipListType::Accessor Accessor;
typedef SkipListType::iterator Iterator;
class MemTableIterator {
private:
    shared_ptr<SkipListType> map_;
    Accessor accessor_;
    Iterator iterator_;
    Iterator end_;
    shared_lock<shared_mutex>& lock_; // bind lock with iterator

public:
    MemTableIterator(shared_ptr<SkipListType> sl_ptr, shared_lock<shared_mutex>& lock) : 
        map_(sl_ptr), 
        accessor_(sl_ptr), 
        iterator_(accessor_.begin()),
        end_(accessor_.end()),
        lock_(lock) {}

    MemTableIterator(shared_ptr<SkipListType> sl_ptr, shared_lock<shared_mutex>& lock, Iterator it) : 
        map_(sl_ptr), 
        accessor_(sl_ptr), 
        iterator_(it),
        end_(accessor_.end()),
        lock_(lock) {}
    
    MemTableIterator(shared_ptr<SkipListType> sl_ptr, shared_lock<shared_mutex>& lock, Iterator it, Iterator end) : 
        map_(sl_ptr), 
        accessor_(sl_ptr), 
        iterator_(it),
        end_(end),
        lock_(lock) {}

    Slice key();

    Slice value();

    MemTableIterator next();

    MemTableIterator end();

    bool is_valid();
};

}


#endif