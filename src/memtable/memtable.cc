/*
 * @Author: lxc
 * @Date: 2024-04-08 15:28:13
 * @Description: memtable's implementation
 */

#include "memtable.h"
#include "iterator/iterator.h"
#include "memtable/iterator.h"
#include "slice.h"

namespace minilsm {

using std::shared_ptr;
using std::make_shared;

Slice MemTable::get(Slice key) {
    SkipListType::Accessor acer(this->map_);
    auto res = acer.lower_bound({key, Slice()});
    if (res != acer.end() && !key.compare(res->key)) return res->value;
    return Slice();
}

void MemTable::put(Slice key, Slice value) { // todo : return status
    // unique_lock<shared_mutex> mtx_w(this->snapshot_mtx_);
    auto estimated_size = key.size() + value.size();
    
    SkipListType::Accessor acer(this->map_);
    acer.add({key, value});
    this->approximate_size_ += estimated_size;
    // todo
    // wal
}

shared_ptr<MemTableIterator> MemTable::scan(const Bound& start, const Bound& end) {
    SkipListType::Accessor acer(this->map_);
    SkipListType::iterator start_iter = acer.begin(), end_iter = acer.end();

    if (!start.compare(end)) { return make_shared<MemTableIterator>(acer, end_iter); }

    if (start.fin_ptr) {
        start_iter =  
            acer.lower_bound(KVPair{start.fin_ptr->key, Slice()});
        if (start_iter.good() && 
                (start_iter->key.compare(start.fin_ptr->key) ||
                !start.fin_ptr->contains)) {
            start_iter = std::next(start_iter);
        }
    } 

    return make_shared<MemTableIterator>(acer, start_iter, end);
}

shared_ptr<MemTableIterator> MemTable::create_iterator() { 
    SkipListType::Accessor acer(this->map_);
    SkipListType::iterator iter = acer.begin();
    return make_shared<MemTableIterator>(acer, iter);
}

void MemTable::flush() {}

u64 MemTable::get_id() { return this->id_; }

u64 MemTable::get_size() { return this->map_->size(); }

u64 MemTable::get_approximate_size() { return this->approximate_size_; }

bool MemTable::is_empty() { return this->map_->empty(); }

}