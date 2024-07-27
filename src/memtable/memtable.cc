/*
 * @Author: lxc
 * @Date: 2024-04-08 15:28:13
 * @Description: memtable's implementation
 */

#include "memtable.h"
#include "fmt/format.h"
#include "memtable/iterator.h"
#include "slice.h"

namespace minilsm {

using std::array;

Slice MemTable::get(Slice key) {
    SkipListType::Accessor acer(this->map_);
    auto arr = array<Slice, 2>{key, Slice()};
    auto res = acer.lower_bound(arr);
    if (res != acer.end() && key.compare((*res)[0])) return (*res)[1];
    return Slice();
}

void MemTable::put(Slice key, Slice value) { // todo : return status
    unique_lock<shared_mutex> mtx_w(this->snapshot_mtx_);
    auto estimated_size = key.size() + value.size();
    auto arr = array<Slice, 2>{key, value};

    SkipListType::Accessor acer(this->map_);
    acer.add(arr);

    this->approximate_size_ += estimated_size;
    // todo
    // wal
}

MemTableIterator MemTable::scan(Bound& lower, Bound& upper) {
    shared_lock<shared_mutex> mtx_r(this->snapshot_mtx_);
    SkipListType::Accessor acer(this->map_);
    SkipListType::iterator iter_s = acer.begin(), iter_e = acer.end();
    
    auto ibptr_l = std::get_if<InfinBound>(&lower);
    auto ibptr_u = std::get_if<InfinBound>(&upper);

    FinBound* fb_l_ptr = nullptr;
    FinBound* fb_u_ptr = nullptr;

    if (ibptr_l) {
        DCHECK(!ibptr_l->inf);
    } else {
        fb_l_ptr = &std::get<FinBound>(lower);
        iter_s = acer.lower_bound(std::move(array<Slice, 2>{fb_l_ptr->key, Slice()}));
        if (iter_s.good() && !(*iter_s)[0].compare(fb_l_ptr->key) && !fb_l_ptr->contains) {
            iter_s = std::next(iter_s);
        }
    }

    if (ibptr_u) {
        DCHECK(ibptr_u->inf);
    } else {
        fb_u_ptr = &std::get<FinBound>(lower);
        if (fb_l_ptr) { 
            DCHECK(fb_l_ptr->key.compare(fb_u_ptr->key) == 1);
        }
        iter_e = acer.lower_bound(std::move(array<Slice, 2>{fb_l_ptr->key, Slice()}));
        if (iter_e.good() && !(*iter_e)[0].compare(fb_u_ptr->key) && fb_u_ptr->contains) {
            iter_e = std::next(iter_e);
        }
    }

    return MemTableIterator(this->map_, mtx_r, iter_s, iter_e);
}

void MemTable::flush() {}

u64 MemTable::get_id() { return this->id_; }

u64 MemTable::get_approximate_size() { return this->approximate_size_; }

bool MemTable::is_empty() { return this->map_->empty(); }

}