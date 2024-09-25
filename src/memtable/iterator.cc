/*
 * @Author: lxc
 * @Date: 2024-04-08 15:28:44
 * @Description: iterator implementation for memtable
 */

#include "memtable/iterator.h"
#include "mvcc/key.h"
#include <iterator>

namespace minilsm {

KeySlice MemTableIterator::key() const {
    DCHECK(this->iterator_.good());
    return KeySlice(this->iterator_->key);
}

Slice MemTableIterator::value() const {
    DCHECK(this->iterator_.good());
    return this->iterator_->value;
}

void MemTableIterator::next() {
    DCHECK(this->iterator_ != this->acer_.end());
    this->iterator_ = std::next(this->iterator_);
}

bool MemTableIterator::is_valid() const {
    if (this->iterator_ == this->acer_.end()) { return false; }
    auto end_ptr = this->end_.fin_ptr;
    if (!end_ptr) { return true; }
    auto cmp_res = this->key().compare(end_ptr->key);
    if (cmp_res == -1) { return true; } 
    else if (cmp_res == 0 && end_ptr->contains) { return true; } 
    else { return false; }
}

size_t MemTableIterator::num_active_iterators() {
    return this->is_valid();
}

} 