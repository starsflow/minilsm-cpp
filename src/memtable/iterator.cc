/*
 * @Author: lxc
 * @Date: 2024-04-08 15:28:44
 * @Description: iterator implementation for memtable
 */

#include "memtable/iterator.h"

namespace minilsm {

using std::shared_ptr;
using std::make_shared;

Slice MemTableIterator::key() {
    DCHECK(this->is_valid());
    return this->iterator_->kvpair[0];
}

Slice MemTableIterator::value() {
    DCHECK(this->is_valid());
    return this->iterator_->kvpair[1];
}

shared_ptr<Iterator> MemTableIterator::next() {
    DCHECK(this->iterator_ != this->end_);
    auto iter_n = std::next(this->iterator_);
    return make_shared<MemTableIterator>(iter_n, this->end_);
}

bool MemTableIterator::is_valid() {
    return (!this->key().empty() && this->iterator_ != this->end_);
}
}