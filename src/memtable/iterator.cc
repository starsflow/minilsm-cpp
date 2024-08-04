/*
 * @Author: lxc
 * @Date: 2024-04-08 15:28:44
 * @Description: iterator implementation for memtable
 */

#include "memtable/iterator.h"

namespace minilsm {

using std::shared_ptr;
using std::make_shared;

Slice MemTableIterator::key() const {
    DCHECK(this->iterator_.good());
    return this->iterator_->kvpair[0];
}

Slice MemTableIterator::value() const {
    DCHECK(this->iterator_.good());
    return this->iterator_->kvpair[1];
}

shared_ptr<Iterator> MemTableIterator::next() {
    DCHECK(this->iterator_ != this->end_);
    auto iter_n = std::next(this->iterator_);
    return make_shared<MemTableIterator>(iter_n, this->end_);
}

bool MemTableIterator::is_valid() const {
    return (this->iterator_ != this->end_ && !this->key().empty());
}

bool MemTableIterator::operator==(const Iterator& other) const {
    auto other_cast = dynamic_cast<const MemTableIterator&>(other);
    if (!this->is_valid() && !other_cast.is_valid()) return true;
    else if (this->is_valid() || other_cast.is_valid()) return false;
    else if (!this->key().compare(other_cast.key())) return true;
    else return true;
}
} 