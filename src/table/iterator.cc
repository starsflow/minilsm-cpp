/*
 * @Author: lxc
 * @Date: 2024-04-08 15:28:44
 * @Description: 
 */

#include "iterator.h"
#include <iterator>

namespace minilsm {
Slice MemTableIterator::key() {
    return this->iterator_->data()[0];
}

Slice MemTableIterator::value() {
    return this->iterator_->data()[1];
}

MemTableIterator MemTableIterator::next() {
    return MemTableIterator(this->map_, this->lock_, std::next(iterator_));
}

MemTableIterator MemTableIterator::end() {
    return MemTableIterator(this->map_, this->lock_, this->end_);
}

bool MemTableIterator::is_valid() {
    return !this->key().empty();
}
}