/*
 * @Author: lxc
 * @Date: 2024-04-08 15:28:44
 * @Description: iterator implementation for memtable
 */

#include "memtable/iterator.h"

namespace minilsm {

using std::unique_ptr;
using std::make_unique;

Slice MemTableIterator::key() {
    return this->iterator_->data()[0];
}

Slice MemTableIterator::value() {
    return this->iterator_->data()[1];
}

unique_ptr<Iterator> MemTableIterator::next() {
    return make_unique<MemTableIterator>(this->map_, std::next(iterator_));
}

unique_ptr<Iterator> MemTableIterator::end() {
    return make_unique<MemTableIterator>(this->map_, this->end_);
}

bool MemTableIterator::is_valid() {
    return !this->key().empty();
}
}