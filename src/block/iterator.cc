/*
 * @Author: lxc
 * @Date: 2024-07-26 16:29:07
 * @Description: iterator implementation for block
 */

#include "block/iterator.h"
#include "defs.h"

namespace minilsm {

KeySlice BlockIterator::key() const {
    auto offset = this->block_ptr_->offsets[this->current_];
    auto overlap_len = this->block_ptr_->data.get(offset, sizeof(u16));
    auto rest_len = this->block_ptr_->data.get(offset + sizeof(u16), sizeof(u16));
    auto key = KeySlice(
        overlap_len + rest_len, 
        this->first_key_.data(),
        overlap_len,
        this->block_ptr_->data.outstream(offset + sizeof(u16) * 2),
        rest_len
    );
    key.set_ts(this->block_ptr_->data.get(offset + sizeof(u16) * 2 + rest_len, sizeof(u64)));
    return key;
}

Slice BlockIterator::value() const {
    auto offset = this->block_ptr_->offsets[this->current_];
    auto key_rest_len = this->block_ptr_->data.get(offset + sizeof(u16), sizeof(u16));
    auto value_offset = offset + sizeof(u16) + sizeof(u16) + key_rest_len + sizeof(u64);
    auto value_len = this->block_ptr_->data.get(value_offset, sizeof(u16));
    Slice value(this->block_ptr_->data.outstream(value_offset + sizeof(u16)), value_len);
    return value;
}

bool BlockIterator::is_valid() const { 
    if (this->current_ >= this->block_ptr_->offsets.size()) { 
        return false;
    }
    return true;
}

void BlockIterator::next() {
    DCHECK(this->is_valid());
    this->current_++;
}

size_t BlockIterator::num_active_iterators() {
    return this->is_valid();
}

}