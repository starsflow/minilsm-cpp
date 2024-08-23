/*
 * @Author: lxc
 * @Date: 2024-07-26 16:29:07
 * @Description: iterator implementation for block
 */

#include "block/iterator.h"

namespace minilsm {

KeySlice BlockIterator::key() const {
    auto offset = this->block_ptr_->offsets[this->idx_];
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
    auto offset = this->block_ptr_->offsets[this->idx_];
    auto key_rest_len = this->block_ptr_->data.get(offset + sizeof(u16), sizeof(u16));
    auto value_offset = offset + sizeof(u16) + sizeof(u16) + key_rest_len + sizeof(u64);
    auto value_len = this->block_ptr_->data.get(value_offset, sizeof(u16));
    Slice value(this->block_ptr_->data.outstream(value_offset + sizeof(u16)), value_len);
    return value;
}

bool BlockIterator::is_valid() const {
    if (this->idx_ >= this->block_ptr_->offsets.size()) { 
        return false;
    }
    // auto offset = this->block_ptr_->offsets[this->idx_];
    // auto overlap_len = this->block_ptr_->data.get(offset, 2);
    // auto key_len = this->block_ptr_->data.get(offset + 2, 2) + overlap_len;
    // if (key_len == 0) return false;
    return true;
}

// seek the first valid key
void BlockIterator::seek_to_first() {
    this->seek_to(0);
    this->first_key_ = this->key();

    // first key is invalid, move to next one
    if (this->first_key_.empty()) {
        this->next();
        this->first_key_ = this->key(); 
    }
}

void BlockIterator::next() {
    DCHECK(this->is_valid());
    this->idx_++;
}

void BlockIterator::seek_to(size_t idx) {
    if (idx >= this->block_ptr_->offsets.size()) {
        return;
    }
    this->idx_ = idx;
}

// seek to the last key not larger than `key`
void BlockIterator::seek_to_key(const KeySlice& key) {
    size_t low = 0;
    size_t high = this->block_ptr_->offsets.size() - 1;
    while (low < high) {
        auto mid = low + (high - low) / 2 + 1;
        this->seek_to(mid);
        auto res = this->key().compare(key);
        if (res < 0) {
            low = mid;
        } else if (res > 0) {
            high = mid - 1;
        } else {
            return;
        }
    }
    this->seek_to(low);
}

}