/*
 * @Author: lxc
 * @Date: 2024-07-26 16:13:39
 * @Description: block as well as block builder's implementation
 */

#include "block.h"

namespace minilsm {

Bytes Block::serialize() {
    auto buf = this->data;
    auto offsets_len = this->offsets.size();
    for (auto offset : offsets) {
        buf.push(offset, sizeof(u16));
    }
    buf.push(offsets_len, sizeof(u16));
    return buf;
}

KeySlice Block::get_first_key_from_encoded() {
    auto& buf = this->data;
    auto key_len = buf.get(0, sizeof(u16));
    return KeySlice{buf.outstream(), key_len};
}

size_t BlockBuilder::estimated_size() {
    return this->data_.size()                    /* data section */ 
        + this->offsets_.size() * sizeof(u16)    /* offset section */ 
        + sizeof(u16);                          /* extra */
}

bool BlockBuilder::add(const KeySlice& key, const Slice& value) {
    if (this->estimated_size() 
        + sizeof(u16) * 2 + key.size()      /* key_len (overlap and rest) + key */
        + sizeof(u16) + value.size()        /* value_len + value */
        + sizeof(u64)                       /* timestamp */
        + sizeof(u16)                       /* offset */ 
        > this->block_size_ 
        && !this->is_empty()) {
        return false;
    }

    auto offset = this->data_.size();
    this->offsets_.push_back(offset);
    
    auto overlap = key.compute_overlap(this->first_key_);
    this->data_.push(overlap, sizeof(u16));
    this->data_.push(key.size() - overlap, sizeof(u16));
    this->data_.instream(key.data() + overlap, key.size() - overlap);
    this->data_.push(key.get_ts(), sizeof(u64));
    this->data_.push(value.size(), sizeof(u16));
    this->data_.instream(value.data(), value.size());

    // set the first valid key as the first key
    if (this->first_key_.empty()) {
        this->first_key_ = key;
    }

    return true;
}

bool BlockBuilder::is_empty() {
    return this->offsets_.size() == 0;
}

Block BlockBuilder::build() {
    DCHECK(!this->is_empty());
    return Block{ this->data_, this->offsets_ };
}

}