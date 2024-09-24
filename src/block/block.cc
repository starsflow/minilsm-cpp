/*
 * @Author: lxc
 * @Date: 2024-07-26 16:13:39
 * @Description: block as well as block builder's implementation
 */

#include "block.h"
#include "block/iterator.h"
#include "mvcc/key.h"
#include <cstddef>

namespace minilsm {

using std::make_shared;

Block::Block(const Bytes& buf) {
    auto size = buf.size();
    auto entry_offsets_len = buf.get(size - sizeof(u16), sizeof(u16));
    auto data_end = size - sizeof(u16) - entry_offsets_len * sizeof(u16);
    for (auto addr = data_end; addr < size - sizeof(u16); addr += sizeof(u16)) {
        auto offset = buf.get(addr, sizeof(u16));
        this->offsets.push_back(offset);
    }
    
    this->data.resize(data_end);
    std::copy_n(buf.cbegin(), data_end, this->data.begin());

    auto first_key_len = buf.get(sizeof(u16), sizeof(u16));
    this->first_key = KeySlice(buf.outstream() + sizeof(u16) + sizeof(u16), first_key_len);
    this->first_key.set_ts(buf.get(sizeof(u16) + sizeof(u16) + first_key_len, sizeof(u64)));
}

Bytes Block::serialize() {
    auto buf = this->data;
    auto offsets_len = this->offsets.size();
    for (auto offset : offsets) {
        buf.push(offset, sizeof(u16));
    }
    buf.push(offsets_len, sizeof(u16));
    return buf;
}

KeySlice Block::get_key(size_t idx) {
    DCHECK(idx < this->offsets.size());
    auto offset = this->offsets[idx];
    auto overlap_len = this->data.get(offset, sizeof(u16));
    auto rest_len = this->data.get(offset + sizeof(u16), sizeof(u16));
    auto key = KeySlice(
        overlap_len + rest_len,
        this->first_key.data(),
        overlap_len,
        this->data.outstream(offset + sizeof(u16) + sizeof(u16)),
        rest_len
    );
    key.set_ts(this->data.get(offset + sizeof(u16) + sizeof(u16) + rest_len, sizeof(u64)));
    auto& buf = this->data;
    auto key_len = buf.get(sizeof(u16), sizeof(u16));

    return key;
}

size_t Block::num_of_keys() {
    return this->offsets.size();
}

size_t Block::locate_key(const KeySlice& key, bool contains, bool start) {
    if (key.compare(this->first_key) < 0) { return 0; }
    
    size_t low = 0;
    size_t high = this->offsets.size() - 1;
    KeySlice anchor_key;
    while (low < high) {
        auto mid = low + (high - low) / 2 + 1;
        anchor_key = this->get_key(mid);
        auto res = anchor_key.compare(key);
        if (res <= 0) {
            low = mid;
        } else {
            high = mid - 1;
        } 
    }
    anchor_key = this->get_key(low);
    auto res = anchor_key.compare(key);
    DCHECK(res <= 0);
    
    return !start && (res == -1 || (res == 0 && contains)) ? 
            low + 1: 
            low;
}

size_t BlockBuilder::estimated_size() {
    return this->data_.size()                    /* data section */ 
        + this->offsets_.size() * sizeof(u16)    /* offset section */ 
        + sizeof(u16);                           /* extra */
}

bool BlockBuilder::add(const KeySlice& key, const Slice& value) {
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

    if (this->estimated_size() > this->block_size_) {
        return false;
    }

    return true;
}

shared_ptr<BlockIterator> Block::create_iterator(size_t idx) {
    return make_shared<BlockIterator>(shared_from_this(), idx);
}

bool BlockBuilder::is_empty() {
    return this->offsets_.size() == 0;
}

shared_ptr<Block> BlockBuilder::build() {
    DCHECK(!this->is_empty());
    return make_shared<Block>(this->data_, this->offsets_, this->first_key_);
}

KeySlice BlockBuilder::first_key() {
    return this->first_key_;
}
}