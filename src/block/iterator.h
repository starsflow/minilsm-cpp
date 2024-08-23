/*
 * @Author: lxc
 * @Date: 2024-07-26 16:29:07
 * @Description: 
 */
#ifndef BLOCK_ITERATOR_H
#define BLOCK_ITERATOR_H

#include "defs.h"
#include "block.h"
#include "iterator/iterator.h"
#include "mvcc/key.h"
#include <type_traits>

namespace minilsm {

using std::pair;

class BlockIterator : public Iterator {
private:
    // reference to the block
    shared_ptr<Block> block_ptr_;
    // current index of the iterator
    size_t idx_;
    // first key in the block
    KeySlice first_key_;

public:
    BlockIterator() = default;

    BlockIterator(shared_ptr<Block> block_ptr) : 
        block_ptr_(block_ptr),
        first_key_(block_ptr_->get_first_key_from_encoded()) {
        this->seek_to_first();
    }

    BlockIterator(shared_ptr<Block> block_ptr, const KeySlice& key) :
        block_ptr_(block_ptr),
        first_key_(block_ptr_->get_first_key_from_encoded()) {
        this->seek_to_key(key);
    }

    KeySlice key() const override;

    Slice value() const override;

    bool is_valid() const override;

    void next() override;

    void seek_to_first();

    void seek_to(size_t idx);

    // seek to the first key that is >= key
    void seek_to_key(const KeySlice& key);
};

}

#endif