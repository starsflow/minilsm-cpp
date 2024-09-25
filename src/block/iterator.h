/*
 * @Author: lxc
 * @Date: 2024-07-26 16:29:07
 * @Description: iterator for block
 */
#ifndef BLOCK_ITERATOR_H
#define BLOCK_ITERATOR_H

#include "defs.h"
#include "block.h"
#include "iterator/iterator.h"
#include "mvcc/key.h"

namespace minilsm {

using std::pair;

class SSTableIterator;
class LevelIterator;

class BlockIterator : public Iterator {
private:
    // reference to the block
    shared_ptr<Block> block_ptr_;
    // current index of the iterator
    size_t current_;
    // first key in the block
    KeySlice first_key_;

    friend class LevelIterator;
    friend class SSTableIterator;
public:
    BlockIterator() {}

    BlockIterator(shared_ptr<Block> block_ptr, size_t idx) : 
            block_ptr_(block_ptr),
            current_(idx),
            first_key_(block_ptr_->first_key) {}

    KeySlice key() const override;

    Slice value() const override;

    bool is_valid() const override;

    void next() override;

    size_t num_active_iterators() override;
};

}

#endif