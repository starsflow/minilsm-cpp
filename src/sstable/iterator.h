/*
 * @Author: lxc
 * @Date: 2024-07-26 16:29:00
 * @Description: iterator for sstable
 */
#ifndef SSTABLE_ITERATOR_H
#define SSTABLE_ITERATOR_H

#include "block/block.h"
#include "block/iterator.h"
#include "defs.h"
#include "iterator/iterator.h"
#include "mvcc/key.h"
#include "slice.h"
#include "sstable/sstable.h"
#include <cstddef>
#include <type_traits>

namespace minilsm {

class LevelIterator;

class SSTableIterator : public Iterator {
private:
    shared_ptr<SSTable> table_ptr_;
    shared_ptr<BlockIterator> current_block_iter_;
    size_t current_block_idx_;
    size_t current_key_idx_;

    friend class LevelIterator;
public:
    SSTableIterator(shared_ptr<SSTable> table,
        size_t block_idx = 0, 
        size_t key_idx = 0);

    Slice value() const override;

    KeySlice key() const override;

    bool is_valid() const override;

    void next() override;

    size_t num_active_iterators() override;
};

class LevelIterator : public Iterator {
private:
    shared_ptr<Level> level_;
    // the end bound of the iterator
    // the format:
    // <sstable index in `ssts_`, 
    // block index in the sstable,
    // key index in the block>
    array<size_t, 3> end_;
    array<size_t, 3> current_;
    shared_ptr<SSTableIterator> current_sst_iter_;

public:
    LevelIterator(shared_ptr<Level> level_ptr, const array<size_t, 3>& start,
        const array<size_t, 3>& end, const Bound& start_bound);

    KeySlice key() const override;

    Slice value() const override;

    bool is_valid() const override;

    void next() override;

    size_t num_active_iterators() override;
};
}

#endif