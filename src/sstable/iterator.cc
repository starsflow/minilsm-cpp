/*
 * @Author: lxc
 * @Date: 2024-07-26 16:26:48
 * @Description: iterator implementation for sstable
 */

#include "sstable/iterator.h"
#include "sstable/sstable.h"

namespace minilsm {

SSTableIterator::SSTableIterator(shared_ptr<SSTable> table,
            size_t block_idx, 
            size_t key_idx) :
        table_ptr_(table),
        current_block_idx_(block_idx),
        current_key_idx_(key_idx) {
    this->current_block_iter_ = make_shared<BlockIterator>(
        table->get_block(this->current_block_idx_), 
        this->current_key_idx_);
}

Slice SSTableIterator::value() const {
    return this->current_block_iter_->value();
}

KeySlice SSTableIterator::key() const {
    return this->current_block_iter_->key();
}

bool SSTableIterator::is_valid() const {
    return this->current_block_idx_ < this->table_ptr_->num_of_blocks()
        && this->current_block_iter_->is_valid();
}

void SSTableIterator::next() {
    DCHECK(this->is_valid());
    this->current_block_iter_->next();
    if (!this->current_block_iter_->is_valid()) {
        this->current_block_idx_++;
        this->current_key_idx_ = 0;
        if (this->current_block_idx_ < this->table_ptr_->num_of_blocks()) {
            this->current_block_iter_ = make_shared<BlockIterator>(
                this->table_ptr_->get_block(this->current_block_idx_), 0);
        }
    } else {
        this->current_key_idx_++;
    }
}

size_t SSTableIterator::num_active_iterators() {
    return this->is_valid();
}

LevelIterator::LevelIterator(shared_ptr<Level> level_ptr, 
            const array<size_t, 3>& start,
            const array<size_t, 3>& end,
            const Bound& start_bound) : 
        level_(level_ptr),
        end_(end),
        current_(start) {
    this->current_sst_iter_ = make_shared<SSTableIterator>(
        level_ptr->get_sstable(start[0]),
        start[1],
        start[2]);
    if (start_bound.fin_ptr && 
            (this->key().compare(start_bound.fin_ptr->key) < 0 ||
            (this->key().compare(start_bound.fin_ptr->key) == 0 && 
            !start_bound.fin_ptr->contains))) {
        this->next();
    }
}

KeySlice LevelIterator::key() const {
    return this->current_sst_iter_->key();
}

Slice LevelIterator::value() const {
    return this->current_sst_iter_->value();
}

bool LevelIterator::is_valid() const {
    if (!this->current_sst_iter_->is_valid()) { return false; }
    if (this->current_[0] < this->end_[0]) { return true; }
    if (this->current_[1] < this->end_[1]) { return true; }
    if (this->current_[2] < this->end_[2]) { return true; }
    return false;
}

void LevelIterator::next() {
    DCHECK(this->is_valid());
    if (this->current_[0] == this->end_[0] &&
            this->current_[1] == this->end_[1] &&
            this->current_[2] + 1 == this->end_[2]) { 
        this->current_[2]++;
    } else {
        this->current_sst_iter_->next();
        if (!this->current_sst_iter_->is_valid()) {
            this->current_ = {this->current_[0] + 1, 0, 0};
            if (this->current_[0] < this->level_->num_of_ssts()) {
                this->current_sst_iter_ = make_shared<SSTableIterator>(
                    this->level_->get_sstable(this->current_[0]),
                    0, 0); 
            } 
        } else {
            this->current_ = {
                this->current_[0],
                this->current_sst_iter_->current_block_idx_,
                this->current_sst_iter_->current_key_idx_
            };
        }
    }
}

size_t LevelIterator::num_active_iterators() {
    return this->is_valid();
}

}