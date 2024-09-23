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
    // end index of the iterator
    // iterator is invalid when
    // `idx_` == `end_idx`
    // -1 indicates on upper limit
    // size_t end_idx_;

    friend class LevelIterator;
    friend class SSTableIterator;
public:
    // BlockIterator(shared_ptr<Block> block_ptr,
    //         const Bound& start,
    //         const Bound& end) :
    //         block_ptr_(block_ptr),
    //         first_key_(block_ptr_->get_first_key_from_encoded()),
    //         idx_(0),
    //         end_idx_(block_ptr_->offsets.size())
    //         {
    //     DCHECK(start.compare(end));
        
    //     if (end.fin_ptr) {
    //         auto end_idx = this->locate_index(end.fin_ptr->key, this->idx_);
    //         this->seek_to(end_idx);
    //         if (this->key().compare(end.fin_ptr->key) == 0 && 
    //                 !start.fin_ptr->contains) {
    //             this->end_idx_ = end_idx;
    //         } else {
    //             this->end_idx_ = end_idx + 1;
    //         }
    //     }

    //     if (start.fin_ptr && this->first_key_.compare(start.fin_ptr->key) <= 0) {
    //         this->seek_to_key(start.fin_ptr->key);
    //         if (this->is_valid() && 
    //                 (this->key().compare(start.fin_ptr->key) == -1 ||
    //                 this->key().compare(start.fin_ptr->key) == 0 && 
    //                 !start.fin_ptr->contains)) {
    //             this->next();
    //         }
    //     } 
    // }

    BlockIterator() {}

    BlockIterator(shared_ptr<Block> block_ptr, size_t idx) : 
            block_ptr_(block_ptr),
            current_(idx),
            first_key_(block_ptr_->first_key) {}

    // BlockIterator(shared_ptr<Block> block_ptr, const KeySlice& key) :
    //     block_ptr_(block_ptr),
    //     first_key_(block_ptr_->get_first_key_from_encoded()) {
    //     this->seek_to_key(key);
    // }

    KeySlice key() const override;

    Slice value() const override;

    bool is_valid() const override;

    void next() override;
};

}

#endif