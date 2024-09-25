/*
 * @Author: lxc
 * @Date: 2024-08-20 20:06:40
 * @Description: implementation of merging multiple iterators
 */

#include "iterator/merge.h"
#include "iterator/iterator.h"

namespace minilsm {

MergeBinIterator::MergeBinIterator(shared_ptr<Iterator> a, shared_ptr<Iterator> b) :
    a_ptr_(a), b_ptr_(b) {
    this->skip_b();
    this->choose_a_ = this->choose_a();
}

bool MergeBinIterator::choose_a() {
    if (!a_ptr_->is_valid()) return false;
    if (!b_ptr_->is_valid()) return true;
    return a_ptr_->key().compare(b_ptr_->key()) < 0;
}

void MergeBinIterator::skip_b() {
    if (this->a_ptr_->is_valid() && this->b_ptr_->is_valid()
        && !this->a_ptr_->key().compare(this->b_ptr_->key())) {
        this->b_ptr_->next();
    }
}

KeySlice MergeBinIterator::key() const {
    if (this->choose_a_) {
        DCHECK(this->a_ptr_->is_valid());
        return this->a_ptr_->key();
    } else {
        DCHECK(this->b_ptr_->is_valid());
        return this->b_ptr_->key();
    }
}

Slice MergeBinIterator::value() const {
    if (this->choose_a_) {
        return this->a_ptr_->value();
    } else {
        return this->b_ptr_->value();
    }
}

bool MergeBinIterator::is_valid() const {
    if (this->choose_a_) {
        return this->a_ptr_->is_valid();
    } else {
        return this->b_ptr_->is_valid();
    }
}

void MergeBinIterator::next() {
    if (this->choose_a_) {
        this->a_ptr_->next();
    } else {
        this->b_ptr_->next();
    }
    this->skip_b();
    this->choose_a_ = this->choose_a();
}

size_t MergeBinIterator::num_active_iterators() {
    return this->a_ptr_->num_active_iterators() + this->b_ptr_->num_active_iterators();
}

MergeMultiIterator::MergeMultiIterator(const vector<shared_ptr<Iterator>>& iters) {
    for (size_t i = 0; i < iters.size(); i++) {
        if (iters[i]->is_valid()) {
            this->num_active_iter_++;
            this->iters_.push(
                    HeapWrapper{i, iters[i]}
            );
        }
    }

    this->current_ = this->iters_.top();
    this->iters_.pop();
}

KeySlice MergeMultiIterator::key() const {
    return this->current_.iterator->key();
}

Slice MergeMultiIterator::value() const {
    return this->current_.iterator->value();
}

bool MergeMultiIterator::is_valid() const {
    return this->current_.iterator->is_valid() || iters_.size();
}

void MergeMultiIterator::next() {
    DCHECK(this->is_valid());

    auto current_key = this->current_.iterator->key();
    this->current_.iterator->next();
    if (this->current_.iterator->is_valid()) {
        this->iters_.push(this->current_);
    } else {
        this->num_active_iter_--;
    }

    while (true) {
        if (this->iters_.empty()) { break; }

        auto top = this->iters_.top();
        if (!current_key.compare(top.iterator->key())) {
            this->iters_.pop();
            top.iterator->next();
            if (top.iterator->is_valid()) {
                this->iters_.push(top);
            } else {
                this->num_active_iter_--;
            }
        } else {
            DCHECK(current_key.compare(top.iterator->key()) < 0);
            break;
        }
    }

    if (!this->iters_.empty()) {
        this->current_ = this->iters_.top();
        this->iters_.pop();
    }
}

size_t MergeMultiIterator::num_active_iterators() {
    return this->num_active_iter_;
}

}