/*
 * @Author: lxc
 * @Date: 2024-08-18 15:35:22
 * @Description: merge multiple iterators
 */
#ifndef ITERATOR_MERGE_H
#define ITERATOR_MERGE_H

#include "defs.h"
#include "iterator/iterator.h"
#include "mvcc/key.h"
#include <cstddef>
#include <memory>
#include <queue>

namespace minilsm {

using std::priority_queue;
using std::vector;
using std::make_shared;

class MergeBinIterator : public Iterator {
private:
    shared_ptr<Iterator> a_ptr_;
    shared_ptr<Iterator> b_ptr_;
    bool choose_a_;

public:
    MergeBinIterator(shared_ptr<Iterator> a, shared_ptr<Iterator> b);

    KeySlice key() const override;

    Slice value() const override;

    bool is_valid() const override;

    void next() override;

    size_t num_active_iterators() override;

private:
    bool choose_a();

    void skip_b();

};

struct HeapWrapper {
    size_t idx;
    shared_ptr<Iterator> iterator = nullptr;
};

// min heap
struct HeapComparator {
    bool operator()(const HeapWrapper& lhs, const HeapWrapper& rhs) {
        auto res = lhs.iterator->key().compare(rhs.iterator->key());
        if (res) return res > 0;
        else return lhs.idx > rhs.idx;
    }
};

class MergeMultiIterator : public Iterator {
private:
    priority_queue<HeapWrapper, vector<HeapWrapper>, HeapComparator> iters_;
    HeapWrapper current_;
    size_t num_active_iter_;

public:
    // the order of iterators in `iters` need to meet that:
    // the fronter the iterator in `iters`, the newer of the
    // data in sstable corresponding to iterator
    MergeMultiIterator(const vector<shared_ptr<Iterator>>& iters);

    KeySlice key() const override;

    Slice value() const override;

    bool is_valid() const override;

    void next() override;

    size_t num_active_iterators() override;
};

}

#endif