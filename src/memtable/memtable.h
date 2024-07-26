/*
 * @Author: lxc
 * @Date: 2024-04-08 15:27:23
 * @Description: memtable's head file
 */
#ifndef MEMTABLE_H
#define MEMTABLE_H

#include "defs.h"
#include "slice.h"
#include "memtable/iterator.h"
#include <variant>

using folly::ConcurrentSkipList;
using std::vector;
using std::atomic;
using std::array;
using std::shared_ptr;
using std::shared_lock;
using std::unique_lock;
using std::shared_mutex;
using std::variant;
using std::holds_alternative;
using std::nullopt;

namespace minilsm {

typedef ConcurrentSkipList<array<Slice, 2>, SliceArrayComparator> SkipListType;

typedef struct {
    bool inf; // true -> positive infinity; false -> negative infinity
} InfinBound;

typedef struct {
    Slice key;
    bool contains; // true -> do not contain the endpoint; false -> contain
} FinBound;

using Bound = variant<InfinBound, FinBound>;

class MemTable {
private:
    shared_ptr<SkipListType> map_;
    u64 id_;
    // todo
    // optional<Wal> wal_;
    atomic<u64> approximate_size_;
    // shared_mutex snapshot_mtx_; 

public:
    MemTable(u64 id) : 
        map_(SkipListType::createInstance(10)),
        id_(id),
        // wal_(nullopt),
        approximate_size_(0) {}

    // todo
    // MemTable(u64 id, string path) :
    //     id_(id),
    //     wal_(path),
    //     approximate_size_(0),
    //     map_(SkipListType::createInstance(10)) {}
        

    ~MemTable() = default;

    void create_with_wal(); // todo

    void recover_from_val(); // todo

    Slice get(Slice);

    void put(Slice, Slice);

    MemTableIterator scan(Bound& lower, Bound& upper); // todo

    void flush(); // todo

    void sync_wal(); // todo

    u64 get_id();

    u64 get_approximate_size();

    bool is_empty();
};
}

#endif