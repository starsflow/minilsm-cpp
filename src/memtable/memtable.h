/*
 * @Author: lxc
 * @Date: 2024-04-08 15:27:23
 * @Description: memtable's head file
 */
#ifndef MEMTABLE_H
#define MEMTABLE_H

#include "defs.h"
#include "iterator/iterator.h"
#include "slice.h"
#include "mvcc/key.h"

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

using std::shared_ptr;
using std::make_shared;

struct KVPair {
    KeySlice key;
    Slice value;

    bool operator==(const KVPair& other) const {
        return this->key.compare(other.key) == 0;
    }

    bool operator<(const KVPair& other) const {
        return this->key.compare(other.key) < 0;
    }
};

using SkipListType = ConcurrentSkipList<KVPair>;

class MemTableIterator;
class MemTable {
private:
    shared_ptr<SkipListType> map_;
    u64 id_;
    // todo
    // optional<Wal> wal_;
    atomic<u64> approximate_size_;

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

    shared_ptr<MemTableIterator> scan(
        const Bound& lower = Bound(false), 
        const Bound& upper = Bound(true));

    shared_ptr<MemTableIterator> create_iterator();

    void flush(); // todo

    void sync_wal(); // todo

    u64 get_id();

    u64 get_size();

    u64 get_approximate_size();

    bool is_empty();

#ifdef Debug
    void debug_traverse() {
        SkipListType::Accessor acer(this->map_);
        for (auto iter = acer.begin(); iter != acer.end(); iter = std::next(iter)) {
            auto key = iter->key;
            auto value = iter->value;
            LOG(INFO) << key << ":" << value;
        }
    }
#endif
};
}

#endif