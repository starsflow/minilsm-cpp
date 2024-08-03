/*
 * @Author: lxc
 * @Date: 2024-04-08 15:27:23
 * @Description: memtable's head file
 */
#ifndef MEMTABLE_H
#define MEMTABLE_H

#include "defs.h"
#include "slice.h"
#include <cstdint>
#include <cstring>
#include <string>

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

struct SlicePair {
    std::array<Slice, 2> kvpair;

    bool operator==(const SlicePair& other) const {
        return this->kvpair[0].compare(other.kvpair[0]) == 0;
    }

    bool operator<(const SlicePair& other) const { 
        return kvpair[0].compare(other.kvpair[0]) < 0;
    }
};

typedef struct {
    bool inf; // true -> positive infinity; false -> negative infinity
} InfinBound;

typedef struct {
    Slice key;
    bool contains; // true -> do not contain the endpoint; false -> contain
} FinBound;

using Bound = variant<InfinBound, FinBound>;
using SkipListType = ConcurrentSkipList<SlicePair>;

class MemTableIterator;
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

    shared_ptr<MemTableIterator> jump(Bound& lower); // todo

    void flush(); // todo

    void sync_wal(); // todo

    u64 get_id();

    u64 get_approximate_size();

    bool is_empty();

    shared_ptr<MemTableIterator> begin();

    shared_ptr<MemTableIterator> end();

    shared_mutex& get_mutex();

    void debug_traverse() {
        SkipListType::Accessor acer(this->map_);
        for (auto iter = acer.begin(); iter != acer.end(); iter = std::next(iter)) {
            auto key = iter->kvpair[0];
            auto value = iter->kvpair[1];
            auto key_new = new unsigned char(key.size() + 1);
            memcpy(key_new, key.data(), key.size());
            key_new[key.size()] = '\0';
            auto value_new = new unsigned char(value.size() + 1);
            memcpy(value_new, value.data(), value.size());
            value_new[value.size()] = '\0';
            LOG(INFO) << key_new << ":" << key.size() << ":" << value_new << ":" << value.size();
        }
    }
};
}

#endif