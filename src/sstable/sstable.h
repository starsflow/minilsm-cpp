/*
 * @Author: lxc
 * @Date: 2024-04-09 23:24:02
 * @Description: head file of sstable's implementation
 */
#ifndef SSTABLE_H
#define SSTABLE_H

#include "block/iterator.h"
#include "defs.h"
#include "block/block.h"
#include "folly/container/Access.h"
#include "mvcc/key.h"
#include "folly/hash/Checksum.h"
#include "folly/concurrency/ConcurrentHashMap.h"
#include "slice.h"
#include "util/bloom.h"
#include "util/bytes.h"
#include "util/file.h"
#include "util/queue.h"
#include <bits/types/FILE.h>
#include <cstddef>
#include <ios>
#include <memory>
#include <string>

namespace minilsm {

using std::pair;

// abstract of block
struct BlockMeta {
public:
    // offset of the block
    size_t offset;
    // first key of the block
    KeySlice first_key;
    // last key of the block
    KeySlice last_key;

public: 
    static size_t estimated_size(const vector<BlockMeta>& block_meta_list);

    static void encode_block_meta(const vector<BlockMeta>& block_meta_list, u64 max_ts, Bytes& buf);

    static pair<vector<BlockMeta>, u64> decode_block_meta(const Bytes& buf, size_t meta_offset = 0);
};

using std::ios_base;

class FileObject {
private:
    File file_;
    u64 size_;

public:
    // default mode : overwrite
    FileObject(const string& path, bool readonly) :
        file_(File(path, 
            readonly ? 
                ios_base::in : 
                ios_base::out | ios_base::trunc)),
        size_(file_.size()) {}

    FileObject(string&& input);

    Bytes read(size_t offset, size_t len);

    void write(const Bytes& buf);

    bool is_open();

    bool close();

    size_t size();
};

using std::shared_ptr;
using std::make_shared;
using std::tuple;
using BlockCache = folly::ConcurrentHashMap<size_t, shared_ptr<Block>>;

/*
 * -------------------------------------------------------------------------------------------
 * |         Block Section         |          Meta Section         |          Extra          |
 * -------------------------------------------------------------------------------------------
 * | data block | ... | data block |            metadata           | meta block offset (u32) |
 * -------------------------------------------------------------------------------------------
 */

class SSTableIterator;

class SSTable : public std::enable_shared_from_this<SSTable> {
public:
    // sstable id
    size_t id;
    // first key in sstable
    KeySlice first_key;
    // last key in sstable
    KeySlice last_key;
    // max timestamp in sstable
    u64 max_ts;

private:
    // sstable file path
    FileObject file_obj_;
    // vector of block meta
    vector<BlockMeta> block_meta_;
    // offset of block meta
    size_t block_meta_offset_;
    // bloom filter
    bloom_filter bloom_;
    // block cache
    shared_ptr<BlockCache> block_cache_;

public:
    // build sstable with block metas (without specific block) from file
    SSTable(size_t id, shared_ptr<BlockCache> cache, const string& file_path);

    SSTable(size_t id, const string& file_path, vector<BlockMeta>& meta, 
        size_t meta_offset, shared_ptr<BlockCache> cache, bloom_filter& bloom, u64 ts);

    ~SSTable();

    shared_ptr<Block> get_block(size_t block_idx);

    size_t locate_block(const KeySlice& key);

    shared_ptr<SSTableIterator> create_iterator(size_t blk_idx = 0, size_t key_idx = 0);

    size_t num_of_blocks();

    u64 table_size();

#ifdef Debug
    vector<BlockMeta>& debug_get_block_meta() { return this->block_meta_; }

    bloom_filter& debug_get_bloom() { return this->bloom_; }
#endif

private:
    shared_ptr<Block> get_block_from_encoded(size_t block_idx);
};

/*
 * sstable format:
 * -------------------------------------------------------------------------------------------
 * |         Block Section         |          Meta Section         |          Extra          |
 * -------------------------------------------------------------------------------------------
 * | data block | ... | data block |            metadata           | meta block offset (u32) |
 * -------------------------------------------------------------------------------------------
 *
 * in detail:
 *     - Block Section
 *         - data block
 *             - block data (encoded)
 *             - crc (u32)
 *     - Meta Section
 *         - meta number (u32)
 *         - metadata
 *             - meta offset (u32)
 *             - meta first key size (u16)
 *             - meta first key data
 *             - meta first timestamp (u64)
 *             - meta last key size (u16)
 *             - meta last key data
 *             - meta last key timestamp (u64)
 *         - max timestamp (u64)
 *         - crc (u32)
 *     - Extra
 *         - meta section offset (u32)
 *         - bloom filter (serialized)
 *         - bloom filter offset (u32)
 */
class SSTableBuilder {
private:
    // builder of current block
    BlockBuilder builder_;
    // first key of current block
    KeySlice first_key_;
    // last key of current block
    KeySlice last_key_;
    // byte vector of total sstable
    Bytes data_;
    // block size threshold
    size_t block_size_;
    // bloom filter for keys in the sstable
    bloom_filter bloom_;
    // max timestamp of keys in current sstable
    u64 max_ts_;

public:
    vector<BlockMeta> meta;

public:
    SSTableBuilder() = default;

    SSTableBuilder(size_t block_size, size_t estimated_key_cnt, 
        double expected_false_positive_rate);

    bool add(const KeySlice& key, const Slice& value);

    size_t estimated_size();

    shared_ptr<SSTable> build(size_t id, shared_ptr<BlockCache> block_cache, 
        const string& path);

#ifdef Debug
    KeySlice debug_get_first_key() {
        return this->first_key_;
    }

    KeySlice debug_get_last_key() {
        return this->last_key_;
    }
#endif

#ifndef Debug
private:
#endif
    void finish_block();
};

class Level : public std::enable_shared_from_this<Level> {
public:
    size_t id;

private:
    vector<shared_ptr<SSTable>> ssts_;
    vector<size_t> sst_ids_;

public:
    Level(int id, vector<shared_ptr<SSTable>>& sstables) :
            id(id),
            ssts_(sstables) {
        for (auto sst_ptr : sstables) {
            sst_ids_.push_back(sst_ptr->id);
        }
    }

    size_t num_of_ssts();

    shared_ptr<LevelIterator> scan(
        const Bound& lower = Bound(false), 
        const Bound& upper = Bound(true));

    shared_ptr<SSTable> get_sstable(size_t idx);

    size_t locate_sstable(const KeySlice& key);
};

class Level0 {
private:
    SPSC::Queue<shared_ptr<SSTable>> ssts_queue_;
    
public:
    bool insert(const shared_ptr<SSTable>& sst_ptr) {
        return this->ssts_queue_.push(sst_ptr);
    }

    bool fetch(shared_ptr<SSTable>& sst_ptr) {
        return this->ssts_queue_.pop(sst_ptr);
    }

    size_t num_of_ssts() {
        return this->ssts_queue_.size();
    }
};

}

#endif