/*
 * @Author: lxc
 * @Date: 2024-04-09 23:23:56
 * @Description: sstable's implementation
 */

#include "sstable/sstable.h"
#include "sstable/iterator.h"

namespace minilsm {

size_t BlockMeta::estimated_size(const vector<BlockMeta>& block_meta_list) {
    size_t estimated_size = sizeof(u32); // number of blocks
    for (auto& meta : block_meta_list) {
        estimated_size += 
            sizeof(u32) + // size of offset
            sizeof(u16) + // size of (first) key size
            meta.first_key.size() + // size of first key
            sizeof(u64) + // size of (first) key timestamp
            sizeof(u16) + // size of (last) key size
            meta.last_key.size() + // size of last key
            sizeof(u64); // size of (last) key timestamp
    }
    estimated_size += sizeof(u64); // max timestamp
    estimated_size += sizeof(u32); // checksum
    return estimated_size;
}

void BlockMeta::encode_block_meta(const vector<BlockMeta>& block_meta_list, u64 max_ts, Bytes& buf) {
    auto size_prev = buf.size();
    
    buf.push(block_meta_list.size(), sizeof(u32)); // size of block_meta size
    size_t size_curr = 0;
    for (auto& meta : block_meta_list) {
        buf.push(meta.offset, sizeof(u32)); size_curr += sizeof(u32);
        buf.push(meta.first_key.size(), sizeof(u16)); size_curr += sizeof(u16);
        buf.instream(meta.first_key.data(), meta.first_key.size()); size_curr += meta.first_key.size();
        buf.push(meta.first_key.get_ts(), sizeof(u64)); size_curr += sizeof(u64);
        buf.push(meta.last_key.size(), sizeof(u16)); size_curr += sizeof(u16);
        buf.instream(meta.last_key.data(), meta.last_key.size());  size_curr += meta.last_key.size();
        buf.push(meta.last_key.get_ts(), sizeof(u64)); size_curr += sizeof(u64);
    }
    buf.push(max_ts, sizeof(u64)); 
    size_curr += sizeof(u64);

    auto checksum_crc = folly::crc32(
        buf.outstream() + size_prev + sizeof(u32) /* start from the first block_meta */, 
        size_curr /* all block_metas as well as max_ts */);
    buf.push(checksum_crc, sizeof(u32));
}

pair<vector<BlockMeta>, u64> BlockMeta::decode_block_meta(const Bytes& buf, size_t meta_offset) {
    pair<vector<BlockMeta>, u64> res;

    auto num = buf.get(meta_offset, sizeof(u32));
    auto idx = sizeof(u32) + meta_offset;
    for (size_t i = 0; i < num; i++) {
        auto offset = buf.get(idx, sizeof(u32)); idx += sizeof(u32);
        auto first_key_len = buf.get(idx, sizeof(u16)); idx += sizeof(u16);
        auto first_key = KeySlice{buf.outstream(idx), first_key_len}; idx += first_key_len;
        first_key.set_ts(buf.get(idx, sizeof(u64))); idx += sizeof(u64);
        auto last_key_len = buf.get(idx, sizeof(u16)); idx += sizeof(u16);
        auto last_key = KeySlice{buf.outstream(idx), last_key_len}; idx += last_key_len;
        last_key.set_ts(buf.get(idx, sizeof(u64))); idx += sizeof(u64);
        res.first.emplace_back(BlockMeta{offset, first_key, last_key});
    }
    
    auto max_ts = buf.get(idx, sizeof(u64)); idx += sizeof(u64);
    res.second = max_ts;

    auto checksum_crc = folly::crc32(
        buf.outstream() + meta_offset + sizeof(u32), 
        idx - meta_offset - sizeof(u32));
    DCHECK(checksum_crc == buf.get(idx, sizeof(u32)));

    return res;
}

Bytes FileObject::read(size_t offset, size_t len) {
    Bytes buf = file_.read(offset, len);
    return buf;
}

void FileObject::write(const Bytes& buf) {
    if (file_.write(buf.outstream(), buf.size())) {
        size_ += buf.size();
    }
    DCHECK((size_t)file_.size() == buf.size());
}

bool FileObject::is_open() { return this->file_.is_open(); }

bool FileObject::close() { return this->file_.close(); }

size_t FileObject::size() { return this->size_; }

SSTable::SSTable(size_t id, shared_ptr<BlockCache> cache, const string& file_path) :
        file_obj_(FileObject(file_path, true)), block_cache_(cache) {
    auto len = file_obj_.size();
    auto bloom_offset = file_obj_.
        read(len - sizeof(u32), sizeof(u32)).
        get(0, sizeof(u32));
    bloom_filter bloom(file_obj_.read(bloom_offset, len - sizeof(u32) - bloom_offset));

    auto meta_offset = file_obj_.
        read(bloom_offset - sizeof(u32), sizeof(u32)).
        get(0, sizeof(u32));
    auto meta_buf = file_obj_.read(meta_offset, bloom_offset - sizeof(u32) - meta_offset);
    auto pair = BlockMeta::decode_block_meta(meta_buf);

    this->block_meta_ = std::move(pair.first);
    this->block_meta_offset_ = meta_offset;
    this->bloom_ = std::move(bloom);

    this->max_ts = pair.second;
    this->first_key = this->block_meta_.begin()->first_key;
    this->last_key = this->block_meta_.rbegin()->last_key;
}

SSTable::SSTable(size_t id, const string& file_path, vector<BlockMeta>& meta, 
    size_t meta_offset, shared_ptr<BlockCache> cache, bloom_filter& bloom, u64 ts) :
    id(id), 
    first_key(meta.begin()->first_key), 
    last_key(meta.rbegin()->last_key), 
    max_ts(ts),
    file_obj_(FileObject(file_path, true)),
    block_meta_(meta), 
    block_meta_offset_(meta_offset),
    bloom_(bloom),
    block_cache_(cache) {}

shared_ptr<Block> SSTable::get_block(size_t block_idx) {
    auto res = this->block_cache_->find(block_idx);
    if (res != this->block_cache_->end()) {
        return res->second;
    } else {
        auto block_ptr = get_block_from_encoded(block_idx);
        this->block_cache_->assign(block_idx, block_ptr);
        return block_ptr;
    }
}

size_t SSTable::locate_block(const KeySlice& key) {
    if (key.compare(this->block_meta_[0].first_key) < 0) { return  0; }
    
    size_t low = 0;
    size_t high = this->block_meta_.size() - 1;
    while (low < high) {
        auto mid = low + (high - low) / 2 + 1;
        auto& anchor_block = this->block_meta_[mid];
        auto res = anchor_block.first_key.compare(key);
        if (res <= 0) {
            low = mid;
        } else {
            high = mid - 1;
        }
    }

    return low;
}

shared_ptr<SSTableIterator> SSTable::create_iterator(size_t blk_idx, size_t key_idx) {
    return make_shared<SSTableIterator>(shared_from_this(), blk_idx, key_idx);
}

size_t SSTable::num_of_blocks() { return this->block_meta_.size(); }

u64 SSTable::table_size() { return this->file_obj_.size(); }

shared_ptr<Block> SSTable::get_block_from_encoded(size_t block_idx) {
    DCHECK(block_idx < this->block_meta_.size());
    auto offset = this->block_meta_[block_idx].offset;
    auto offset_end = (block_idx == this->block_meta_.size() - 1) ?
        this->block_meta_offset_ : this->block_meta_[block_idx + 1].offset;
    
    auto raw_block = this->file_obj_.read(offset, offset_end - offset - sizeof(u32));
    auto checksum_crc_stored = this->file_obj_.read(offset_end - sizeof(u32), sizeof(u32)).get(0, sizeof(u32));

    auto blk_ptr = make_shared<Block>(raw_block);
    auto checksum_crc = folly::crc32(raw_block.data(), raw_block.size());
    DCHECK(checksum_crc == checksum_crc_stored);

    return blk_ptr;
}

SSTableBuilder::SSTableBuilder(size_t block_size, 
            size_t estimated_key_cnt, 
            double expected_false_positive_rate) :
        builder_(BlockBuilder(block_size)),
        first_key_(),
        last_key_(),
        data_(),
        block_size_(block_size),
        max_ts_(0) {
    bloom_parameters param;
    param.projected_element_count = estimated_key_cnt;
    param.false_positive_probability = expected_false_positive_rate;
    param.compute_optimal_parameters();
    bloom_ = bloom_filter(param);
}

bool SSTableBuilder::add(const KeySlice& key, const Slice& value) {
    // block is empty, mark as first key
    if (this->first_key_.empty()) {
        this->first_key_ = key;
    }

    if (key.get_ts() > this->max_ts_) {
        this->max_ts_ = key.get_ts();
    }

    this->bloom_.insert(key.data(), key.size());
    this->last_key_ = key;

    // block is full, add the key then 
    // switch to the new block 
    if (!this->builder_.add(key, value)) {
        this->finish_block();
        return false;
    }
    return true;
}

size_t SSTableBuilder::estimated_size() {
    return this->data_.size();
}

SSTable SSTableBuilder::build(
        size_t id, 
        shared_ptr<BlockCache> block_cache, 
        const string& path) {
    if (!this->last_key_.empty()) {
        this->finish_block();
    }
    
    auto& buf = this->data_;
    auto meta_offset = buf.size();

    buf.reserve(
        buf.size() + // block section size
        BlockMeta::estimated_size(this->meta) +  // meta section size
        sizeof(u32) + // meta offset size
        this->bloom_.estimated_size() + // bloom data size 
        sizeof(u32) // bloom offset size
    );

    /******************** Meta Section ********************/
    BlockMeta::encode_block_meta(this->meta, this->max_ts_, buf);      
    /******************** Meta Section ********************/

    /******************** Extra Section ********************/
    // meta offset
    buf.push(meta_offset, sizeof(u32));
    auto bloom_offset = buf.size();
    this->bloom_.serialize(buf);
    // bloom offset
    buf.push(bloom_offset, sizeof(u32));
    /******************** Extra Section ********************/

    FileObject file(path, false);
    if (file.is_open()) {
        file.write(buf);
        file.close();
    }

    return SSTable {
        id,
        path,
        this->meta,
        meta_offset,
        block_cache,
        this->bloom_,
        this->max_ts_
    };
}

void SSTableBuilder::finish_block() {
    auto size_prev = this->data_.size();
    auto encoded_block = this->builder_.build()->serialize();
    auto checksum_crc = 
        folly::crc32(encoded_block.data(), encoded_block.size());

    this->data_.reserve(size_prev + encoded_block.size() + sizeof(u32));
    this->data_.instream(encoded_block.outstream(), encoded_block.size());
    this->data_.push(checksum_crc, sizeof(u32));
    this->meta.emplace_back(
        BlockMeta{
            size_prev, 
            this->builder_.first_key().clone(), 
            this->last_key_.clone()
        }
    );

    this->builder_ = BlockBuilder(this->block_size_);
    this->last_key_.clear();
}

shared_ptr<LevelIterator> Level::scan(const Bound& start, const Bound& end) {
    if (!start.compare(end)) { return nullptr; }

    auto level_size = this->num_of_ssts();
    auto last_sst_size = this->ssts_[level_size - 1]->num_of_blocks();
    auto last_blk_size = 
        this->ssts_[level_size - 1]->get_block(last_sst_size - 1)->num_of_keys();
    array<size_t, 3> start_idx = {0};
    array<size_t, 3> end_idx = {
        level_size,
        last_sst_size,
        last_blk_size
    };

    if (start.fin_ptr) {
        start_idx[0] = this->locate_sstable(start.fin_ptr->key);
        start_idx[1] = this->ssts_[start_idx[0]]->locate_block(start.fin_ptr->key);
        start_idx[2] = this->ssts_[start_idx[0]]->get_block(start_idx[1])->locate_key(
            start.fin_ptr->key, 
            start.fin_ptr->contains, 
            true);
    } else if (start.infin_ptr->inf) {
        start_idx = end_idx;
    } 

    if (end.fin_ptr) {
        end_idx[0] = this->locate_sstable(end.fin_ptr->key);
        end_idx[1] = this->ssts_[end_idx[0]]->locate_block(end.fin_ptr->key);
        end_idx[2] = this->ssts_[end_idx[0]]->get_block(end_idx[1])->locate_key(
            end.fin_ptr->key, 
            end.fin_ptr->contains, 
            false);
    } 
    return make_shared<LevelIterator>(shared_from_this(), start_idx, end_idx, start);
}

shared_ptr<SSTable> Level::get_sstable(size_t idx) {
    DCHECK(idx < this->ssts_.size());
    return this->ssts_[idx];
}

size_t Level::locate_sstable(const KeySlice& key) {
    if (key.compare(this->ssts_[0]->first_key) < 0) { return 0; }
    
    size_t low = 0;
    size_t high = this->ssts_.size() - 1;

    while (low < high) {
        auto mid = low + (high - low) / 2 + 1;
        auto& anchor_sst = this->ssts_[mid];
        auto res = anchor_sst->first_key.compare(key);
        if (res <= 0) {
            low = mid;
        } else {
            high = mid - 1;
        }
    }

    return low;
}

size_t Level::num_of_ssts() { return this->ssts_.size(); }

}