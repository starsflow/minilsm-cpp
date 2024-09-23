/*
 * @Author: lxc
 * @Date: 2024-07-26 16:13:29
 * @Description: block's head file
 */
#ifndef BLOCK_H
#define BLOCK_H

#include "defs.h"
#include "slice.h"
#include "util/bytes.h"
#include "mvcc/key.h"
#include <cstddef>
#include <memory>
#include <type_traits>

namespace minilsm {

using std::vector;
using std::shared_ptr;

/* 
 * block format:
 * ------------------------------------------------------------------------------------------------------------------------
 * |             Data Section             |                       Offset Section                   |         Extra        |
 * ------------------------------------------------------------------------------------------------------------------------
 * | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 (2B) | Offset #2 (2B) | ... | Offset #N (2B) | num_of_elements (2B) |
 * ------------------------------------------------------------------------------------------------------------------------
 */

/*
 * entry format:
 * -----------------------------------------------------------------------------------------------------------------------
 * |                                                   Entry #1                                                    | ... |
 * -----------------------------------------------------------------------------------------------------------------------
 * | key_len (overlap, 2B) | key_len (rest, 2B) | key (rest) | timestamp (8B) | value_len (2B) | value (value_len) | ... |
 * -----------------------------------------------------------------------------------------------------------------------
 */

class BlockIterator;

class Block : public std::enable_shared_from_this<Block> {
public:
    // data section
    Bytes data;
    // offset section
    vector<u16> offsets;
    // first key
    KeySlice first_key;

public:
    Block(const Bytes& data, 
            const vector<u16>& offsets, 
            const KeySlice& first_key) :
        data(data), 
        offsets(offsets), 
        first_key(first_key) {}

    // deserialize from Bytes
    Block(const Bytes& buf);

    // serialize to Bytes
    Bytes serialize();

    // get the key according to `idx`
    KeySlice get_key(size_t idx);
    
    // locate the position of the last key less or equal to `key` in the block.
    // the result will be tuned according to extra limitations such as whether 
    // the key is start/end of scanning, or the key can be included.
    size_t locate_key(const KeySlice& key, bool contains = true, bool start = true);

    // number of keys in current block
    size_t num_of_keys();

    // create a iterator starting from the `idx` key
    shared_ptr<BlockIterator> create_iterator(size_t idx = 0);

#ifdef DEBUG
    bool debug_equal(const Block& other) {
        if (this->data.size() != other.data.size()) { return false; }
        if (this->offsets.size() != other.offsets.size()) { return false; }
        for (size_t i = 0; i < this->data.size(); i++) {
            if (this->data.get(i) != other.data.get(i)) {
                return false;
            }
        }
        for (size_t i = 0; i < this->offsets.size(); i++) {
            if (this->offsets[i] != other.offsets[i]) {
                return false;
            }
        }
        return true;
    }
#endif
};

class BlockBuilder {
private:
    // offset vector before encoded
    vector<u16> offsets_;
    // block data after encoded
    Bytes data_;
    // targeted block size
    size_t block_size_;
    // first key in block
    KeySlice first_key_;

public:
    BlockBuilder(size_t block_size) : block_size_(block_size) {}

    // estimated size of current block, which can be slightly exceed 
    // the `block_size`
    size_t estimated_size();

    // add `key` and `value` pair into block until first exceed the 
    // `block_size`
    bool add(const KeySlice& key, const Slice& value);

    bool is_empty() ;

    // build a shared pointer pointing to a new block
    shared_ptr<Block> build();

    // get first key from serialized Bytes
    KeySlice first_key();
};

}

#endif