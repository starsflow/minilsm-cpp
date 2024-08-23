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

namespace minilsm {

using std::vector;

/*
 * ------------------------------------------------------------------------------------------------------------------------
 * |             Data Section             |                       Offset Section                   |         Extra        |
 * ------------------------------------------------------------------------------------------------------------------------
 * | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 (2B) | Offset #2 (2B) | ... | Offset #N (2B) | num_of_elements (2B) |
 * ------------------------------------------------------------------------------------------------------------------------
 */


class Block {
public:
    // data section
    Bytes data;
    // offset section
    vector<u16> offsets;

public:
    Block(const Bytes& data, const vector<u16>& offsets) :
        data(data), offsets(offsets) {}

    // construct from Bytes
    Block(const Bytes& buf) {
        auto size = buf.size();
        auto entry_offsets_len = buf.get(size - sizeof(u16), sizeof(u16));
        auto data_end = size - sizeof(u16) - entry_offsets_len * sizeof(u16);
        for (auto addr = data_end; addr < size - sizeof(u16); addr += sizeof(u16)) {
            auto offset = buf.get(addr, sizeof(u16));
            this->offsets.push_back(offset);
        }
        
        this->data.resize(data_end);
        std::copy_n(buf.cbegin(), data_end, this->data.begin());
    }

    Bytes serialize();

    KeySlice get_first_key_from_encoded();

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
};

/*
 * -----------------------------------------------------------------------------------------------------------------------
 * |                                                   Entry #1                                                    | ... |
 * -----------------------------------------------------------------------------------------------------------------------
 * | key_len (overlap, 2B) | key_len (rest, 2B) | key (rest) | timestamp (8B) | value_len (2B) | value (value_len) | ... |
 * -----------------------------------------------------------------------------------------------------------------------
 */

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

    size_t estimated_size();

    bool add(const KeySlice& key, const Slice& value);

    bool is_empty() ;

    Block build();

    KeySlice debug_get_first_key() {
        return this->first_key_;
    }
};

}

#endif