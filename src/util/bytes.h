/*
 * @Author: lxc
 * @Date: 2024-08-06 22:53:33
 * @Description: wrapper of bytes vector
 */
#ifndef BYTES_H
#define BYTES_H

#include "defs.h"
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <sys/stat.h>
#include <type_traits>
#include <variant>
#include <vector>

namespace minilsm {

using std::vector;

class Bytes {
private:
    vector<u8> data_;

public:
    template <typename T, typename = std::enable_if<std::numeric_limits<T>::is_integer>>
    void push(T value, size_t size = 1) {
        // DCHECK(sizeof(T) == size);
        for (size_t i = 0; i < size; i++) {
            this->data_.push_back(value & 0xFF);
            value = value >> 8;
        } 
    }

    void instream(const u8* is, size_t size) {
        for (size_t i = 0; i < size; i++) {
            this->data_.push_back(is[i]);
        }
    }

    auto get(size_t start, size_t size = 1) const {
        DCHECK(size == 1 || size == 2 || size == 4 || size == 8);
        DCHECK(start + size <= this->data_.size());
        u64 value = 0;
        for (size_t i = size; i > 0; i--) {
            value = value << 8;
            value |= this->data_[start + i - 1];
        }
        return value;
    }

    const u8* outstream(size_t start = 0) const {
        return this->data_.data() + start;
    }

    void reserve(size_t size) { this->data_.reserve(size); }
    
    void resize(size_t size, u8 pending = 0) { this->data_.resize(size, pending); } 

    template <typename T, typename = std::enable_if<std::numeric_limits<T>::is_integer>>
    void push(size_t start, T value, size_t size) {
        // DCHECK(sizeof(T) >= size);
        DCHECK(start < this->data_.size());
        auto origin_size = this->data_.size();
        for (size_t i = 0; i < size; i++) {
            if (start + i < origin_size) {
                this->data_[start + i] = value & 0xFF;
            } else {
                this->data_.push_back(value & 0xFF);
            }
            value = value >> 8;
        } 
    }

    void instream(size_t start, const u8* is, size_t size) {
        DCHECK(start < this->data_.size());
        auto origin_size = this->data_.size();
        for (size_t i = 0; i < size; i++) {
            if (start + i < origin_size) {
                this->data_[start + i] = is[i];
            } else {
                this->data_.push_back(is[i]);
            }
        }
    }

    auto size() const { return this->data_.size(); }

    auto begin() { return this->data_.begin(); }

    auto end() { return this->data_.end(); }

    auto cbegin() const { return this->data_.cbegin(); }

    auto cend() const { return this->data_.cend(); }

    auto data() { return this->data_.data(); }
};
}

#endif