/*
 * @Author: lxc
 * @Date: 2024-04-09 10:12:14
 * @Description: unified implementation of key/value struct
 */
#ifndef INCLUDE_SLICE_H
#define INCLUDE_SLICE_H

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>

using std::atomic;
using std::string;
using std::array;

namespace minilsm {
class Slice {
private:
    struct BaseSlice {
        size_t size;
        uint8_t* data;
        atomic<size_t> refcnt;

        BaseSlice(size_t size, uint8_t* data) : 
            size(size), 
            data(data), 
            refcnt(1) {} 
    };

    BaseSlice* ctrl_;

public:
    Slice() { ctrl_ = new BaseSlice(0, nullptr); }

    Slice(const uint8_t* src, size_t size) { ctrl_ = new BaseSlice(size, clone(src, size)); }

    // splice slices with varying size
    // total size is required in advance
    template <typename... Args>
    Slice(size_t size, Args... args) { ctrl_ = new BaseSlice(size, clone(size, args...)); }

    Slice(const string& src) { 
        auto size = src.length();
        ctrl_ = new BaseSlice(size, 
            clone(reinterpret_cast<const uint8_t*>(src.c_str()), size));
    }

    Slice(const char* src) {
        auto size = strlen(src);
        ctrl_ = new BaseSlice(size, 
            clone(reinterpret_cast<const uint8_t*>(src), size));
    }

    Slice(const Slice& src) : ctrl_(src.ctrl_) {
        if (ctrl_) { ctrl_->refcnt++; }
    }

    Slice& operator=(const Slice& src) {
        if (this != &src) {
            if (ctrl_ && --ctrl_->refcnt == 0) {
                free(ctrl_->data);
                delete ctrl_;
            }
            ctrl_ = src.ctrl_;
            if (ctrl_) { ctrl_->refcnt++; }
        }
        return *this;
    }

    ~Slice() {
        ctrl_->refcnt--;
        if (ctrl_ && ctrl_->refcnt == 0) {
            free(ctrl_->data);
            delete ctrl_;
        }
    }

    const uint8_t* data() const { return ctrl_->data; }

    size_t size() const { return ctrl_->size; }

    bool empty() const { return ctrl_->size == 0; }

    void clear() { 
        free(ctrl_->data); 
        ctrl_->size = 0; 
    }

    // compare Slices by follwoing order:
    // 1. compare size
    // 2. compare lexicograpic order
    // value returns when Slice a and b `a.compare(b)`:
    //  1: a > b
    //  0: a = b
    // -1: a < b
    int8_t compare(const Slice& other) const {
        if (ctrl_->size > other.size()) return 1;
        if (ctrl_->size < other.size()) return -1;
        auto r = memcmp(ctrl_->data, other.data(), ctrl_->size);
        return (r > 0) - (r < 0);
    }

    size_t compute_overlap(const Slice& s) const {
        size_t res = 0;
        while (true) {
            if (res >= this->ctrl_->size || res >= s.ctrl_->size) break;
            if (this->ctrl_->data[res] != s.ctrl_->data[res]) break;
            res++;
        }
        return res;
    }

    size_t debug_ref_cnt() const {
        return this->ctrl_->refcnt;
    }

private:
    uint8_t* clone(const uint8_t* src, size_t size) {
        if (!size) { return nullptr; }
        auto dst = (uint8_t* )malloc(size);
        memcpy(dst, src, size);
        return dst;
    }

    template <typename... Args>
    uint8_t* clone(size_t total_size, Args&... args) {
        if (!total_size) { return nullptr; }
        auto dst = (uint8_t*) malloc(total_size);
        multi_copy(dst, args...);
        return dst;
    }

    template <typename... Args>
    void multi_copy(uint8_t* dst, const uint8_t* src, size_t size, Args&... args) {
        assert(!(sizeof...(args) & 1));
        memcpy(dst, src, size);
        if constexpr (sizeof...(args)) {
            multi_copy(dst + size, args...);
        }
    }
};

struct SliceArrayComparator {
    int operator()(const array<Slice, 2>& ca1, const array<Slice, 2>& ca2) const { 
        return ca1[0].compare(ca2[0]) < 0;
    }
};
};

#endif