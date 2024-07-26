/*
 * @Author: lxc
 * @Date: 2024-04-09 10:12:14
 * @Description: unified implementation of key/value struct
 */
#ifndef INCLUDE_SLICE_H
#define INCLUDE_SLICE_H

#include <array>
#include <atomic>
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
        uint64_t size_;
        uint8_t* data_;
        atomic<uint64_t> refcnt_;

        BaseSlice(uint64_t size, uint8_t* data) : 
            size_(size), 
            data_(data), 
            refcnt_(1) {} 
    };

    BaseSlice* ctrl_;

public:
    Slice() { ctrl_ = new BaseSlice(0, nullptr); }

    Slice(const char* src, uint64_t size) { ctrl_ = new BaseSlice(size, clone(src, size)); }

    Slice(string& src) { 
        auto size = src.length();
        ctrl_ = new BaseSlice(size, clone(src.c_str(), size));
    }

    Slice(const char* src) {
        auto size = strlen(src);
        ctrl_ = new BaseSlice(size, clone(src, size));
    }

    Slice(const Slice& src) : ctrl_(src.ctrl_) {
        if (ctrl_) { ctrl_->refcnt_++; }
    }

    Slice& operator=(const Slice& src) {
        if (this != &src) {
            if (ctrl_ && --ctrl_->refcnt_ == 0) {
                free(ctrl_->data_);
                delete ctrl_;
            }
            ctrl_ = src.ctrl_;
            if (ctrl_) { ctrl_->refcnt_ ++; }
        }
        return *this;
    }

    ~Slice() {
        ctrl_->refcnt_--;
        if (ctrl_ && ctrl_->refcnt_ == 0) {
            free(ctrl_->data_);
            delete ctrl_;
        }
    }

    const uint8_t* data() const { return ctrl_->data_; }

    uint64_t size() const { return ctrl_->size_; }

    bool empty() const { return ctrl_->size_ == 0; }

    void clear() { 
        free(ctrl_->data_); 
        ctrl_->size_ = 0; 
    }

    int8_t compare(const Slice& s) const {
        if (!s.data()) return 1;
        auto min_len = std::min(ctrl_->size_, s.size());
        auto r = memcmp(ctrl_->data_, s.data(), min_len);
        if (!r) {
            if (min_len != ctrl_->size_) { r = 1; }
            else if (min_len != s.size()) { r = -1; }
        }
        return r;
    }

private:
    uint8_t* clone(const char* src, uint64_t size) {
        auto dst = (uint8_t* )malloc(size);
        memcpy(dst, src, size);
        return dst;
    }
};

struct SliceArrayComparator {
    int operator()(const array<Slice, 2>& ca1, const array<Slice, 2>& ca2) const { 
        return ca1[0].compare(ca2[0]);
    }
};
};

#endif