/*
 * @Author: lxc
 * @Date: 2024-10-02 11:04:47
 * @Description: thread-safe queue
 */
#ifndef  QUEUE_H
#define QUEUE_H

#include "defs.h"
#include <atomic>
#include <cstddef>
#include <iterator>
#include <memory>
#include <utility>

namespace minilsm {

using std::vector;
using std::allocator;
using std::atomic;
using std::shared_ptr;
using std::make_shared;

namespace SPSC {

template<class T>
class Queue {
using TP = shared_ptr<T>;

private:
    size_t size_;
    vector<TP> que_; 
    
    atomic<size_t> front_;
    atomic<size_t> tail_;

public:
    Queue(size_t size) : size_(size), tail_(0), front_(0) { this->que_.reserve(size); }
    Queue(const Queue&) = delete;
    Queue& operator=(const Queue&) = delete;

    bool push(const T& val) {
        auto front = this->head_.load(std::memory_order_relaxed);
        // queue is full
        if (front == this->tail_.load(std::memory_order_acquire)) { return false; }
        // queue is not full
        this->que_[front] = std::forward<T>(make_shared<T>(val));
        front = (front + 1) % this->size_;
        this->head_.store(front, std::memory_order_release);
        return true;
    }

    bool pop(T& val) {
        auto end = this->tail_.load(std::memory_order_relaxed);
        // queue is empty
        if (end == this->head_.load(std::memory_order_acquire)) { return false; }
        // queue is not empty
        val = *std::forward<T>(this->que_[end]);
        this->tail_.store(end + 1, std::memory_order_release);
        return true;
    }

    size_t size() {
        return this->front_.load(std::memory_order_relaxed) - 
            this->tail_.load(std::memory_order_relaxed);
    }
};

}
}

#endif