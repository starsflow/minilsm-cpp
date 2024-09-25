/*
 * @Author: lxc
 * @Date: 2024-04-08 22:26:36
 * @Description: test for memtable operations
 */

#include "defs.h"
#include "memtable/iterator.h"
#include "memtable/memtable.h"
#include "slice.h"
#include "gtest/gtest.h"
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

using namespace minilsm;

class MemTableTest : public ::testing::Test {
public:
    MemTable* memtable;

public:
    void SetUp() override {
        memtable = new MemTable(0);
    }
};

int main() {
    ::testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}

TEST_F(MemTableTest, SigThd) {
    bool pass = true;

    for (i32 i = 1000; i >= 0; i -= 2) {
        KeySlice key(std::to_string(i));
        Slice value(std::to_string(i * 2));
        memtable->put(key, value);
    }

    for (i32 i = 0; i < 1001; i += 2) {
        Slice value(memtable->get(std::to_string(i)));
        if(value.compare(std::to_string(i * 2))) {
            pass = false;
            LOG(INFO) << "test case : " << i << " fails";
            break;
        }
    }
    EXPECT_TRUE(pass);

    int num_cnt = 0;
    for (auto iter = memtable->create_iterator(); iter->is_valid(); iter->next()) 
        num_cnt++;
    EXPECT_EQ(num_cnt, 501);

    auto invalid_scan_1 = memtable->scan(Bound(true), Bound(false));
    auto invalid_scan_2 = memtable->scan(Bound(true), Bound(Slice(std::to_string(111)), false));
    auto invalid_scan_3 = memtable->scan(Bound(true), Bound(Slice(std::to_string(111)), true));
    auto invalid_scan_4 = memtable->scan(Bound(Slice(std::to_string(112)), false), Bound(Slice(std::to_string(111)), true));
    auto invalid_scan_5 = memtable->scan(Bound(Slice(std::to_string(111)), true), Bound(Slice(std::to_string(111)), false));
    auto invalid_scan_6 = memtable->scan(Bound(Slice(std::to_string(1003)), true), Bound(Slice(std::to_string(11111)), false));
    EXPECT_FALSE(invalid_scan_1->is_valid());
    EXPECT_FALSE(invalid_scan_2->is_valid());
    EXPECT_FALSE(invalid_scan_3->is_valid());
    EXPECT_FALSE(invalid_scan_4->is_valid());
    EXPECT_FALSE(invalid_scan_5->is_valid());
    EXPECT_FALSE(invalid_scan_6->is_valid());

    auto valid_scan_1 = memtable->scan(Bound(false), Bound(true));
    for (i32 i = 0; i < 1001; i += 2) {
        EXPECT_FALSE(valid_scan_1->key().compare(Slice(std::to_string(i))));
        valid_scan_1->next();
    }
    auto valid_scan_2 = memtable->scan();
    for (i32 i = 0; i < 1001; i += 2) {
        EXPECT_FALSE(valid_scan_2->key().compare(Slice(std::to_string(i))));
        valid_scan_2->next();
    }
    auto valid_scan_3 = memtable->scan(Bound(Slice(std::to_string(10)), false));
    for (i32 i = 12; i < 1001; i += 2) {
        EXPECT_FALSE(valid_scan_3->key().compare(Slice(std::to_string(i))));
        valid_scan_3->next();
    }
    auto valid_scan_4 = memtable->scan(Bound(Slice(std::to_string(10)), false), Bound(Slice(std::to_string(16)), false));
    for (i32 i = 12; i < 16; i += 2) {
        EXPECT_FALSE(valid_scan_4->key().compare(Slice(std::to_string(i))));
        valid_scan_4->next();
    }
    auto valid_scan_5 = memtable->scan(Bound(Slice(std::to_string(998)), true), Bound(Slice(std::to_string(11111)), false));
    for (i32 i = 998; i < 1001; i += 2) {
        EXPECT_FALSE(valid_scan_5->key().compare(Slice(std::to_string(i))));
        valid_scan_5->next();
    }
}

TEST_F(MemTableTest, MulThd) {
    bool pass = true;

    std::thread insert_after_thread ([&](){
        for (int i = 30000; i >= 0; i -= 2) {
            memtable->put(std::to_string(i), std::to_string(i * 2));
        }
    });

    std::thread insert_before_thread ([&](){
        for (int i = 29999; i >= 0; i -= 2) {
            memtable->put(std::to_string(i), std::to_string(i * 2));
        }
    });

    insert_after_thread.join();
    insert_before_thread.join();
    EXPECT_EQ(memtable->get_size(), 30001);

    i32 rewrite_thread_num = 8, get_thread_num = 8;
    std::vector<std::thread> rewrite_threads(rewrite_thread_num);
    for (i32 i = 0; i < rewrite_thread_num; i++) {
        rewrite_threads[i] = std::thread([&]() {
            for (i32 j = i; j < 30000; j += rewrite_thread_num) {
                memtable->put(std::to_string(j), std::to_string(j));
            }
        });
    }

    std::vector<std::thread> get_threads(get_thread_num);
    for (i32 i = 0; i < get_thread_num; i++) {
        get_threads[i] = std::thread([&]() {
            for (i32 j = i; j < 30000; j += get_thread_num) {
                auto res = memtable->get(std::to_string(j));
                if (res.compare(std::to_string(j)) && res.compare(std::to_string(j * 2))) {
                    LOG(INFO) << "test case " << j << " fails";
                    pass = false;
                    break;
                }
            }
        });
    }

    for (auto& thread : rewrite_threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    for (auto& thread : get_threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    EXPECT_EQ(memtable->get_size(), 30001);
    EXPECT_TRUE(pass);
}

