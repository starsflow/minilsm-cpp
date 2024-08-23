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
        // LOG(INFO) << i << ":" << key.size() << ":" << 2 * i << ":" << value.size() << ":" << (int32_t)key.compare(Slice("100"));
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
    for (auto iter = memtable->begin(); iter->is_valid(); iter->next()) 
        num_cnt++;
    EXPECT_EQ(num_cnt, 501);

    auto bnd_1 = memtable->jump(InfinBound{false});
    EXPECT_TRUE(!bnd_1->key().empty() && !bnd_1->key().compare("0"));
    // auto bnd_2 = memtable->jump(InfinBound{true});
    // EXPECT_TRUE(bnd_2->key().empty());
    auto bnd_3 = memtable->jump(FinBound{std::to_string(0), false});
    EXPECT_TRUE(!bnd_3->key().empty() && !bnd_3->key().compare("2"));
    auto bnd_4 = memtable->jump(FinBound{std::to_string(0), true});
    EXPECT_TRUE(!bnd_4->key().empty() && !bnd_4->key().compare("0"));
    auto bnd_5 = memtable->jump(FinBound{std::to_string(5), false});
    EXPECT_TRUE(!bnd_5->key().empty() && !bnd_5->key().compare("6"));
    auto bnd_6 = memtable->jump(FinBound{std::to_string(5), true});
    EXPECT_TRUE(!bnd_6->key().empty() && !bnd_6->key().compare("6"));
    auto bnd_7 = memtable->jump(FinBound{std::to_string(1000), false});
    EXPECT_TRUE(!bnd_7->is_valid());
    auto bnd_8 = memtable->jump(FinBound{std::to_string(1000), true});
    EXPECT_TRUE(!bnd_8->key().empty() && !bnd_8->key().compare("1000"));
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

