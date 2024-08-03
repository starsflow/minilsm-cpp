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

class MemTableTest : public ::testing::Test {
public:
    minilsm::MemTable* memtable;

public:
    void SetUp() override {
        memtable = new minilsm::MemTable(0);
        for (i32 i = 1000; i >= 0; i -= 1) {
            minilsm::Slice key(std::to_string(i));
            minilsm::Slice value(std::to_string(i * 2));
            LOG(INFO) << i << ":" << key.size() << ":" << 2 * i << ":" << value.size() << ":" << 
            (int32_t)key.compare(minilsm::Slice("100"));
            memtable->put(key, value);
        }
        memtable->debug_traverse();
    }
};

int main() {
    ::testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}

TEST_F(MemTableTest, Get) {
    bool pass = true;
    for (i32 i = 0; i < 1001; i += 1) {
        minilsm::Slice value(memtable->get(std::to_string(i)));
        LOG(INFO) << value.data();
        if(value.compare(std::to_string(i * 2))) {
            pass = false;
            LOG(INFO) << i << ":" << value.data();
            break;
        }
    }
    EXPECT_TRUE(pass);
}
