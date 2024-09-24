/*
 * @Author: lxc
 * @Date: 2024-09-15 16:15:04
 * @Description: test for iterator implementation
 */

#include "defs.h"
#include "iterator/merge.h"
#include "memtable/memtable.h"
#include "sstable/iterator.h"
#include "memtable/iterator.h"
#include "gtest/gtest.h"
#include <filesystem>
#include <random>

namespace minilsm {

class IteratorTest : public ::testing::Test {
public:
    std::string sst_dir = string(PROJECT_ROOT_PATH) + "/binary/unittest";
    std::mt19937 seed;

public:
    void SetUp() override {
        std::filesystem::create_directories(sst_dir);
    }

    shared_ptr<MemTable>
};

int main() {
    ::testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}

TEST_F(IteratorTest, mergebiniterator) {
    {
    }
}

}