/*
 * @Author: lxc
 * @Date: 2024-09-16 23:04:49
 * @Description: 
 */

#include "defs.h"
#include "util/skiplist.h"
#include "gtest/gtest.h"
#include "memtable/memtable.h"

struct ArrayCompare {
    bool operator()(const std::array<int, 2>& arr1, const std::array<int, 2>& arr2) {
        return arr1[0] < arr2[0];
    }
};

class SkipListTest : public testing::Test {
public:
    sl_map<int, int> slist;
public:
    void SetUp() override {
        //   << Insertion >> 
        // Insert 3 KV pairs: {0, 0}, {1, 10}, {2, 20}.
        for (int i=0; i<10; ++i) {
            slist.insert(std::make_pair(i, i*2));
        }
    }
};

int main() {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}

TEST_F(SkipListTest, SigThd) {
        //   << Point lookup >>
    for (int i=0; i<10; ++i) {
        auto itr = slist.find(i);
        EXPECT_EQ(itr->second, i * 2);
        printf("[point lookup] key: %d, value: %d\n", itr->first, itr->second);

        // Note: In `sl_map`, while `itr` is alive and holding a node
        //       in skiplist, other thread cannot erase and free the node.
        //       Same as `shared_ptr`, `itr` will automatically release
        //       the node when it is not referred anymore.
        //       But if you want to release the node before that,
        //       you can do it as follows:
        // itr = slist.end();
    }

    //   << Erase >>
    // Erase the KV pair for key 1: {1, 10}.
    slist.erase(1);

    //   << Iteration >>
    for (auto& entry: slist) {
        printf("[iteration] key: %d, value: %d\n", entry.first, entry.second);
    }
}
