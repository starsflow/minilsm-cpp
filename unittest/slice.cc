/*
 * @Author: lxc
 * @Date: 2024-07-31 14:32:21
 * @Description: test for slice
 */

#include "slice.h"
#include "gtest/gtest.h"
#include <cstring>
#include <string>

using namespace minilsm;

class SliceTest : public ::testing::Test {
public:
    void SetUp() override {}
};

int main() {
    ::testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}

TEST_F(SliceTest, Construt) {
    Slice key1("12345");
    std::string str1 = "12345";
    Slice key2(std::to_string(1000));
    std::string str2 = std::to_string(1000);
    Slice key3 = key1;
    Slice key4(key2);
    EXPECT_EQ(key1.size(), 5);
    EXPECT_EQ(key2.size(), 4);
    EXPECT_FALSE(memcmp(key1.data(), str1.c_str(), key1.size()));
    EXPECT_EQ(key1.data(), key3.data());
    EXPECT_FALSE(memcmp(key2.data(), str2.c_str(), key2.size()));
    EXPECT_EQ(key4.data(), key2.data());
}

TEST_F(SliceTest, RefCnt) {
    Slice key1("aaaaa");
    EXPECT_EQ(key1.debug_ref_cnt(), 1);
    {
        Slice key2 = key1;
        Slice key3(key1);
        EXPECT_EQ(key3.debug_ref_cnt(), 3);
        EXPECT_EQ(key1.debug_ref_cnt(), 3);
    }
    EXPECT_EQ(key1.debug_ref_cnt(), 1);
}

TEST_F(SliceTest, Cmp) {
    Slice key1("1234");
    Slice key2("12345");
    Slice key3;
    Slice key4("1234");
    EXPECT_EQ(key1.compare(key2), -1);
    EXPECT_EQ(key2.compare(key1), 1);
    EXPECT_EQ(key1.compare(key3), 1);
    EXPECT_EQ(key1.compare(key4), 0);
}