/*
 * @Author: lxc
 * @Date: 2024-08-20 15:15:41
 * @Description: test for block operations
 */
 
#include "defs.h"
#include "block/block.h"
#include "block/iterator.h"
#include "mvcc/key.h"
#include "slice.h"
#include "gtest/gtest.h"
#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>

using namespace minilsm;

class BlockTest : public ::testing::Test {
public:
    const std::string char_list = "0123456789qwertyuiopasdfghjklzxcvbnm";
    std::mt19937 seed;

public:
    void SetUp() override {
        seed = std::mt19937(0);
    }

    std::string generate_random_string(size_t size) {
        std::string str = "";
        if (!size) {
            return str;
        }
        std::uniform_int_distribution<> distrib(0, this->char_list.size() - 1);
        for (size_t i = 0; i < size; i++) {
            int num_rand = distrib(this->seed);
            str += this->char_list[num_rand];
        }
        return str;
    }

    size_t generate_random_int(size_t bound) {
        std::uniform_int_distribution<> distrib(0, bound);
        return distrib(this->seed);
    }

    std::string generate_ordered_string(size_t size) {
        DCHECK(size < char_list.size());
        return char_list.substr(0, size);
    }
};

int main() {
    ::testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}

TEST_F(BlockTest, builder) {
    {
        BlockBuilder builder(1000);     
        std::vector<size_t> lens(10);
        std::iota(lens.begin(), lens.end(), 0);
        std::shuffle(lens.begin(), lens.end(), this->seed);
        
        auto str = generate_random_string(lens[0]);
        KeySlice first_key(str);
        builder.add(first_key, Slice(generate_random_string(2)));

        for (size_t i = 1; i < lens.size(); i++) {
            builder.add(
                KeySlice(generate_random_string(lens[i])), 
                Slice(generate_random_string(2))
            );
        }

        auto block = builder.build();
        auto serialized = block->serialize();
        Block new_block(serialized);

        EXPECT_TRUE(builder.first_key().compare(first_key) >= 0);
        EXPECT_FALSE(builder.is_empty());
        EXPECT_TRUE(block->debug_equal(new_block));
    }

    {
        BlockBuilder builder(10000);
        std::uniform_int_distribution<> lens(0, 10);

        KeySlice first_key(generate_random_string(lens(this->seed)));
        builder.add(first_key, Slice(generate_random_string(lens(this->seed))));

        for (size_t i = 0; i < 10000; i++) {
            auto res = builder.add(
                KeySlice(generate_random_string(lens(this->seed))), 
                Slice(generate_random_string(lens(this->seed)))
            );
            if (!res) {
                break;
            }
        }

        auto block = builder.build();
        auto serialized = block->serialize();
        Block new_block(serialized);

        EXPECT_TRUE(builder.first_key().compare(first_key) >= 0);
        EXPECT_FALSE(builder.is_empty());
        EXPECT_TRUE(block->debug_equal(new_block));
    }
}

TEST_F(BlockTest, iterator) {
    {
        BlockBuilder builder(102400);
        std::vector<size_t> keys;
        for (int i = 0; i < 20; i++) {
            keys.push_back(this->generate_random_int(100));
        }
        sort(keys.begin(), keys.end());

        for (auto key : keys) {
            KeySlice k(std::to_string(key));
            Slice v(std::to_string(key * 2));
            builder.add(k, v);
        }

        auto block = builder.build();
        auto iter = block->create_iterator();
        int idx = 0;
        while (iter->is_valid()) {
            EXPECT_EQ(iter->key().compare(KeySlice(std::to_string(keys[idx]))), 0);
            iter->next();
            idx++;
        }
    }

    {
        BlockBuilder builder(102400);
        std::vector<std::pair<KeySlice, Slice>> slices;

        for (size_t i = 1; i <= 10; i++) {
            KeySlice key(std::to_string(i * 5));
            Slice value(std::to_string(i));
            slices.emplace_back(key, value);
            builder.add(key, value);
        }
        auto block = builder.build();
        auto iter = block->create_iterator();
        while (iter->is_valid()) {
            iter->next();
        }

        auto out_of_low_bound_key = KeySlice(std::to_string(2));
        auto low_bound_key = KeySlice(std::to_string(5));
        auto min_key_1 = KeySlice(std::to_string(10));
        auto mid_key_2 = KeySlice(std::to_string(9));
        auto mid_key_3 = KeySlice(std::to_string(11));
        auto up_bound_key = KeySlice(std::to_string(50));
        auto out_of_up_bound_key = KeySlice(std::to_string(51));
        EXPECT_EQ(block->locate_key(out_of_low_bound_key), 0);

        EXPECT_EQ(block->locate_key(low_bound_key, true, true), 0);
        EXPECT_EQ(block->locate_key(low_bound_key, true, false), 1);
        EXPECT_EQ(block->locate_key(low_bound_key, false, true), 0);
        EXPECT_EQ(block->locate_key(low_bound_key, false, false), 0);

        EXPECT_EQ(block->locate_key(min_key_1, true, true), 1);
        EXPECT_EQ(block->locate_key(min_key_1, true, false), 2);
        EXPECT_EQ(block->locate_key(min_key_1, false, true), 1);
        EXPECT_EQ(block->locate_key(min_key_1, false, false), 1);

        EXPECT_EQ(block->locate_key(mid_key_2, true, true), 0);
        EXPECT_EQ(block->locate_key(mid_key_2, true, false), 1);
        EXPECT_EQ(block->locate_key(mid_key_2, false, true), 0);
        EXPECT_EQ(block->locate_key(mid_key_2, false, false), 1);

        EXPECT_EQ(block->locate_key(mid_key_3, true, true), 1);
        EXPECT_EQ(block->locate_key(mid_key_3, true, false), 2);
        EXPECT_EQ(block->locate_key(mid_key_3, false, true), 1);
        EXPECT_EQ(block->locate_key(mid_key_3, false, false), 2);
        
        EXPECT_EQ(block->locate_key(up_bound_key, true, true), 9);
        EXPECT_EQ(block->locate_key(up_bound_key, true, false), 10);
        EXPECT_EQ(block->locate_key(up_bound_key, false, true), 9);
        EXPECT_EQ(block->locate_key(up_bound_key, false, false), 9);
        
        EXPECT_EQ(block->locate_key(out_of_up_bound_key, true, true), 9);
        EXPECT_EQ(block->locate_key(out_of_up_bound_key, true, false), 10);
        EXPECT_EQ(block->locate_key(out_of_up_bound_key, false, true), 9);
        EXPECT_EQ(block->locate_key(out_of_up_bound_key, false, false), 10);
    }

    {
        BlockBuilder builder(10000);
        std::vector<size_t> lens(100);
        std::iota(lens.begin(), lens.end(), 0);
        std::vector<std::pair<KeySlice, Slice>> slices;

        for (size_t i = 0; i < lens.size(); i++) {
            KeySlice key(generate_random_string(lens[i] + 1));
            Slice value(generate_random_string(lens[i] + 1));
            if (!key.empty()) {
                slices.emplace_back(key, value);
            }

            builder.add(key, value);
        }

        auto block = builder.build();
        auto serialized = block->serialize();
        auto new_block = std::make_shared<Block>(serialized);

        auto iter = new_block->create_iterator();
        
        int idx = 0;
        while (iter->is_valid()) {
            EXPECT_EQ(iter->key().compare(slices[idx].first), 0);
            EXPECT_EQ(iter->value().compare(slices[idx++].second), 0);
            iter->next();
        }
    }

    {
        BlockBuilder builder(10000);
        for (int i = 0; i < 1000; i += 2) {
            char buff[5];
            sprintf(buff, "%0*d", 4, i);
            builder.add(KeySlice(reinterpret_cast<u8*>(buff), 4), Slice());
        }
        
        auto block = builder.build();
        auto serialized = block->serialize();
        auto new_block = std::make_shared<Block>(serialized);

        auto key_1 = KeySlice("0588");  
        auto idx_1 = new_block->locate_key(key_1);
        auto iter_1 = new_block->create_iterator(idx_1);
        EXPECT_EQ(key_1.compare(iter_1->key()), 0);

        auto key_2 = KeySlice("0589");
        auto key_3 = KeySlice("0590");
        auto key_7 = KeySlice("0588");
        auto idx_2 = new_block->locate_key(key_2);
        auto iter_2 = new_block->create_iterator(idx_2);
        EXPECT_EQ(key_7.compare(iter_2->key()), 0);
        iter_2->next();
        EXPECT_EQ(key_3.compare(iter_2->key()), 0);

        auto key_4 = KeySlice("0000");
        auto iter_3 = new_block->create_iterator();
        EXPECT_EQ(key_4.compare(iter_3->key()), 0);

        auto key_5 = KeySlice("111111");
        auto key_6 = KeySlice("0998");
        auto idx_3 = new_block->locate_key(key_5);
        auto iter_4 = new_block->create_iterator(idx_3);
        EXPECT_EQ(key_6.compare(iter_4->key()), 0);
    }
}