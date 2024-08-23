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
        auto serialized = block.serialize();
        Block new_block(serialized);

        EXPECT_TRUE(builder.debug_get_first_key().compare(first_key) >= 0);
        EXPECT_FALSE(builder.is_empty());
        EXPECT_TRUE(block.debug_equal(new_block));
    }

    {
        BlockBuilder builder(10000);
        std::uniform_int_distribution<> lens(0, 10);

        KeySlice first_key(generate_random_string(lens(this->seed)));
        builder.add(first_key, Slice(generate_random_string(lens(this->seed))));

        for (size_t i = 0; i < 10000; i++) {
            builder.add(
                KeySlice(generate_random_string(lens(this->seed))), 
                Slice(generate_random_string(lens(this->seed)))
            );
        }

        auto block = builder.build();
        auto serialized = block.serialize();
        Block new_block(serialized);

        EXPECT_TRUE(builder.estimated_size() <= 10000);
        EXPECT_TRUE(builder.debug_get_first_key().compare(first_key) >= 0);
        EXPECT_FALSE(builder.is_empty());
        EXPECT_TRUE(block.debug_equal(new_block));
    }
}

TEST_F(BlockTest, iterator) {
    {
        BlockBuilder builder(10000);
        std::vector<size_t> lens(100);
        std::iota(lens.begin(), lens.end(), 0);
        std::vector<std::pair<KeySlice, Slice>> slices;

        for (size_t i = 0; i < lens.size(); i++) {
            KeySlice key(generate_random_string(lens[i]));
            Slice value(generate_random_string(lens[i]));
            if (!key.empty()) {
                slices.emplace_back(key, value);
            }

            builder.add(key, value);
        }

        auto block = builder.build();
        auto serialized = block.serialize();
        Block new_block(serialized);

        BlockIterator iter(std::make_shared<Block>(std::move(new_block)));
        
        int idx = 0;
        for (iter.seek_to_first(); iter.is_valid(); iter.next()) {
            EXPECT_EQ(iter.key().compare(slices[idx].first), 0);
            EXPECT_EQ(iter.value().compare(slices[idx++].second), 0);
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
        auto serialized = block.serialize();
        Block new_block(serialized);

        BlockIterator iter(std::make_shared<Block>(Block(serialized)));
        
        iter.seek_to_key(KeySlice("0588"));
        EXPECT_EQ(iter.key().compare(KeySlice("0588")), 0);
    
        iter.seek_to_key(KeySlice("0589"));
        EXPECT_EQ(iter.key().compare(KeySlice("0588")), 0);
        iter.next();
        EXPECT_EQ(iter.key().compare(KeySlice("0590")), 0);

        iter.seek_to_first();
        EXPECT_EQ(iter.key().compare(KeySlice("0000")), 0);

        iter.seek_to_key(KeySlice("111111"));
        EXPECT_EQ(iter.key().compare(KeySlice("0998")), 0);
    }
}