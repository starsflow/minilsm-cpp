/*
 * @Author: lxc
 * @Date: 2024-08-23 10:39:43
 * @Description: test for sstable operations
 */

#include "block/block.h"
#include "defs.h"
#include "mvcc/key.h"
#include "slice.h"
#include "sstable/sstable.h"
#include "sstable/iterator.h"
#include "gtest/gtest.h"
#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <random>
#include <string>

using namespace minilsm;

class SSTableTest : public ::testing::Test {
public:
    std::string sst_dir = string(PROJECT_ROOT_PATH) + "/binary/unittest";
    std::mt19937 seed;

public:
    void SetUp() override {
        std::filesystem::create_directory(sst_dir);
    }

    shared_ptr<SSTable> create_sstable(size_t level_id, size_t sst_id, size_t start, size_t span, size_t blk_size, size_t blk_cnt) {
        std::string sst_path = sst_dir + "/level-" + std::to_string(level_id) + "-sstable-" + std::to_string(sst_id) + ".sst";

        size_t block_size = 10240;
        double expected_false_rate = 0.01;

        std::vector<size_t> keys(blk_size * blk_cnt);
        for (size_t i = 0; i < blk_size * blk_cnt; i++) {
            keys[i] = (i + 1) * span + start;
        }

        auto block_cache = make_shared<BlockCache>();
        SSTableBuilder builder(
            block_size, 
            blk_size * blk_cnt, 
            expected_false_rate
        );

        for (size_t i = 0; i < blk_size * blk_cnt; i++) {
            builder.add(
                KeySlice(std::to_string(keys[i])), 
                Slice(std::to_string(keys[i] * 2))
            );
            if ((i + 1) % blk_size == 0) {
                builder.finish_block();
            }
        }

        builder.build(0, block_cache, sst_path);
        return make_shared<SSTable>(sst_id, block_cache, sst_path);
    }

    void check_iterator(const shared_ptr<LevelIterator>& iter, size_t start, size_t end, size_t span) {
        int key = start;
        while (iter->is_valid()) {
            EXPECT_EQ(iter->key().compare(KeySlice(std::to_string(key))), 0);
            iter->next();
            key += span;
        }
        EXPECT_EQ(key - span, end);
    }
};  

int main() {
    ::testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}

TEST_F(SSTableTest, sstable) {
    {
        std::string sst_path = sst_dir + "/sstable-1.sst";

        auto block_cache = make_shared<BlockCache>();
        SSTableBuilder builder(10, 1024, 0.01);

        for (size_t i = 0; i < 1000; i++) {
            builder.add(
                KeySlice(std::to_string(i)), 
                Slice(std::to_string(i * 2))
            );
        }

        builder.build(0, block_cache, sst_path);

        block_cache->clear();
        SSTable sstable(0, block_cache, sst_path);
        EXPECT_EQ(sstable.num_of_blocks(), sstable.debug_get_block_meta().size());
        for (size_t i = 0; i < sstable.num_of_blocks(); i++) {
            auto block_ptr = sstable.get_block(i);
            KeySlice first_key = block_ptr->first_key;
            EXPECT_EQ(first_key.compare(sstable.debug_get_block_meta()[i].first_key), 0);
        }

        EXPECT_EQ(sstable.first_key.compare(KeySlice(std::to_string(0))), 0);
        EXPECT_EQ(sstable.last_key.compare(KeySlice(std::to_string(999))), 0);
    }

    {
        std::string sst_path = sst_dir + "/sstable-2.sst";

        auto block_cache = make_shared<BlockCache>();
        SSTableBuilder builder(10240, 1024, 0.01);

        for (size_t i = 0; i < 100; i++) {
            builder.add(
                KeySlice(std::to_string(i)), 
                Slice(std::to_string(i * 2))
            );
        }

        builder.build(0, block_cache, sst_path);

        // block_cache->clear();
        SSTable sstable(0, block_cache, sst_path);
        EXPECT_EQ(sstable.num_of_blocks(), sstable.debug_get_block_meta().size());
        for (size_t i = 0; i < sstable.num_of_blocks(); i++) {
            auto block_ptr = sstable.get_block(i);
            KeySlice first_key = block_ptr->first_key;
            EXPECT_EQ(first_key.compare(sstable.debug_get_block_meta()[i].first_key), 0);
        }

        EXPECT_EQ(sstable.first_key.compare(KeySlice(std::to_string(0))), 0);
        EXPECT_EQ(sstable.last_key.compare(KeySlice(std::to_string(99))), 0);
    }

    {
        std::string sst_path = sst_dir + "/sstable-3.sst";
        size_t key_size = 512;
        size_t block_size = 1024;
        double expected_false_rate = 0.01;

        std::vector<size_t> keys(key_size);
        for (size_t i = 0; i < key_size; i++) {
            keys[i] = i * 2;
        }

        auto block_cache = make_shared<BlockCache>();
        SSTableBuilder builder(
            block_size, 
            key_size, 
            expected_false_rate
        );

        for (auto key : keys) {
            builder.add(
                KeySlice(std::to_string(key)), 
                Slice(std::to_string(key * 2))
            );
        }

        builder.build(0, block_cache, sst_path);

        SSTable sstable(0, block_cache, sst_path);
        EXPECT_EQ(sstable.num_of_blocks(), sstable.debug_get_block_meta().size());

        size_t hit_cnt = 0;
        for (size_t i = 0; i < key_size * 2; i++) {
            auto key = KeySlice(std::to_string(i));
            if (sstable.debug_get_bloom().contains(key.data(), key.size())) {
                hit_cnt++;
            }
            auto block_idx = sstable.locate_block(key);
            DCHECK(block_idx < sstable.debug_get_block_meta().size());
            auto block = sstable.get_block(block_idx);
            EXPECT_NE(block->first_key.compare(key), 1);
        }
        EXPECT_GE(hit_cnt, key_size);
        EXPECT_LE((double)(hit_cnt - keys.size()) / (keys.size() * 2), expected_false_rate);

        for (size_t i = 0; i < sstable.num_of_blocks(); i++) {
            auto block_ptr = sstable.get_block(i);
            KeySlice first_key = block_ptr->first_key;
            EXPECT_EQ(first_key.compare(sstable.debug_get_block_meta()[i].first_key), 0);
        }

        EXPECT_EQ(sstable.first_key.compare(KeySlice(std::to_string(0))), 0);
        EXPECT_EQ(sstable.last_key.compare(KeySlice(std::to_string((key_size - 1) * 2))), 0);
    }

    {
        size_t sst_cnt = 10;
        size_t start = 0;
        size_t key_span = 5;
        size_t blk_size = 10;
        size_t blk_cnt_per_sst = 10;

        vector<shared_ptr<SSTable>> ssts;
        for (size_t i = 0; i < sst_cnt; i++) {
            ssts.push_back(this->create_sstable(1, i, start, key_span, blk_size, blk_cnt_per_sst));
            start += key_span * blk_size * blk_cnt_per_sst;
        }

        auto level_ptr = make_shared<Level>(0, ssts);
        
        auto out_of_low_bound_key = KeySlice(std::to_string(2));
        auto low_bound_key = KeySlice(std::to_string(5));
        auto min_key_1 = KeySlice(std::to_string(500));
        auto mid_key_2 = KeySlice(std::to_string(505));
        auto mid_key_3 = KeySlice(std::to_string(506));
        auto up_bound_key = KeySlice(std::to_string(5000));
        auto out_of_up_bound_key = KeySlice(std::to_string(5001));

        EXPECT_EQ(level_ptr->locate_sstable(out_of_low_bound_key), 0);
        EXPECT_EQ(level_ptr->locate_sstable(low_bound_key), 0);
        EXPECT_EQ(level_ptr->locate_sstable(min_key_1), 0);
        EXPECT_EQ(level_ptr->locate_sstable(mid_key_2), 1);
        EXPECT_EQ(level_ptr->locate_sstable(mid_key_3), 1);
        EXPECT_EQ(level_ptr->locate_sstable(up_bound_key), 9);
        EXPECT_EQ(level_ptr->locate_sstable(out_of_up_bound_key), 9);
    }
}

TEST_F(SSTableTest, iterator) {
    {
        std::string sst_path = sst_dir + "/sstable-4.sst";
        size_t key_size = 512;
        size_t block_size = 1024;
        double expected_false_rate = 0.01;

        std::vector<size_t> keys(key_size);
        for (size_t i = 0; i < key_size; i++) {
            keys[i] = i * 2;
        }

        auto block_cache = make_shared<BlockCache>();
        SSTableBuilder builder(
            block_size, 
            key_size, 
            expected_false_rate
        );
        for (auto key : keys) {
            builder.add(
                KeySlice(std::to_string(key)), 
                Slice(std::to_string(key * 2))
            );
        }
        builder.build(0, block_cache, sst_path);

        auto sst_ptr =  make_shared<SSTable>(0, block_cache, sst_path);
        EXPECT_EQ(sst_ptr->num_of_blocks(), sst_ptr->debug_get_block_meta().size());

        auto iter = sst_ptr->create_iterator();
        int idx = 0;

        while (iter->is_valid()) {
            EXPECT_EQ(iter->key().compare(KeySlice(std::to_string(keys[idx]))), 0);
            EXPECT_EQ(iter->value().compare(Slice(std::to_string(keys[idx] * 2))), 0);
            iter->next();
            idx++;
        }
        EXPECT_EQ(idx, key_size);
    }

    {
        std::string sst_path = sst_dir + "/sstable-5.sst";
        size_t key_size = 100;
        size_t block_size = 10240;
        double expected_false_rate = 0.01;

        std::vector<size_t> keys(key_size);
        for (size_t i = 0; i < key_size; i++) {
            keys[i] = (i + 1) * 5;
        }

        auto block_cache = make_shared<BlockCache>();
        SSTableBuilder builder(
            block_size, 
            key_size, 
            expected_false_rate
        );

        for (size_t i = 0; i < key_size; i++) {
            builder.add(
                KeySlice(std::to_string(keys[i])), 
                Slice(std::to_string(keys[i] * 2))
            );
            if ((i + 1) % 10 == 0) {
                builder.finish_block();
            }
        }

        builder.build(0, block_cache, sst_path);

        auto sst_ptr =  make_shared<SSTable>(0, block_cache, sst_path);
        EXPECT_EQ(sst_ptr->num_of_blocks(), sst_ptr->debug_get_block_meta().size());

        auto out_of_low_bound_key = KeySlice(std::to_string(2));
        auto low_bound_key = KeySlice(std::to_string(5));
        auto min_key_1 = KeySlice(std::to_string(54));
        auto mid_key_2 = KeySlice(std::to_string(55));
        auto mid_key_3 = KeySlice(std::to_string(56));
        auto up_bound_key = KeySlice(std::to_string(500));
        auto out_of_up_bound_key = KeySlice(std::to_string(501));

        EXPECT_EQ(sst_ptr->locate_block(out_of_low_bound_key), 0);
        EXPECT_EQ(sst_ptr->locate_block(low_bound_key), 0);
        EXPECT_EQ(sst_ptr->locate_block(min_key_1), 0);
        EXPECT_EQ(sst_ptr->locate_block(mid_key_2), 1);
        EXPECT_EQ(sst_ptr->locate_block(mid_key_3), 1);
        EXPECT_EQ(sst_ptr->locate_block(up_bound_key), 9);
        EXPECT_EQ(sst_ptr->locate_block(out_of_up_bound_key), 9);
    }

    {
        size_t sst_cnt = 10;
        size_t start = 0;
        size_t key_span = 5;
        size_t blk_size = 10;
        size_t blk_cnt_per_sst = 10;

        vector<shared_ptr<SSTable>> ssts;
        for (size_t i = 0; i < sst_cnt; i++) {
            auto sst_ptr = this->create_sstable(2, i, start, key_span, blk_size, blk_cnt_per_sst);
            ssts.emplace_back(sst_ptr);
            start += key_span * blk_size * blk_cnt_per_sst;
        }

        auto level_ptr = make_shared<Level>(0, ssts);
        level_ptr->get_sstable(0);
        
        size_t out_of_low_bound_key = 2;
        size_t low_bound_key = 5;
        size_t mid_key_1 = 500;
        size_t mid_key_2 = 505;
        size_t mid_key_3 = 506;
        size_t up_bound_key = 5000;
        size_t out_of_up_bound_key = 5001;

#define K(key) KeySlice(std::to_string(key))

        auto neg_inf = Bound(false);
        auto pos_inf = Bound(true);
        auto out_of_low_bound_contains = Bound(K(out_of_low_bound_key), true);
        auto out_of_low_bound_not_contains = Bound(K(out_of_low_bound_key), false);
        auto low_bound_contains = Bound(K(low_bound_key), true);
        auto low_bound_not_contains = Bound(K(low_bound_key), false);
        auto mid_key_1_contains = Bound(K(mid_key_1), true);
        auto mid_key_1_not_contains = Bound(K(mid_key_1), false);
        auto mid_key_2_contains = Bound(K(mid_key_2), true);
        auto mid_key_2_not_contains = Bound(K(mid_key_2), false);
        auto mid_key_3_contains = Bound(K(mid_key_3), true);
        auto mid_key_3_not_contains = Bound(K(mid_key_3), false);
        auto up_bound_contains = Bound(K(up_bound_key), true);
        auto up_bound_not_contains = Bound(K(up_bound_key), false);
        auto out_of_up_bound_contains = Bound(K(out_of_up_bound_key), true);
        auto out_of_up_bound_not_contains = Bound(K(out_of_up_bound_key), false);

        auto iter = level_ptr->scan(neg_inf, pos_inf);
        check_iterator(iter, 5, 5000, 5);
        iter = level_ptr->scan(out_of_low_bound_contains, out_of_up_bound_contains);
        check_iterator(iter, 5, 5000, 5);
        iter = level_ptr->scan(out_of_low_bound_not_contains, out_of_up_bound_not_contains);
        check_iterator(iter, 5, 5000, 5);
        iter = level_ptr->scan(low_bound_contains, up_bound_contains);
        check_iterator(iter, 5, 5000, 5);
        iter = level_ptr->scan(low_bound_not_contains, up_bound_not_contains);
        check_iterator(iter, 10, 4995, 5);
        iter = level_ptr->scan(mid_key_1_not_contains, mid_key_2_not_contains);
        check_iterator(iter, 505, 500, 5);
        iter = level_ptr->scan(mid_key_1_not_contains, out_of_up_bound_contains);
        check_iterator(iter, 505, 5000, 5);

        iter = level_ptr->scan(mid_key_1_contains, mid_key_1_contains);
        check_iterator(iter, 500, 500, 5);
        iter = level_ptr->scan(mid_key_1_not_contains, mid_key_1_contains);
        EXPECT_FALSE(iter);
        iter = level_ptr->scan(mid_key_1_contains, mid_key_1_not_contains);
        EXPECT_FALSE(iter);
        iter = level_ptr->scan(neg_inf, neg_inf);
        EXPECT_FALSE(iter);
        iter = level_ptr->scan(pos_inf, pos_inf);
        EXPECT_FALSE(iter);
        iter = level_ptr->scan(pos_inf, mid_key_1_contains);
        EXPECT_FALSE(iter);
        iter = level_ptr->scan(mid_key_1_contains, neg_inf);
        EXPECT_FALSE(iter);
    }
}
