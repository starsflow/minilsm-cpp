/*
 * @Author: lxc
 * @Date: 2024-09-15 16:15:04
 * @Description: test for iterator implementation
 */

#include "defs.h"
#include "iterator/iterator.h"
#include "iterator/merge.h"
#include "memtable/memtable.h"
#include "sstable/iterator.h"
#include "memtable/iterator.h"
#include "sstable/sstable.h"
#include "gtest/gtest.h"
#include <algorithm>
#include <cstddef>
#include <ctime>
#include <random>

using namespace minilsm;

#define K(key) KeySlice(std::to_string(key))
#define V(key) Slice(std::to_string(key))

class IteratorTest : public ::testing::Test {
public:
    std::string sst_dir = string(PROJECT_ROOT_PATH) + "/binary/unittest";
    std::random_device rd; 
    std::mt19937 seed;
     
public:
    void SetUp() override {
        std::filesystem::create_directories(sst_dir);
        this->seed = std::mt19937(0);
    }

    size_t generate_random_int(size_t bound) {
        std::uniform_int_distribution<> distrib(0, bound);
        return distrib(this->seed);
    }

    shared_ptr<Level> create_level(size_t level_id, const std::set<size_t>& input, size_t blk_size, size_t blk_cnt_per_sst) {
        size_t block_size = 10240;
        double expected_false_rate = 0.01;

        std::vector<shared_ptr<SSTable>> ssts;
        auto block_cache = make_shared<BlockCache>();

        SSTableBuilder builder(block_size,
            blk_size * blk_cnt_per_sst,
            expected_false_rate);
        size_t index = 0;
        for (auto key : input) {            
            builder.add(K(key), V(level_id));
            index++;
            if (index % blk_size == 0) {
                builder.finish_block();
            }
            if (index % (blk_size * blk_cnt_per_sst) == 0 || index == input.size()) {
                size_t sst_id = index / (blk_size * blk_cnt_per_sst);
                std::string sst_path = sst_dir + "/iterator-2-level-" + std::to_string(level_id) + "-sstable-" + std::to_string(sst_id) + ".sst";

                ssts.push_back(builder.build(sst_id, block_cache, sst_path));

                builder = SSTableBuilder(block_size,
                    blk_size * blk_cnt_per_sst,
                    expected_false_rate);
            }
        }

        return make_shared<Level>(level_id, ssts);
    }
};

int main() {
    ::testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}

TEST_F(IteratorTest, mergebiniterator) {
    {
        std::string sst_path = sst_dir + "/iterator-1.sst";
        size_t key_size = 512;
        size_t block_size = 1024;
        double expected_false_rate = 0.01;

        std::vector<size_t> keys(key_size);
        for (size_t i = 0; i < key_size; i++) {
            keys[i] = i * 2;
        }

        auto block_cache = make_shared<BlockCache>();
        SSTableBuilder sst_builder(
            block_size, 
            key_size, 
            expected_false_rate);

        auto mem_builder = make_shared<MemTable>(0);

        for (auto key : keys) {
            auto res =  generate_random_int(1);
            if (res) {
                mem_builder->put(K(key), K(key * 2));
            } else {
                sst_builder.add(K(key), K(key * 2));
            }
        }
        auto mem_iter = mem_builder->create_iterator();
        auto sst_iter = sst_builder.build(0, block_cache, sst_path)->create_iterator();
        auto merge_iter = make_shared<MergeBinIterator>(mem_iter, sst_iter);
        size_t idx = 0;
        while (merge_iter->is_valid()) {
            EXPECT_EQ(merge_iter->key().compare(K(keys[idx])), 0);
            EXPECT_EQ(merge_iter->value().compare(V(keys[idx] * 2)), 0);
            merge_iter->next();
            idx++;
        }
        EXPECT_EQ(idx, keys.size());
    }

    {
        std::string sst_path = sst_dir + "/iterator-2.sst";
        size_t key_size = 512;
        size_t block_size = 512;
        double expected_false_rate = 0.01;

        std::vector<size_t> keys(key_size);
        for (size_t i = 0; i < key_size; i++) {
            keys[i] = i * 3;
        }

        auto block_cache = make_shared<BlockCache>();
        SSTableBuilder sst_builder(
            block_size, 
            key_size, 
            expected_false_rate);

        auto mem_builder = make_shared<MemTable>(0);

        for (auto key : keys) {
            auto res =  generate_random_int(2);
            if (res == 2) {
                mem_builder->put(K(key), V(key * 2));
            } else if (res == 1) {
                sst_builder.add(K(key), V(key * 2));
            } else {
                mem_builder->put(K(key), V(key * 2));
                sst_builder.add(K(key), V(key * 3));
            }
        }
        auto mem_iter = mem_builder->create_iterator();
        auto sst_iter = sst_builder.build(0, block_cache, sst_path)->create_iterator();
        auto merge_iter = make_shared<MergeBinIterator>(mem_iter, sst_iter);
        int idx = 0;
        while (merge_iter->is_valid()) {
            EXPECT_EQ(merge_iter->key().compare(K(keys[idx])), 0);
            EXPECT_EQ(merge_iter->value().compare(V(keys[idx] * 2)), 0);
            merge_iter->next();
            idx++;
        }
        EXPECT_EQ(idx, keys.size());
    }
}

TEST_F(IteratorTest, mergemultiiterator) {
    {
        std::string sst_path = sst_dir + "/iterator-3.sst";
        size_t key_size = 512;
        size_t block_size = 512;
        double expected_false_rate = 0.01;

        std::vector<size_t> keys(key_size);
        for (size_t i = 0; i < key_size; i++) {
            keys[i] = i * 3;
        }

        auto block_cache = make_shared<BlockCache>();
        SSTableBuilder sst_builder(
            block_size, 
            key_size, 
            expected_false_rate);

        auto mem_builder = make_shared<MemTable>(0);

        for (auto key : keys) {
            auto res =  generate_random_int(2);
            if (res == 2) {
                mem_builder->put(K(key), V(key * 2));
            } else if (res == 1) {
                sst_builder.add(K(key), V(key * 2));
            } else {
                mem_builder->put(K(key), V(key * 2));
                sst_builder.add(K(key), V(key * 3));
            }
        }
        auto mem_iter = mem_builder->create_iterator();
        auto sst_iter = sst_builder.build(0, block_cache, sst_path)->create_iterator();
        std::vector<shared_ptr<Iterator>> iter_vec = {mem_iter, sst_iter};
        auto merge_iter = make_shared<MergeMultiIterator>(iter_vec);
        int idx = 0;
        while (merge_iter->is_valid()) {
            EXPECT_EQ(merge_iter->key().compare(K(keys[idx])), 0);
            EXPECT_EQ(merge_iter->value().compare(V(keys[idx] * 2)), 0);
            merge_iter->next();
            idx++;
        }
        EXPECT_EQ(idx, keys.size());
    }

    {
        size_t key_size = 10000;
        size_t block_size = 100;
        size_t block_cnt_per_sst = 10;
        size_t mem_key_size = 7000;
        vector<size_t> level_key_size = {5000, 6000, 7000};

        vector<size_t> keys_vec;
        for (size_t i = 0; i < key_size; i++) { keys_vec.push_back(i); }
        
        std::shuffle(keys_vec.begin(), keys_vec.end(), std::mt19937(this->rd()));
        std::set<size_t> mem_set(keys_vec.begin(), keys_vec.begin() + mem_key_size);

        vector<std::set<size_t>> sst_sets;
        for (size_t i = 0; i < level_key_size.size(); i++) {
            std::shuffle(keys_vec.begin(), keys_vec.end(), std::mt19937(this->rd()));
            sst_sets.push_back(std::set<size_t>(keys_vec.begin(), keys_vec.begin() + level_key_size[i]));
        }

        vector<shared_ptr<Iterator>> iter_vec;
        auto mem_builder = make_shared<MemTable>(0);
        for (auto key : mem_set) {
            mem_builder->put(K(key), V(-1));
        }   
        iter_vec.push_back(mem_builder->create_iterator());     
        for (size_t i = 0; i < level_key_size.size(); i++) {
            auto level_ptr = this->create_level(i, sst_sets[i], block_size, block_cnt_per_sst);
            iter_vec.push_back(level_ptr->scan());
        }

        // for (auto key : mem_set) { LOG(INFO) << "memtable key: " << key; }
        // for (size_t i = 0; i < sst_sets.size(); i++) {
        //     for (auto key : sst_sets[i]) {
        //         LOG(INFO) << i << "th sstable key: " << key;
        //     }
        // }

        auto merge_iter = make_shared<MergeMultiIterator>(iter_vec);
        while (merge_iter->is_valid()) {
            auto key_int = std::stoi(reinterpret_cast<const char*>(merge_iter->key().data()));
            if (mem_set.find(key_int) != mem_set.end()) {
                EXPECT_EQ(merge_iter->value().compare(V(-1)), 0);
            } else {
                size_t idx = 0;
                while (idx < sst_sets.size()) {
                    if (sst_sets[idx].find(key_int) != sst_sets[idx].end()) {
                        EXPECT_EQ(merge_iter->value().compare(V(idx)), 0);
                        break;
                    }
                    idx++;
                }
                EXPECT_LT(idx, sst_sets.size());
            }
            merge_iter->next();
        }
    }
}