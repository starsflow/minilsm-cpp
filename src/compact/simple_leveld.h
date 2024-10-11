/*
 * @Author: lxc
 * @Date: 2024-08-28 10:45:59
 * @Description: implementation for simple leveled compaction
 */
#ifndef COMPACT_SIMPLE_LEVELED_H
#define COMPACT_SIMPLE_LEVELED_H

#include "defs.h"
#include "folly/Portability.h"
#include "iterator/merge.h"
#include "iterator/iterator.h"
#include "lsm/compact.h"
#include "lsm/lsm.h"
#include "sstable/iterator.h"
#include "sstable/sstable.h"
#include <cstddef>
#include <iterator>
#include <memory>
#include <mutex>
#include <shared_mutex>

namespace minilsm {

struct SimpleLeveledCompactionOptions : 
        CompactionOptions {
    // threshold of size ratio between
    // lower level and upper level
    size_t size_ratio_percent_threshold;
    // threshold of number of sstables 
    // in level0
    size_t sst_num_in_level0_threshold;
    // threshold of number of levels
    // without level0
    size_t levels_num_threshold;

    SimpleLeveledCompactionOptions(
            size_t size_ratio,
            size_t sst_num,
            size_t level_num) 
      : size_ratio_percent_threshold(size_ratio),
        sst_num_in_level0_threshold(sst_num),
        levels_num_threshold(level_num) {}
};

using std::vector;

struct SimpleLeveledCompactionTask :
        CompactionTask {
    // upper level in the task, i.e. newer level
    size_t upper_level_id;
    // lower level in the task, i.e. older level
    size_t lower_level_id;

    SimpleLeveledCompactionTask(
        size_t upper_level,
        size_t lower_level) :
        upper_level_id(upper_level),
        lower_level_id(lower_level) {}         
};

using std::shared_ptr;
using std::pair;
using std::set;
using std::unique_ptr;
using std::make_unique;
using std::prev;
using std::next;
using std::advance;

class SimpleLeveledCompactionController :
    public CompactionController {
private:
    SimpleLeveledCompactionOptions* options_;

public:
    // the only trigger is the size ratio of 
    // upper level size / lower level size
    unique_ptr<CompactionTask> generate_compaction_task(
            shared_ptr<LSMStorageState> snapshot) override {
        this->options_ = 
            dynamic_cast<SimpleLeveledCompactionOptions*>(&this->base_options_);

        size_t idx = 0;
        shared_ptr<Level> upper_level = nullptr;
        shared_ptr<Level> current_level = nullptr;
        for (auto level_iter = snapshot->level_ptrs_list.begin();
                level_iter != snapshot->level_ptrs_list.end();
                level_iter++) {
            DCHECK(idx < this->options_->levels_num_threshold);
            
            // prerequisite for level0 to perform compaction:
            // number of sstables is greater than threshold
            if (idx++ == 0 && 
                snapshot->level0_ptr->num_of_ssts() < 
                    this->options_->sst_num_in_level0_threshold) {
                continue;
            }
            
            current_level = *level_iter;
            // size ratio trigger
            auto size_ratio = (double)(idx == 1 ? 
                snapshot->level0_ptr->num_of_ssts() : 
                upper_level->num_of_ssts()) / 
                current_level->num_of_ssts();
            if (size_ratio * 100 < 
                    this->options_->size_ratio_percent_threshold) {
                LOG(WARNING) << 
                    "compaction triggered at level " <<
                    idx << " and " << idx + 1 <<
                    " with size ratio " << size_ratio;
                return make_unique<SimpleLeveledCompactionTask>(
                    idx - 1, 
                    idx);
            }
            upper_level = current_level;
        }
        return nullptr;
    }

    // compact two levels in compaction task into one level, 
    // which can be assigned to background thread. 
    // Note that all background threads' compaction task
    // can not overlap.
    unique_ptr<Iterator> execute_compact(
            shared_ptr<LSMStorageState> snapshot,
            const CompactionTask& compaction_task) override {
        auto& task = 
            dynamic_cast<const SimpleLeveledCompactionTask&>(compaction_task);    
        auto level_upper_offset = task.upper_level_id;      

        shared_ptr<Level> upper_level_ptr = nullptr;
        shared_ptr<Level> lower_level_ptr = nullptr;

        std::shared_lock<shared_mutex> lock(snapshot->state_lock);
        if (task.upper_level_id != -1) {
            auto upper_iter = snapshot->level_ptrs_list.begin();
            advance(upper_iter, level_upper_offset - 1);

            if (upper_iter == snapshot->level_ptrs_list.end()) {
                return nullptr;
            }
            upper_level_ptr = *upper_iter;
            lower_level_ptr = *(next(upper_iter));
        // if upper level is level0
        } else {
            // get all sstables in snapshot's level0
            vector<shared_ptr<SSTable>> sst_ptrs;
            shared_ptr<SSTable> sst_ptr;
            while (snapshot->level0_ptr->fetch(sst_ptr) && 
                    sst_ptr->max_ts >= snapshot->shoot_ts) {
                sst_ptrs.push_back(sst_ptr);
            }
            upper_level_ptr = make_shared<Level>(0, sst_ptrs);
            lower_level_ptr = *snapshot->level_ptrs_list.begin();
        }
        
        auto upper_iter = upper_level_ptr->scan();
        auto lower_iter = lower_level_ptr->scan();

        return make_unique<MergeBinIterator>(upper_iter, lower_iter);
    }

    // remove old sstables and insert compacted sstables
    // `result`. `snapshot` is the snapshot of lsm state
    // with the timestamp of generating compaction task
    void apply_compaction_result(
            shared_ptr<LSMStorageInner> inner,
            const CompactionTask& compaction_task,
            shared_ptr<Level> result) override { 
        auto& task = 
            dynamic_cast<const SimpleLeveledCompactionTask&>(compaction_task);
        auto level_upper_offset = task.upper_level_id;
        // if upper level is not level0
        if (task.upper_level_id != -1) {
            auto upper_iter = inner->state->level_ptrs_list.begin();
            advance(upper_iter, level_upper_offset - 1);

            std::unique_lock<std::shared_mutex> lock(inner->state->state_lock);
            // remove sstables in upper level, and the garage sstables
            // will be cleared with SSTable's destruction function
            *upper_iter = make_shared<Level>();
            // assign upper level with new result level
            auto lower_iter = next(upper_iter);
            *lower_iter = result;
        // if upper level is level0
        } else {
            std::unique_lock<std::shared_mutex> lock(inner->state->state_lock);

            auto lower_iter = inner->state->level_ptrs_list.begin();
            *lower_iter = result;
        }
    }
};

}

#endif