/*
 * @Author: lxc
 * @Date: 2024-09-01 15:50:38
 * @Description: 
 */
#ifndef LSM_COMPACT_H
#define LSM_COMPACT_H

#include "defs.h"
#include "lsm/lsm.h"
#include "sstable/sstable.h"

namespace minilsm {

struct CompactionTask {
    virtual ~CompactionTask() = default;
};

struct CompactionOptions {
    virtual ~CompactionOptions() = default;
};

using std::unique_ptr;
using std::make_unique;

class CompactionController {
protected:
    CompactionOptions base_options_;

public:
    CompactionController(const CompactionOptions& opts) :
        base_options_(opts) {}
    
    virtual unique_ptr<CompactionTask>
    generate_compaction_task(shared_ptr<LSMStorageState>) = 0;

    virtual void apply_compaction_result(
        shared_ptr<LSMStorageInner>,
        const CompactionTask&,
        shared_ptr<Level>) = 0;

    virtual unique_ptr<Iterator>
    execute_compact(
        shared_ptr<LSMStorageState>,
        const CompactionTask&) = 0;

    virtual ~CompactionController() = default;

    shared_ptr<Level>
    compact_generate_sst_from_iter(
            shared_ptr<LSMStorageInner> inner,
            Iterator& iter) {
        SSTableBuilder builder(inner->options->block_size, 
            inner->options->target_sst_size / 16, 
            0.0001);
        vector<shared_ptr<SSTable>> new_ssts;
        KeySlice first_key, last_key;
        
        // add watermark, compaction filter
        // todo

        while (iter.is_valid()) {
            if (!builder.add(iter.key(), iter.value())) {
                auto sst_id = inner->next_sst();
                new_ssts.push_back(
                    builder.build(
                        sst_id,
                        inner->block_cache, 
                        inner->path_of_sst(sst_id)
                    )
                );
                builder = std::move(
                    SSTableBuilder(
                        inner->options->block_size, 
                        inner->options->target_sst_size / 16, 
                        0.0001
                    )
                );
            }
            iter.next();
        }

        if (builder.estimated_size()) {
            auto sst_id = inner->next_sst();
            new_ssts.push_back(
                builder.build(
                    sst_id,
                    inner->block_cache, 
                    inner->path_of_sst(sst_id)
                )
            );
        }

        return make_shared<Level>(0, new_ssts);
    }
};
}

#endif