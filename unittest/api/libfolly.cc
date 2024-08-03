/*
 * @Author: lxc
 * @Date: 2024-04-10 15:13:18
 * @Description: Test for external library's usage.
 */

#include "defs.h"
#include "gtest/gtest.h"
#include "folly/ConcurrentSkipList.h"

using folly::ConcurrentSkipList;
struct ArrayCompare {
    bool operator()(const std::array<int, 2>& arr1, const std::array<int, 2>& arr2) {
        return arr1[0] < arr2[0];
    }
};

using SkipListType = ConcurrentSkipList<std::array<int, 2>, ArrayCompare>;
using AccessorType = SkipListType::Accessor;

class LibfollyTest : public testing::Test {
public:
    std::shared_ptr<SkipListType> sl;
public:
    void SetUp() override {
        sl = SkipListType::createInstance(10);
    }
};

int main() {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}

TEST_F(LibfollyTest, SigThd) {
    auto acer = AccessorType(sl);
    acer.add({150, 300});
    for (int i = 0; i < 100; i ++) acer.add({i, 2 * i});
    auto slice = acer.find({150,150});
    std::cout << (*slice)[0] << " " << (*slice)[1] << std::endl;
    // for (int i = 100; i < 200; i ++) acer.add({i, 2 * i});
}

// TEST_F(LibfollyTest, MulThd) {
//     std::thread insert_after_thread ([&](){
//         auto iacer = AccessorType(sl);
//         for (int i = 30000; i >= 15000; i--) {
//             iacer.add({i, i * 2});
//         }
//     });

//     std::thread insert_before_thread ([&](){
//         auto iacer = AccessorType(sl);
//         for (int i = 0; i <= 15000; i++) {
//             iacer.add({i, i * 2});
//         }
//     });

//     std::thread write_thread ([&](){
//         auto wacer = AccessorType(sl);
//         for (int i = 0; i < 15000; i++) wacer.add({15000, i});
//     });

//     std::thread read_thread ([&](){
//         auto racer = AccessorType(sl);
//         for (int i = 0; i < 150000; i++) {
//             std::this_thread::sleep_for(std::chrono::microseconds(1));
//             auto iter = racer.find({15000, -1});
//             if (iter != racer.end()) LOG(INFO) << (*iter)[0] << ":" << (*iter)[1] << ":" << sl->size();
//         }
//     });

//     insert_after_thread.join();
//     insert_before_thread.join();
//     write_thread.join();
//     read_thread.join();

//     // auto arr = std::array<int, 2>({150, 150});
//     // auto res = acer.add(arr);
//     // auto iter = acer.find({150, 10});
//     // LOG(INFO) << (*iter)[0] << ":" << (*iter)[1];
//     // while (iter != acer.end()) {
//     //     LOG(INFO) << (*iter)[0] << (*iter)[1];
//     //     iter = std::next(iter);
//     // }
// }