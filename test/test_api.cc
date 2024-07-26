/*
 * @Author: lxc
 * @Date: 2024-04-10 15:13:18
 * @Description: Test for external library's usage.
 */

#include <chrono>
#include <iterator>
#include <array>
#include <thread>
#include "folly/ConcurrentSkipList.h"

using folly::ConcurrentSkipList;
using std::array;
struct ArrayCompare {
    bool operator()(const array<int, 2>& arr1, const array<int, 2>& arr2) {
        return arr1[0] < arr2[0];
    }
};

typedef ConcurrentSkipList<array<int, 2>, ArrayCompare>::Accessor Accessor;

int main() {
    auto sl = ConcurrentSkipList<array<int, 2>, ArrayCompare>::createInstance(10);
    auto acer = Accessor(sl);
    // acer.add({150, 300});
    // for (int i = 0; i < 100; i ++) acer.add({i, 2 * i});
    // for (int i = 100; i < 200; i ++) acer.add({i, 2 * i});

    std::thread insert_after_thread ([sl](){
        auto iacer = Accessor(sl);
        for (int i = 30000; i >= 15000; i--) {
            iacer.add({i, i * 2});
        }
    });

    std::thread insert_before_thread ([sl](){
        auto iacer = Accessor(sl);
        for (int i = 0; i <= 15000; i++) {
            iacer.add({i, i * 2});
        }
    });

    std::thread write_thread ([sl](){
        auto wacer = Accessor(sl);
        for (int i = 0; i < 15000; i++) wacer.add({15000, i});
    });

    std::thread read_thread ([sl](){
        auto racer = Accessor(sl);
        for (int i = 0; i < 150000; i++) {
            std::this_thread::sleep_for(std::chrono::microseconds(1));
            auto iter = racer.find({15000, -1});
            if (iter != racer.end()) LOG(INFO) << (*iter)[0] << ":" << (*iter)[1] << ":" << sl->size();
        }
    });

    insert_after_thread.join();
    insert_before_thread.join();
    write_thread.join();
    read_thread.join();

    // auto arr = array<int, 2>({150, 150});
    // auto res = acer.add(arr);
    // auto iter = acer.find({150, 10});
    // LOG(INFO) << (*iter)[0] << ":" << (*iter)[1];
    // while (iter != acer.end()) {
    //     LOG(INFO) << (*iter)[0] << (*iter)[1];
    //     iter = std::next(iter);
    // }

    return 0;
}