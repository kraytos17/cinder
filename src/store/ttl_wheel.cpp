#include "cinder/store/ttl_wheel.hpp"

#include <algorithm>

namespace cinder {

void
TtlWheel::insert(const std::string& key, size_t ttl_ticks) {
    if (auto it = key_to_slot_.find(key); it != key_to_slot_.end()) {
        wheel_[it->second].erase(key);
        key_to_slot_.erase(it);
    }

    heap_removed_.erase(key);
    if (ttl_ticks <= K_SLOT_COUNT) {
        auto slot = (cursor_ + ttl_ticks) % K_SLOT_COUNT;
        wheel_[slot].insert(key);
        key_to_slot_[key] = slot;
    } else {
        heap_.push_back({tick_count_ + ttl_ticks, key});
        std::push_heap(heap_.begin(), heap_.end(), [](const HeapEntry& a, const HeapEntry& b) {
            return a.absolute_tick > b.absolute_tick;
        });
    }
}

void
TtlWheel::remove(const std::string& key) {
    if (auto it = key_to_slot_.find(key); it != key_to_slot_.end()) {
        wheel_[it->second].erase(key);
        key_to_slot_.erase(it);
    }
    heap_removed_.insert(key);
}

void
TtlWheel::tick(std::move_only_function<void(const std::string&)> on_expired) {
    cursor_ = (cursor_ + 1) % K_SLOT_COUNT;
    ++tick_count_;

    auto& slot = wheel_[cursor_];
    for (const auto& key : slot) {
        key_to_slot_.erase(key);
        on_expired(key);
    }

    slot.clear();
    // Fire heap entries whose absolute_tick has been reached.
    while (!heap_.empty() && heap_.front().absolute_tick <= tick_count_) {
        auto entry = heap_.front();
        std::pop_heap(heap_.begin(), heap_.end(), [](const HeapEntry& a, const HeapEntry& b) {
            return a.absolute_tick > b.absolute_tick;
        });

        heap_.pop_back();
        if (heap_removed_.contains(entry.key)) {
            heap_removed_.erase(entry.key);
            continue;
        }
        on_expired(entry.key);
    }
}

auto
TtlWheel::cursor() const -> size_t {
    return cursor_;
}

auto
TtlWheel::tickCount() const -> size_t {
    return tick_count_;
}
} // namespace cinder
