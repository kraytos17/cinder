#include "cinder/store/ttl_wheel.hpp"

namespace cinder {

void
TtlWheel::insert(const std::string& key, size_t ttl_ticks) {
    // Overwriting a key must not leak it in its previous slot — otherwise a
    // stale tick() would fire the key again after its expiry was moved.
    if (auto it = key_to_slot_.find(key); it != key_to_slot_.end()) {
        wheel_[it->second].erase(key);
    }

    auto slot = (cursor_ + ttl_ticks) % K_SLOT_COUNT;
    wheel_[slot].insert(key);
    key_to_slot_[key] = slot;
}

void
TtlWheel::remove(const std::string& key) {
    auto it = key_to_slot_.find(key);
    if (it == key_to_slot_.end()) {
        return;
    }

    wheel_[it->second].erase(key);
    key_to_slot_.erase(it);
}

void
TtlWheel::tick(std::move_only_function<void(const std::string&)> on_expired) {
    cursor_ = (cursor_ + 1) % K_SLOT_COUNT;
    auto& slot = wheel_[cursor_];
    for (const auto& key : slot) {
        key_to_slot_.erase(key);
        on_expired(key);
    }
    slot.clear();
}

auto
TtlWheel::cursor() const -> size_t {
    return cursor_;
}
} // namespace cinder
