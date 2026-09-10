#pragma once

#include <concepts>
#include <cstddef>
#include <functional>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace cinder {

// Base struct for intrusive TTL-wheel linkage.
struct WheelNode {
    WheelNode* wheel_prev = nullptr;
    WheelNode* wheel_next = nullptr;
    // 0 = not in any wheel slot; slot = index + 1 (so sentinel at index i
    // has wheel_slot == 0 and never looks "scheduled").
    uint16_t wheel_slot = 0;
    bool in_heap = false; // true when scheduled in the min-heap
};

// Concept: Node must inherit from WheelNode and expose a `key` member.
template <typename Node>
concept WheelNodeLike = std::derived_from<Node, WheelNode> && requires(const Node& n) {
    { n.key } -> std::convertible_to<std::string_view>;
};

// Intrusive TTL wheel using a circular doubly-linked list per slot
// and a min-heap for long TTLs (> K_SLOT_COUNT ticks).
//
// insert/remove are O(1). tick() walks the current slot's intrusive list
// and pops matching heap entries — total cost proportional to the number
// of entries that actually expire.
template <WheelNodeLike Node> class TtlWheel {
  public:

    static constexpr size_t K_SLOT_COUNT = 256;

    TtlWheel() {
        // Initialize each sentinel as a self-referencing circular list.
        for (auto& sentinel : wheel_) {
            sentinel.wheel_prev = &sentinel;
            sentinel.wheel_next = &sentinel;
        }
    }

    // Schedule node for expiry after ttl_ticks. O(1).
    // If the node is already scheduled, it is removed first.
    void insert(Node* node, size_t ttl_ticks) {
        remove(node);
        if (ttl_ticks <= K_SLOT_COUNT) {
            auto slot = (cursor_ + ttl_ticks) % K_SLOT_COUNT;
            node->wheel_slot = static_cast<uint16_t>(slot + 1);
            auto* sentinel = &wheel_[slot];

            node->wheel_next = sentinel;
            node->wheel_prev = sentinel->wheel_prev;
            sentinel->wheel_prev->wheel_next = node;
            sentinel->wheel_prev = node;
        } else {
            auto absolute_tick = tick_count_ + ttl_ticks;
            heap_.push_back({absolute_tick, node});
            node->in_heap = true;
            std::push_heap(heap_.begin(), heap_.end(), [](const HeapEntry& a, const HeapEntry& b) {
                return a.absolute_tick > b.absolute_tick;
            });
        }
    }

    // Remove node from wherever it is scheduled.
    void remove(Node* node) {
        if (node->wheel_slot > 0) {
            node->wheel_prev->wheel_next = node->wheel_next;
            node->wheel_next->wheel_prev = node->wheel_prev;
            node->wheel_prev = nullptr;
            node->wheel_next = nullptr;
            node->wheel_slot = 0;
        }
        // Tombstone for heap path — only if actually in the heap.
        if (node->in_heap) {
            heap_removed_.insert(node);
            node->in_heap = false;
        }
    }

    // Advance the cursor by one tick and fire the callback for every entry
    // that has reached its expiry.
    void tick(std::move_only_function<void(Node&)> on_expired) {
        cursor_ = (cursor_ + 1) % K_SLOT_COUNT;
        ++tick_count_;

        // Walk the intrusive list in the current slot.
        auto* sentinel = &wheel_[cursor_];
        auto* node = sentinel->wheel_next;
        while (node != sentinel) {
            auto* next = node->wheel_next;
            node->wheel_prev->wheel_next = node->wheel_next;
            node->wheel_next->wheel_prev = node->wheel_prev;
            node->wheel_prev = nullptr;
            node->wheel_next = nullptr;
            node->wheel_slot = 0;

            on_expired(*static_cast<Node*>(node));
            node = next;
        }
        // Fire heap entries whose absolute_tick has been reached.
        while (!heap_.empty() && heap_.front().absolute_tick <= tick_count_) {
            auto entry = heap_.front();
            std::pop_heap(heap_.begin(), heap_.end(), [](const HeapEntry& a, const HeapEntry& b) {
                return a.absolute_tick > b.absolute_tick;
            });

            heap_.pop_back();
            entry.node->in_heap = false;
            if (heap_removed_.contains(entry.node)) {
                heap_removed_.erase(entry.node);
                continue;
            }
            on_expired(*entry.node);
        }
    }

    [[nodiscard]] auto cursor() const -> size_t { return cursor_; }

    [[nodiscard]] auto tickCount() const -> size_t { return tick_count_; }

  private:

    // Sentinel-headed circular doubly-linked list per slot.
    // Each wheel_[i] is a WheelNode sentinel; real nodes link between
    // sentinel.wheel_prev and sentinel.wheel_next.
    std::vector<WheelNode> wheel_{K_SLOT_COUNT};
    size_t cursor_ = 0;
    size_t tick_count_ = 0;

    // Min-heap for long TTLs (> K_SLOT_COUNT ticks).
    struct HeapEntry {
        size_t absolute_tick = 0;
        Node* node = nullptr;
    };

    std::vector<HeapEntry> heap_;
    std::unordered_set<Node*> heap_removed_;
};
} // namespace cinder
