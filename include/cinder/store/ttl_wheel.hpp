#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <string_view>
#include <vector>

namespace cinder {

    // Base struct for intrusive TTL-wheel linkage.
    // Kept first in every store node so list/wheel pointer-chasing pulls the
    // linkage, not key/value heap data, into the line.
    struct WheelNode {
        WheelNode* wheel_prev = nullptr;
        WheelNode* wheel_next = nullptr;
        // 0 = not in any wheel slot; slot = index + 1 (so sentinel at index i
        // has wheel_slot == 0 and never looks "scheduled").
        uint16_t wheel_slot = 0;
        bool in_heap = false; // true when scheduled in the min-heap
        // Position in the long-TTL heap, or K_NOT_IN_HEAP. Heap entries are
        // removed before their node is destroyed or its slab slot is reused.
        static constexpr size_t K_NOT_IN_HEAP = std::numeric_limits<size_t>::max();
        size_t heap_index = K_NOT_IN_HEAP;
    };

    static_assert(sizeof(WheelNode) == 32, "WheelNode should be 32 bytes");

    // Concept: Node must inherit from WheelNode and expose a `key` member.
    template <typename Node>
    concept WheelNodeLike = std::derived_from<Node, WheelNode> && requires(const Node& n) {
        { n.key } -> std::convertible_to<std::string_view>;
    };

    // Intrusive TTL wheel using a circular doubly-linked list per slot
    // and a min-heap for long TTLs (> K_SLOT_COUNT ticks).
    //
    // Near-term insert/remove are O(1); long-TTL heap insert/remove are
    // O(log n). tick() walks the current slot and pops expired heap roots.
    template <WheelNodeLike Node> class TtlWheel {
    public:

        static constexpr size_t K_SLOT_COUNT = 256;
        // Retained drain-batch ceiling: 64K pointers (512 KiB). A one-off
        // mass expiry may grow the buffer past this; it is cut back on the
        // next tick instead of pinning the spike forever.
        static constexpr size_t K_MAX_DRAIN_RETAIN = 65'536;

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
                linkBefore(node, &wheel_[slot]);
            } else {
                const size_t index = heap_.size();
                heap_.push_back({tick_count_ + ttl_ticks, node});
                node->heap_index = index;
                siftUp(node->heap_index);
            }
        }

        // Remove node from wherever it is scheduled. Long-TTL heap entries
        // are eagerly removed, so the heap never retains dead-node pointers.
        void remove(Node* node) {
            unlink(node);
            if (node->heap_index != WheelNode::K_NOT_IN_HEAP) {
                removeHeapAt(node->heap_index);
            }
        }

        // Advance the cursor by one tick and fire the callback for every entry
        // that has reached its expiry.
        void tick(std::move_only_function<void(Node&)> on_expired) {
            cursor_ = (cursor_ + 1) % K_SLOT_COUNT;
            ++tick_count_;

            expireSlot(on_expired);
            expireHeap(on_expired);
        }

        [[nodiscard]]
        auto cursor() const -> size_t {
            return cursor_;
        }

        [[nodiscard]]
        auto tickCount() const -> size_t {
            return tick_count_;
        }

    private:

        static void unlink(WheelNode* node) {
            if (node->wheel_slot == 0) {
                return;
            }

            node->wheel_prev->wheel_next = node->wheel_next;
            node->wheel_next->wheel_prev = node->wheel_prev;
            node->wheel_prev = nullptr;
            node->wheel_next = nullptr;
            node->wheel_slot = 0;
        }

        static void linkBefore(WheelNode* node, WheelNode* sentinel) {
            node->wheel_next = sentinel;
            node->wheel_prev = sentinel->wheel_prev;
            sentinel->wheel_prev->wheel_next = node;
            sentinel->wheel_prev = node;
        }

        auto earlier(size_t lhs, size_t rhs) const -> bool {
            return heap_[lhs].absolute_tick < heap_[rhs].absolute_tick;
        }

        void swapHeap(size_t lhs, size_t rhs) {
            std::ranges::swap(heap_[lhs], heap_[rhs]);
            heap_[lhs].node->heap_index = lhs;
            heap_[rhs].node->heap_index = rhs;
        }

        void siftUp(size_t index) {
            while (index > 0) {
                const size_t parent = (index - 1) / 2;
                if (!earlier(index, parent)) {
                    break;
                }

                swapHeap(index, parent);
                index = parent;
            }
        }

        void siftDown(size_t index) {
            while (true) {
                const size_t left = index * 2 + 1;
                if (left >= heap_.size()) {
                    return;
                }

                const size_t right = left + 1;
                size_t first = left;
                if (right < heap_.size() && earlier(right, left)) {
                    first = right;
                }
                if (!earlier(first, index)) {
                    return;
                }

                swapHeap(index, first);
                index = first;
            }
        }

        void removeHeapAt(size_t index) {
            const size_t last = heap_.size() - 1;
            Node* removed = heap_[index].node;
            if (index != last) {
                heap_[index] = heap_[last];
                heap_[index].node->heap_index = index;
            }

            heap_.pop_back();
            removed->heap_index = WheelNode::K_NOT_IN_HEAP;
            if (index < heap_.size()) {
                if (index > 0 && earlier(index, (index - 1) / 2)) {
                    siftUp(index);
                } else {
                    siftDown(index);
                }
            }
        }

        void expireSlot(std::move_only_function<void(Node&)>& on_expired) {
            // Drain the current slot into a local batch first, then invoke the
            // callback per node. The callback may re-insert the node it was
            // handed (EvictionStoreBase::evictExpired re-schedules an entry
            // that fired early due to wheel wrap or sub-second drift),
            // including back into this same slot. Processing a live intrusive
            // walk would then either revisit it forever (if re-reading the
            // head) or chase a stale `next` (if pre-captured before the
            // callback rewrote the links). Draining first makes re-inserts
            // land in the wheel for a future tick and guarantees termination.
            //
            // The batch buffer is reused across ticks (clear keeps capacity)
            // instead of allocating a fresh vector per sweep; it is capped so
            // a one-off mass expiry cannot pin megabytes forever.
            auto* sentinel = &wheel_[cursor_];
            auto& batch = drain_batch_;
            if (batch.capacity() > K_MAX_DRAIN_RETAIN) {
                batch = std::vector<Node*>{};
                batch.reserve(K_MAX_DRAIN_RETAIN);
            }

            batch.clear();
            while (sentinel->wheel_next != sentinel) {
                auto* node = sentinel->wheel_next;
                unlink(node);
                batch.push_back(static_cast<Node*>(node));
            }
            for (auto* node : batch) {
                on_expired(*node);
            }
        }

        void expireHeap(std::move_only_function<void(Node&)>& on_expired) {
            // Remove the root before invoking client code. The heap therefore
            // never retains a stale pointer after the callback destroys node.
            while (!heap_.empty() && heap_.front().absolute_tick <= tick_count_) {
                Node* node = heap_.front().node;
                removeHeapAt(0);
                on_expired(*node);
            }
        }

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
        std::vector<Node*> drain_batch_;
    };
} // namespace cinder
