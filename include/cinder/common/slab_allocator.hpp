#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstring>
#include <memory>
#include <mutex>
#include <vector>

namespace cinder {

// A standard-conforming pool allocator backed by fixed-size slabs. Each slab
// is a contiguous block of memory divided into equal-sized slots. Freed slots
// are recycled via a lock-free free list (CAS on free_head_). Only slab
// growth needs the mutex — the hot path (allocate/deallocate when free list
// is non-empty) is entirely lock-free.
//
// Usage:  std::list<Node, SlabAllocator<Node>> lru_list_;
//
// Only allocate/deallocate are hot-path; all other allocator traits are
// constant-time.
template <typename T> class SlabAllocator {
  public:

    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    // Shared state across rebound allocators.  Holds the free list and all
    // allocated slabs so memory is released in bulk when the last allocator
    // copy goes away.
    struct Pool {
        static constexpr size_t K_DEFAULT_SLOTS_PER_SLAB = 256;

        Pool() = default;

        explicit Pool(size_t slots_per_slab)
            : slots_per_slab_(slots_per_slab) {}

        Pool(const Pool&) = delete;
        auto operator=(const Pool&) -> Pool& = delete;
        Pool(Pool&&) = delete;
        auto operator=(Pool&&) -> Pool& = delete;

        ~Pool() {
            for (auto* slab : slabs_) {
                ::operator delete(slab);
            }
        }

        auto allocate() -> T* {
            // Lock-free fast path: pop from the free list via CAS.
            FreeBlock* head = free_head_.load(std::memory_order_acquire);
            while (head) {
                if (free_head_.compare_exchange_weak(
                        head, head->next, std::memory_order_acq_rel, std::memory_order_acquire)) {
                    return reinterpret_cast<T*>(head);
                }
            }
            // Free list empty — need to grow (requires mutex for slab allocation).
            return grow();
        }

        void deallocate(T* ptr) noexcept {
            // Lock-free push onto the free list via CAS.
            auto* block = reinterpret_cast<FreeBlock*>(ptr);
            FreeBlock* head = free_head_.load(std::memory_order_acquire);
            do {
                block->next = head;
            } while (!free_head_.compare_exchange_weak(
                head, block, std::memory_order_acq_rel, std::memory_order_acquire));
        }

        [[nodiscard]] auto allocatedCount() const -> size_t {
            return total_slots_.load(std::memory_order_relaxed);
        }

        [[nodiscard]] auto freeCount() const -> size_t {
            return free_count_.load(std::memory_order_relaxed);
        }

      private:

        // A free-list node overlaying the slot memory.
        struct FreeBlock {
            FreeBlock* next = nullptr;
        };

        auto grow() -> T* {
            std::scoped_lock lock(mutex_);
            // Double-check after acquiring the lock (another thread may have grown).
            FreeBlock* head = free_head_.load(std::memory_order_acquire);
            if (head) {
                if (free_head_.compare_exchange_strong(
                        head, head->next, std::memory_order_acq_rel, std::memory_order_acquire)) {
                    return reinterpret_cast<T*>(head);
                }
            }

            constexpr size_t K_MAX_SLOTS = 256;
            size_t n = std::min(slots_per_slab_, K_MAX_SLOTS);
            size_t slab_bytes = n * sizeof(T);
            auto* raw =
                static_cast<std::byte*>(::operator new(slab_bytes, std::align_val_t{alignof(T)}));

            slabs_.push_back(raw);
            // Push all new slots onto the free list (lock-free, only we are here).
            for (size_t i = n; i > 0; --i) {
                auto* block = reinterpret_cast<FreeBlock*>(raw + (i - 1) * sizeof(T));
                block->next = free_head_.load(std::memory_order_relaxed);
                free_head_.store(block, std::memory_order_release);
            }

            free_count_.fetch_add(n, std::memory_order_relaxed);
            total_slots_.fetch_add(n, std::memory_order_relaxed);
            // Pop one for the caller.
            head = free_head_.load(std::memory_order_acquire);
            if (head) {
                free_head_.store(head->next, std::memory_order_release);
                free_count_.fetch_sub(1, std::memory_order_relaxed);
                return reinterpret_cast<T*>(head);
            }
            return nullptr; // unreachable
        }

        std::vector<std::byte*> slabs_;
        std::atomic<FreeBlock*> free_head_{nullptr};
        std::atomic<size_t> free_count_{0};
        std::atomic<size_t> total_slots_{0};
        size_t slots_per_slab_ = K_DEFAULT_SLOTS_PER_SLAB;
        mutable std::mutex mutex_; // only guards slab growth
    };

    // Default constructor — creates a fresh pool.
    SlabAllocator()
        : pool_(std::make_shared<Pool>()) {}

    // Rebinding copy constructor — shares the pool.
    template <typename U>
    explicit SlabAllocator(const SlabAllocator<U>& other) noexcept
        : pool_(other.getPool()) {}

    SlabAllocator(const SlabAllocator&) = default;
    auto operator=(const SlabAllocator&) -> SlabAllocator& = default;
    SlabAllocator(SlabAllocator&&) noexcept = default;
    auto operator=(SlabAllocator&&) noexcept -> SlabAllocator& = default;

    ~SlabAllocator() = default;

    [[nodiscard]] auto allocate(size_t n) -> T* {
        if (n != 1) {
            // Fall back to the global allocator for multi-element allocations
            // (std::list only ever allocates one node at a time).
            return static_cast<T*>(::operator new(n * sizeof(T)));
        }
        return pool_->allocate();
    }

    void deallocate(T* ptr, size_t n) noexcept {
        if (n != 1) {
            ::operator delete(ptr);
            return;
        }
        pool_->deallocate(ptr);
    }

    template <typename U>
    [[nodiscard]] auto operator==(const SlabAllocator<U>& other) const noexcept -> bool {
        return pool_ == other.getPool();
    }

    template <typename U>
    [[nodiscard]] auto operator!=(const SlabAllocator<U>& other) const noexcept -> bool {
        return !(*this == other);
    }

    // Access the shared pool (needed for rebound-constructor).
    [[nodiscard]] auto getPool() const -> std::shared_ptr<Pool> { return pool_; }

  private:

    template <typename U> friend class SlabAllocator;

    std::shared_ptr<Pool> pool_;
};
} // namespace cinder
