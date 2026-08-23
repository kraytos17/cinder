#pragma once

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <memory>
#include <mutex>
#include <vector>

namespace cinder {

// A standard-conforming pool allocator backed by fixed-size slabs. Each slab
// is a contiguous block of memory divided into equal-sized slots. Freed slots
// are recycled via a lock-free-eligible free list (currently guarded by the
// same mutex as slab growth for simplicity).  The allocator is rebind-aware so
// it works seamlessly with std::list, std::set, etc.
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
            std::scoped_lock lock(mutex_);
            if (free_head_) {
                auto* block = free_head_;
                free_head_ = free_head_->next;
                --free_count_;
                return reinterpret_cast<T*>(block);
            }
            return grow();
        }

        void deallocate(T* ptr) {
            std::scoped_lock lock(mutex_);
            auto* block = reinterpret_cast<FreeBlock*>(ptr);
            block->next = free_head_;
            free_head_ = block;
            ++free_count_;
        }

        [[nodiscard]] auto allocatedCount() const -> size_t {
            std::scoped_lock lock(mutex_);
            return slab_size_ * slots_per_slab_;
        }

        [[nodiscard]] auto freeCount() const -> size_t {
            std::scoped_lock lock(mutex_);
            return free_count_;
        }

      private:

        // A free-list node overlaying the slot memory.
        struct FreeBlock {
            FreeBlock* next = nullptr;
        };

        auto grow() -> T* {
            constexpr size_t K_MAX_SLOTS = 256;
            size_t n = std::min(slots_per_slab_, K_MAX_SLOTS);
            size_t slab_bytes = n * sizeof(T);
            auto* raw =
                static_cast<std::byte*>(::operator new(slab_bytes, std::align_val_t{alignof(T)}));

            slabs_.push_back(raw);
            for (size_t i = n; i > 0; --i) {
                auto* block = reinterpret_cast<FreeBlock*>(raw + (i - 1) * sizeof(T));
                block->next = free_head_;
                free_head_ = block;
                ++free_count_;
            }

            slab_size_ = n;
            auto* block = free_head_;
            free_head_ = free_head_->next;
            --free_count_;
            return reinterpret_cast<T*>(block);
        }

        std::vector<std::byte*> slabs_;
        FreeBlock* free_head_ = nullptr;
        size_t free_count_ = 0;
        size_t slots_per_slab_ = K_DEFAULT_SLOTS_PER_SLAB;
        size_t slab_size_ = 0;
        mutable std::mutex mutex_;
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
