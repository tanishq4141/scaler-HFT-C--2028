#pragma once

#include <atomic>
#include <array>
#include <mutex>
#include <cstddef>
#include <cstdint>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define CPU_RELAX() _mm_pause()
#elif defined(__aarch64__)
#define CPU_RELAX() asm volatile("yield" ::: "memory")
#else
#define CPU_RELAX() ((void)0)
#endif

// Naive test-and-set spinlock (causes severe cache line bouncing)
class SpinLock {
private:
    std::atomic<bool> flag_{false};

public:
    void lock() noexcept {
        while (flag_.exchange(true, std::memory_order_acquire)) {
            // tight spin loop (exchange issues write invalidation on every cycle)
        }
    }

    void unlock() noexcept {
        flag_.store(false, std::memory_order_release);
    }
};

// Optimized Test-and-Test-and-Set (TTAS) SpinLock:
// Spins on a local read-only load before attempting an exclusive exchange,
// dramatically reducing cache line bouncing and coherence bus traffic.
class TTASSpinLock {
private:
    std::atomic<bool> flag_{false};

public:
    void lock() noexcept {
        while (true) {
            // Spin on load: stays in 'Shared' cache state without bus invalidation
            while (flag_.load(std::memory_order_relaxed)) {
                CPU_RELAX();
            }
            // Attempt atomic RMW only when observed unlocked
            if (!flag_.exchange(true, std::memory_order_acquire)) {
                return;
            }
        }
    }

    void unlock() noexcept {
        flag_.store(false, std::memory_order_release);
    }
};

// SPSC Queue templated on Lock type (Ring Buffer)
template <typename T, typename Lock = TTASSpinLock, std::size_t Capacity = 65536>
class SPSCQueue {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");
    static constexpr std::size_t kMask = Capacity - 1;

private:
    std::size_t head_{0};
    std::size_t tail_{0};
    std::array<T, Capacity> buffer_{};
    Lock lock_{};

public:
    SPSCQueue() = default;

    bool push(const T& item) {
        lock_.lock();
        if (tail_ - head_ == Capacity) {
            lock_.unlock();
            return false; // Queue is full
        }
        buffer_[tail_ & kMask] = item;
        ++tail_;
        lock_.unlock();
        return true;
    }

    bool pop(T& item) {
        lock_.lock();
        if (head_ == tail_) {
            lock_.unlock();
            return false; // Queue is empty
        }
        item = buffer_[head_ & kMask];
        ++head_;
        lock_.unlock();
        return true;
    }

    std::size_t size() {
        lock_.lock();
        std::size_t s = tail_ - head_;
        lock_.unlock();
        return s;
    }

    bool empty() {
        lock_.lock();
        bool e = (head_ == tail_);
        lock_.unlock();
        return e;
    }
};

// Aliases for convenience
template <typename T, std::size_t Capacity = 65536>
using SPSCQueueSpinLock = SPSCQueue<T, SpinLock, Capacity>;

template <typename T, std::size_t Capacity = 65536>
using SPSCQueueTTASSpinLock = SPSCQueue<T, TTASSpinLock, Capacity>;

template <typename T, std::size_t Capacity = 65536>
using SPSCQueueMutex = SPSCQueue<T, std::mutex, Capacity>;
