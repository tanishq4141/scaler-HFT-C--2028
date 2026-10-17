#pragma once

#include <atomic>
#include <array>
#include <mutex>
#include <cstddef>
#include <cstdint>

// Naive test-and-set spinlock
class SpinLock {
private:
    std::atomic<bool> flag_{false};

public:
    void lock() noexcept {
        while (flag_.exchange(true, std::memory_order_acquire)) {
            // tight spin loop (exchange writes every time)
        }
    }

    void unlock() noexcept {
        flag_.store(false, std::memory_order_release);
    }
};

// SPSC Queue templated on Lock type (Ring Buffer)
template <typename T, typename Lock = SpinLock, std::size_t Capacity = 65536>
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
using SPSCQueueMutex = SPSCQueue<T, std::mutex, Capacity>;
