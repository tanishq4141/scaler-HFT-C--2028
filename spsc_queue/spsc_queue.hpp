#pragma once

#include <atomic>
#include <array>
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

// SPSC Queue protected by a SpinLock (Ring Buffer)
template <typename T, std::size_t Capacity = 65536>
class SPSCQueueSpinLock {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");
    static constexpr std::size_t kMask = Capacity - 1;

private:
    std::size_t head_{0};
    std::size_t tail_{0};
    std::array<T, Capacity> buffer_{};
    SpinLock lock_{};

public:
    SPSCQueueSpinLock() = default;

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

    std::size_t size() const {
        return tail_ - head_;
    }

    bool empty() const {
        return head_ == tail_;
    }
};
