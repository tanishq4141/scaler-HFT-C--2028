#include <iostream>
#include <iomanip>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include <cstdint>
#include "spsc_queue.hpp"

// 64-byte Order representation (matching L1 cache line size)
struct Order {
    uint64_t order_id;
    uint64_t timestamp_ns;
    double price;
    uint32_t quantity;
    char symbol[16];
    char side;          // 'B' or 'S'
    char order_type;    // 'L' or 'M'
    char padding[14];   // Pad to exactly 64 bytes
};
static_assert(sizeof(Order) == 64, "Order struct must be exactly 64 bytes");

void run_benchmark_1sec() {
    std::cout << "=========================================================\n";
    std::cout << "  SPSC SpinLock Queue 1-Second Benchmark (64-byte items) \n";
    std::cout << "=========================================================\n";

    // 64K capacity ring buffer
    constexpr std::size_t kCapacity = 65536;
    SPSCQueueSpinLock<Order, kCapacity> queue;

    std::atomic<bool> start_flag{false};
    std::atomic<bool> stop_flag{false};
    uint64_t total_produced = 0;
    uint64_t total_consumed = 0;

    std::thread producer([&]() {
        // Wait for start signal
        while (!start_flag.load(std::memory_order_relaxed)) {
            std::this_thread::yield();
        }

        Order order{};
        order.price = 149.99;
        order.quantity = 100;
        order.side = 'B';
        order.order_type = 'L';

        while (!stop_flag.load(std::memory_order_relaxed)) {
            order.order_id = total_produced + 1;
            if (queue.push(order)) {
                ++total_produced;
            }
        }
    });

    std::thread consumer([&]() {
        // Wait for start signal
        while (!start_flag.load(std::memory_order_relaxed)) {
            std::this_thread::yield();
        }

        Order order{};
        while (!stop_flag.load(std::memory_order_relaxed) || !queue.empty()) {
            if (queue.pop(order)) {
                ++total_consumed;
            }
        }
    });

    // Warm-up short delay then fire start
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto start_time = std::chrono::steady_clock::now();
    start_flag.store(true, std::memory_order_release);

    // Run for exactly 1 second
    std::this_thread::sleep_for(std::chrono::seconds(1));
    stop_flag.store(true, std::memory_order_release);

    producer.join();
    consumer.join();
    auto end_time = std::chrono::steady_clock::now();

    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    double ops_per_sec = total_consumed / elapsed_sec;
    double mb_per_sec = (total_consumed * sizeof(Order)) / (elapsed_sec * 1024.0 * 1024.0);
    double gb_per_sec = (total_consumed * sizeof(Order)) / (elapsed_sec * 1e9);

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Payload Size      : " << sizeof(Order) << " bytes\n";
    std::cout << "Elapsed Time      : " << elapsed_sec << " s\n";
    std::cout << "Total Orders Pushed: " << total_produced << "\n";
    std::cout << "Total Orders Popped: " << total_consumed << "\n";
    std::cout << "Throughput        : " << (ops_per_sec / 1e6) << " Million msgs/sec ("
              << static_cast<uint64_t>(ops_per_sec) << " msgs/sec)\n";
    std::cout << "Bandwidth         : " << mb_per_sec << " MB/s (" << gb_per_sec << " GB/s)\n";
    std::cout << "Avg Latency       : " << (1e9 / ops_per_sec) << " ns/op\n";
    std::cout << "=========================================================\n";
}

int main() {
    run_benchmark_1sec();
    return 0;
}
