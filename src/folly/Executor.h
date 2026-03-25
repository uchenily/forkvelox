#pragma once
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace folly {
class Executor {
public:
  virtual ~Executor() = default;
  virtual void add(std::function<void()> func) = 0;
};

class CPUThreadPoolExecutor : public Executor {
public:
  explicit CPUThreadPoolExecutor(int threads) : stop_(false) {
    const int threadCount = threads > 0 ? threads : 1;
    workers_.reserve(threadCount);
    for (int i = 0; i < threadCount; ++i) {
      workers_.emplace_back([this]() { workerLoop(); });
    }
  }

  ~CPUThreadPoolExecutor() override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stop_ = true;
    }
    cv_.notify_all();
    for (auto& worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
  }

  void add(std::function<void()> func) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      tasks_.push_back(std::move(func));
    }
    cv_.notify_one();
  }

 private:
  void workerLoop() {
    while (true) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]() { return stop_ || !tasks_.empty(); });
        if (stop_ && tasks_.empty()) {
          return;
        }
        task = std::move(tasks_.front());
        tasks_.pop_front();
      }
      task();
    }
  }

  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<std::function<void()>> tasks_;
  std::vector<std::thread> workers_;
  bool stop_;
};

// Stub
template <typename T>
struct Synchronized {
  T obj;
  T &wlock() { return obj; }
  const T &rlock() const { return obj; }
};
} // namespace folly
