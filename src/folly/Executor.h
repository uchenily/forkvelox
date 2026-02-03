#pragma once
#include <functional>
#include <thread>

namespace folly {
class Executor {
public:
  virtual ~Executor() = default;
  virtual void add(std::function<void()> func) = 0;
};

class CPUThreadPoolExecutor : public Executor {
public:
  CPUThreadPoolExecutor(int threads) {}
  void add(std::function<void()> func) override {
    func(); // Synchronous for now
  }
};

// Stub
template <typename T>
struct Synchronized {
  T obj;
  T &wlock() { return obj; }
  const T &rlock() const { return obj; }
};
} // namespace folly
