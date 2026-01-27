# Driver 组件说明

本文档说明 ForkVelox 中 `Driver` 与 `DriverFactory` 的职责、关键字段和执行流程，便于阅读与扩展实现。

## 1. 核心概念

在执行模型中，一个 **Driver** 负责驱动一条 pipeline 的运算：从 source operator 拉取数据，依次推进到下游 operator，最终把输出 batch 追加到结果集中。它同时处理取消、让出（yield）等控制信号，并在 source 结束后触发 pipeline 的 flush。

## 2. BlockingReason

`BlockingReason` 描述 Driver 运行过程中返回的状态：

- `kNotBlocked`：正常完成，无阻塞。
- `kWaitForSplit`：等待 split（当前实现未使用）。
- `kWaitForExchange`：等待 exchange（当前实现未使用）。
- `kWaitForJoin`：等待 join（当前实现未使用）。
- `kYield`：触发让出（yield）并退出本轮运行。
- `kCancelled`：触发取消（cancel）并退出。
- `kTerminated`：已终止（当前实现未使用）。

## 3. Driver 类

### 3.1 构造与依赖

```cpp
Driver(std::vector<std::shared_ptr<Operator>> operators)
```

Driver 需要一个按 pipeline 顺序排列的 operator 列表，`operators_[0]` 必须是 source operator。

### 3.2 取消与让出

Driver 支持注入两个回调：

- `setCancelCheck(std::function<bool()>)`
- `setYieldCheck(std::function<bool()>)`

在 `run()` 中会周期性调用它们，用于提前退出执行。

### 3.3 运行流程（run）

`run(std::vector<RowVectorPtr>& results)` 的主流程可以概括为：

1. 循环拉取 source（`operators_.front()->getOutput()`）。
2. 如果拿到 batch，则依次喂给后续 operators，并尝试拿到最终输出。
3. 若 source 返回空且已结束，调用 `noMoreInput()` 传播结束信号，然后进入 flush 阶段。
4. flush 阶段循环调用 `getOutput()` 尝试把各 operator 内部缓冲刷出到结果集。
5. 根据 cancel/yield 返回相应 `BlockingReason`，否则返回 `kNotBlocked`。

简化流程图（伪代码）：

```
while (progress) {
  if (cancel) return kCancelled;
  if (yield) return kYield;

  batch = source.getOutput();
  if (batch) {
    for op in downstream:
      op.addInput(batch);
      batch = op.getOutput();
      if (!batch) break;
    if (batch) results.push_back(batch);
  } else if (source.isFinished()) {
    for op in downstream: op.noMoreInput();
    flush();
    break;
  }
}
return kNotBlocked;
```

### 3.4 输出语义

- `results` 只接收最后一个 operator 的输出。
- flush 阶段会把缓存输出逐步向下游推进，直到没有更多输出为止。

## 4. DriverFactory

`DriverFactory` 用于批量创建 Driver 实例，持有用于构建 pipeline 的元数据：

- `planNodes`：需要转成 operators 的 PlanNode 列表。
- `operatorSupplier`：可选的额外 operator 生成器（用于附加 sink 或特殊 operator）。
- `consumerNode`：下游消费者节点（当前文件中未直接使用）。
- `inputDriver` / `outputPipeline`：标记输入/输出 pipeline（当前文件中未直接使用）。
- `numDrivers`：并行 driver 数量。
- `probeJoinIds`：probe join 的 PlanNodeId 集合（当前文件中未直接使用）。

`createDriver()` 会：

1. 根据 `planNodes` 调用 `makeOperator()` 构建 operators。
2. 如有 `operatorSupplier`，追加一个 operator。
3. 创建并返回 `Driver`。

## 5. 扩展建议

- 若要支持更多阻塞类型（split/exchange/join），需要在 `run()` 中识别 operator 返回的阻塞状态，并转换为 `BlockingReason`。
- 若要支持多 source 或分支 pipeline，需要在 operator 图构建与 `run()` 推进策略上扩展。
