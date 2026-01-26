# 逻辑计划到执行计划：Pipeline 拆分与并行驱动

本文档总结逻辑计划在执行时如何被拆分为多个 pipeline、pipeline 内如何并行执行，以及当前 MiniVelox 的实现方式（尽量对齐 Velox 的思想）。

## 1. 核心概念

- **Plan Fragment（计划片段）**：可在一个 Task 中执行的 PlanNode 子树。
- **Pipeline**：一条不包含全局阻塞算子的线性算子链。
- **Driver**：执行一个 pipeline 的线程实例，同一 pipeline 可由多个 driver 并行执行。
- **Local Exchange**：进程内队列，用于把上游 pipeline 的输出传给下游 pipeline。

## 2. 逻辑计划与执行计划的映射

逻辑计划以 `core::PlanNode` 树表示。
执行计划由多个 pipeline 组成，每个 pipeline 由一个或多个 driver 执行。

### 示例计划

```
Values -> Filter -> OrderBy
```

- `Values -> Filter` 为非阻塞链路。
- `OrderBy` 为阻塞算子。

此计划被拆成两个 pipeline：

```
Pipeline 0: Values -> Filter -> LocalExchangeSink
Pipeline 1: LocalExchangeSource -> OrderBy
```

## 3. Pipeline 拆分规则

当前实现（`exec::Task`）采用如下规则：

- 遇到阻塞算子就形成 pipeline 边界。
- 阻塞算子包括：`OrderBy`、`TopN`、`Aggregation`、`TableWrite`。
- 非阻塞算子（如 `Filter`）保留在同一 pipeline 内。

## 4. Pipeline 执行模型

### 4.1 每条 Pipeline 的 Driver 数

- 非阻塞 pipeline：可按 `maxDrivers` 并行运行。
- 阻塞 pipeline：强制单 driver 执行，保证全局正确性。

### 4.2 Local Exchange

`LocalExchangeQueue` 连接上下游 pipeline：

- 上游 pipeline 通过 `LocalExchangeSinkOperator` 将批次写入队列。
- 下游 pipeline 通过 `LocalExchangeSourceOperator` 读取批次。
- 队列知道生产者数量，上游全部结束后自动关闭。

## 5. Task 执行流程

`Task::run()` 的核心流程：

1. **线性化计划**（从源到根构成链）。
2. **按阻塞算子拆分 pipeline**。
3. **在 pipeline 之间创建 Local Exchange**。
4. **为每条 pipeline 创建 drivers 并发执行**。
5. **只收集最后一个 pipeline 的输出**。

伪流程如下：

```
plan -> [nodes]
(nodes) -> [pipelines]
相邻 pipeline 之间用 LocalExchange 连接
for each pipeline:
  启动 N 个 driver
  driver 执行自己的算子链
收集最后 pipeline 的输出
```

## 6. Pipeline 内并行

同一 pipeline 可由多个 driver 并行执行：

- 每个 driver 拥有自己的算子链实例。
- 数据源通过共享状态（如 `ValuesSourceState`）分配数据批次。

### 源共享状态

以 `ValuesSourceState` 为例，通过原子下标分配批次：

- Driver 0 取 batch 0
- Driver 1 取 batch 1
- ...

## 7. 阻塞算子与全局正确性

阻塞算子所在 pipeline 使用单 driver，确保全局结果正确：

- `OrderBy`：全局排序
- `TopN`：全局 Top-K
- `Aggregation`：全局或分组聚合
- `TableWrite`：单一写入

## 8. HashJoin 的 Pipeline 处理

HashJoin 会拆成两条 pipeline（对齐 Velox 的 `HashBuild`/`HashProbe` 模型）：

- **Build pipeline**：build side -> `HashBuildOperator`，只构建 hash 表，不输出数据。
- **Probe pipeline**：probe side -> `HashProbeOperator`，依赖 build 完成后的 hash 表进行探测输出。

当计划中存在多个 HashJoin 时，每个 Join 会有独立的 build/probe pipeline，先完成各自 build，再运行 probe pipeline。

## 9. Demo 覆盖

- **TaskParallelDemo**：验证单 pipeline 多 driver 并行（`Values -> Filter`）。
- **PipelineSplitDemo**：验证 pipeline 拆分与阻塞算子全局合并（`Values -> Filter -> OrderBy`）。

## 10. 可扩展方向

若进一步逼近 Velox，可在后续补充：

- 多分支计划（如 Join 多输入）。
- 聚合的 partial/final pipeline 拆分。
- Partitioned output pipeline。
- 按算子限制动态确定 driver 数量。
