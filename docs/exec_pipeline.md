# 逻辑计划到执行计划：Pipeline 拆分与并行驱动

本文档总结逻辑计划在执行时如何被拆分为多个 pipeline、pipeline 内如何并行执行，以及当前 MiniVelox 的实现方式（尽量对齐 Velox 的思想）。

## 1. 核心概念

- **Plan Fragment（计划片段）**：可在一个 Task 中执行的 PlanNode 子树。
- **Pipeline**：一条不包含全局阻塞算子的线性算子链。
- **Driver**：执行一个 pipeline 的线程实例，同一 pipeline 可由多个 driver 并行执行。
- **Local Exchange**：进程内队列，用于把上游 pipeline 的输出传给下游 pipeline（MiniVelox 尚未在 pipeline 拆分中启用）。

## 2. 逻辑计划与执行计划的映射

逻辑计划以 `core::PlanNode` 树表示。
执行计划由多个 pipeline 组成，每个 pipeline 由一个或多个 driver 执行。

### 示例计划

```
Values -> Filter -> OrderBy
```

- `Values -> Filter` 为非阻塞链路。
- `OrderBy` 为阻塞算子。

此计划在当前实现中为单 pipeline，但 `OrderBy` 强制单 driver：

```
Pipeline 0: Values -> Filter -> OrderBy
```

## 3. Pipeline 拆分规则

当前实现参考 Velox `LocalPlanner` 的思路：

- **按输入源拆分**：同一 PlanNode 的 **非第 0 个 source** 会启动新 pipeline。
- **Join build pipeline**：`HashJoin` 的 build side 会生成独立 pipeline，尾部追加 `HashBuildOperator`。
- 单输入链路不会因为阻塞算子拆分 pipeline。

## 4. Pipeline 执行模型

### 4.1 每条 Pipeline 的 Driver 数

单 pipeline 内仍会根据算子特性限制并发：

- `OrderBy`、`TopN`、`Aggregation`、`TableWrite` 强制单 driver。
- 其他算子默认按 `maxDrivers` 并行运行。

### 4.2 Pipeline 依赖

Join 的 probe pipeline 依赖其 build pipeline 完成，执行时按依赖顺序调度。

## 5. Task 执行流程

`Task::run()` 的核心流程（参考 `LocalPlanner`）：

1. **遍历计划树**，按非第 0 个 source 拆分 pipeline。
2. **为 HashJoin build side 追加 HashBuildOperator**。
3. **根据依赖顺序执行 pipeline**。
4. **只收集输出 pipeline 的结果**。

伪流程如下：

```
plan -> [pipelines]
for each pipeline in dependency order:
  启动 N 个 driver
  driver 执行自己的算子链
收集输出 pipeline 的结果
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

阻塞算子会强制所在 pipeline 使用单 driver，确保全局结果正确：

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
- **PipelineSplitDemo**：验证单 pipeline + 单 driver 的阻塞算子执行（`Values -> Filter -> OrderBy`）。
- **TwoHashJoinDemo**：验证同一计划包含两个 HashJoin 的 build/probe 流程。
- **MultiSplitScanDemo**：验证单 scan 结点通过 addSplit/noMoreSplits 读取多个文件分片。

## 10. 可扩展方向

若进一步逼近 Velox，可在后续补充：

- 多分支计划（如 Join 多输入）。
- 聚合的 partial/final pipeline 拆分。
- Partitioned output pipeline。
- 按算子限制动态确定 driver 数量。
