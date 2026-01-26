# 逻辑计划到执行计划：Pipeline 拆分与并行驱动

本文档总结当前 ForkVelox 的 pipeline 拆分与执行方式，参考 Velox 的 LocalPlanner 模型。

## 1. 核心概念

- **Plan Fragment**：可在一个 Task 中执行的 PlanNode 子树。
- **Pipeline**：一条线性算子链，通常在 LocalMerge、非第 0 个 source、或 Join build 边界处分割。
- **Driver**：执行一个 pipeline 的线程实例，同一 pipeline 可由多个 driver 并行执行。
- **Local Exchange**：进程内队列，用于把上游 pipeline 的输出传给下游 pipeline（通过 LocalPartition/LocalMerge 连接）。

## 2. PlanNode 到 Pipeline

### 示例：OrderBy partial/final

```
Values -> Filter -> OrderBy(partial) -> LocalPartition -> LocalMerge -> OrderBy(final)
```

拆分结果：

```
Pipeline 0: Values -> Filter -> OrderBy(partial) -> LocalExchangeSink
Pipeline 1: LocalExchangeSource -> OrderBy(final)
```

### 示例：HashJoin build/probe

```
ProbePlan -> HashJoin
BuildPlan
```

拆分结果：

```
Pipeline 0 (build): BuildPlan -> HashBuild
Pipeline 1 (probe): ProbePlan -> HashProbe
```

## 3. Pipeline 拆分规则

当前实现遵循 LocalPlanner 的核心思路：

- **按输入源拆分**：同一 PlanNode 的非第 0 个 source 会启动新 pipeline。
- **LocalMerge 边界**：LocalMerge 强制新 pipeline，和上游 LocalPartition 形成 Local Exchange。
- **Join build pipeline**：HashJoin 的 build side 会生成独立 pipeline，尾部追加 HashBuildOperator。

## 4. Pipeline 依赖与调度

- **Join 依赖**：probe pipeline 依赖 build pipeline 完成。
- **LocalExchange 依赖**：consumer pipeline 依赖 producer pipeline 完成。
- **调度方式**：按依赖拓扑顺序启动 pipelines。

## 5. Driver 并行度

- 默认按 `maxDrivers` 并行运行。
- 下列算子会强制单 driver：
  - `OrderBy`（final）
  - `TopN`
  - `Aggregation`（final/single）
  - `TableWrite`

## 6. Aggregation 的步骤

Aggregation 支持以下步骤：

- `Single`：单阶段聚合，强制单 driver。
- `Partial`：输出中间结果（sum/count）。
- `Intermediate`：继续聚合中间结果。
- `Final`：从中间结果输出最终结果。

Avg 的中间列命名规则：

- `avg(x) AS avg_col` 在 partial/intermediate 输出为 `avg_col_sum` 与 `avg_col_count`。

## 7. Task 执行流程

`Task::run()` 的核心流程：

1. 收集 HashJoin bridges 与 split 信息。
2. LocalPlanner 生成 DriverFactory 列表。
3. 创建 LocalExchange 队列并建立依赖。
4. 按依赖顺序执行 pipelines。
5. 收集 output pipeline 的结果。

## 8. Splits 与多文件扫描

`FileScanNode` 支持通过 `addSplit/noMoreSplits` 注入多个文件分片：

- 未提供 split 时使用 PlanNode 的 `path`。
- 提供 split 时依次读取多个文件并合并批次。

## 9. Demo 覆盖

- **TaskParallelDemo**：单 pipeline 多 driver 并行（`Values -> Filter`）。
- **PipelineSplitDemo**：LocalExchange 拆分 pipeline（partial/final `OrderBy`）。
- **OrderByPartialFinalDemo**：partial/final `OrderBy` 拆分与 driver 数量。
- **TwoHashJoinDemo**：多 HashJoin build/probe 流程。
- **AggregationStepDemo**：single/partial/intermediate/final 聚合。
- **MultiSplitScanDemo**：单 scan 结点多 split 读取。

## 10. 可扩展方向

- 多分支计划与更多 LocalExchange 类型。
- Partitioned output pipeline。
- 更完整的算子级并行度约束。
