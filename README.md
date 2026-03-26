# ForkVelox

ForkVelox 是一个以 Meta Velox 为参考的学习型执行引擎实现，使用 C++23 编写。当前版本已经把执行主路径切换到 `std::execution` 语义之上，底层通过 vendored `stdexec` 提供 sender/receiver 运行时能力，并引入 `liburing` 作为 Linux 异步 I/O 基础设施。

## 当前状态

- `Task::next()` 用于逐批拉取结果。
- driver 数量由引擎内部自适应决定，依据 `hardware_concurrency()` 和实际 pipeline 数量自动收敛。
- 内部异步状态由 `stdexec` 运行时和仓库内异步组件协同管理。
- 文件异步 I/O 通过通用 `IoUringExecutor` 提供，`preadvAsync()` 只是其上的一个具体请求。

## 核心组件

### 执行引擎

- 基于 `Driver + Operator + Task` 的 pipeline 执行模型。
- 支持 `Values`、`Filter`、`Aggregation`、`OrderBy`、`TopN`、`HashJoin`、`LocalExchange`、`TableWrite` 等核心算子。
- `Task::next()` 会在内部启动并发 worker，并逐批返回结果；没有更多数据时返回 `nullptr`。

### 异步与运行时

- `src/velox/core/ExecutionRuntime.*` 基于 `stdexec` 线程池调度执行任务。
- `src/velox/common/async/Async.h` 提供内部异步状态封装。
- `src/velox/common/io/IoUringExecutor.*` 是面向 io_uring 的通用提交/完成执行器。

### 存储与 I/O

- `LocalReadFile::preadvAsync()` 通过 `IoUringExecutor` 走真实 `io_uring` 提交。
- `RowVectorFile`、CSV、FVX 读写路径可直接复用当前异步文件能力。

## 项目结构

```text
forkvelox/
├── src/
│   ├── velox/common/   # 基础设施、内存、文件、io_uring
│   ├── velox/core/     # QueryCtx、ExecutionRuntime、PlanNode
│   ├── velox/exec/     # Driver、Operator、Task、LocalPlanner
│   ├── velox/dwio/     # RowVectorFile、CSV、FVX
│   ├── velox/expression/
│   ├── velox/functions/
│   ├── velox/type/
│   └── velox/vector/
├── examples/           # 可执行示例
├── thirdparties/
│   ├── stdexec/
│   └── liburing/
├── velox/              # 上游 Velox 参考代码
└── build/
```

## 构建

环境要求：

- Linux
- GCC 14+ 或 Clang 16+
- CMake 3.20+
- Ninja

初始化：

```bash
just setup
```

构建：

```bash
cmake --build build -j 32
```

## 运行示例

常用示例：

```bash
./build/examples/SerialNext
./build/examples/TaskParallel
./build/examples/MultiSplitScan
./build/examples/TwoHashJoin
./build/examples/VeloxIn10Min
```

所有示例：

```bash
just examples
```

## 参考

设计和接口语义主要参考 [Meta Velox](https://github.com/facebookincubator/velox)，但实现目标偏向学习、验证和小型可运行系统，而不是直接复刻生产特性。
