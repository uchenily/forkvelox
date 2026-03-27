# ForkVelox

ForkVelox 是一个以 Meta Velox 为参考的学习型全异步执行引擎实现，使用 C++23 编写。使用 `stdexec` 作为基础异步编程模型框架，并引入 `liburing` 作为 Linux 异步 I/O 基础设施。

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
just build
```

## 运行示例

常用示例：

```bash
./build/examples/VeloxIn10Min
./build/examples/MultiSplitScan
./build/examples/TwoHashJoin
./build/examples/ScanAndSort
./build/examples/ScanFvx
```

所有示例：

```bash
just examples
```

## 参考

设计和接口语义主要参考 [Meta Velox](https://github.com/facebookincubator/velox)，但实现目标偏向学习、验证和小型可运行系统，而不是直接复刻生产特性。
