# ForkVelox

ForkVelox 是 Meta Velox 执行引擎的一个重新实现版本，使用现代 C++23 标准编写。完全由AI驱动。

本项目旨在通过从零实现核心组件，深入探索和展示向量化数据库执行引擎的内部架构与工作原理。它严格遵循 Velox 的原始设计理念（如内存池架构、向量化数据结构、表达式求值模型、算子生命周期等），但在实现上进行了简化，移除了 Folly、Thrift、ProtoBuf 等重型第三方依赖，使其轻量级且易于学习。

## ✨ 核心特性

### 1. 内存管理 (Memory)
- 实现了层级化的 **MemoryPool** 和 **MemoryAllocator**。
- 支持内存追踪与统计。

### 2. 类型系统 (Type System)
- 支持 SQL 标准类型：`INTEGER`, `BIGINT`, `VARCHAR`, `ROW`。
- **StringView 优化**：实现了 Velox 特有的字符串视图，支持短字符串内联 (Small String Optimization) 和非所有权引用，极大减少内存分配。
- **Variant**：支持异构类型的动态容器。

### 3. 向量化数据结构 (Vectors)
- **BaseVector**：所有向量的基类，支持统一的接口。
- **FlatVector**：处理定长（int64）和变长（StringView）数据的扁平向量，支持 Null 值位图。
- **RowVector**：支持嵌套结构的行向量，用于表示表数据。
- **SelectivityVector**：高效的行选择掩码，用于算子间的批量数据过滤。

### 4. 表达式引擎 (Expression Engine)
- **SQL 解析器**：内置手写的递归下降解析器，支持算术运算、比较运算、函数调用等语法。
- **类型推导**：基于输入 Schema 自动推导表达式树的类型。
- **向量化计算**：实现了 `ExprSet` 和 `VectorFunction` 框架，支持 `plus`, `multiply`, `mod`, `eq`, `substr`, `upper`, `concat` 等函数。

### 5. 执行引擎 (Execution Engine)
- **Pipeline 架构**：实现了基于 **Driver** 和 **Operator** 的 Pull+Push 模型执行流。
- **算子实现**：
    - **Values**: 数据源算子。
    - **Filter**: 向量化过滤器。
    - **Project**: (隐式支持) 表达式投影。
    - **Aggregation**: 支持 Global Aggregation 和 GroupBy 分组聚合 (`sum`, `count`, `avg`)。
    - **OrderBy**: 基于多列排序键的全量排序。
    - **TopN**: 高效的 Top-K 查询。
    - **HashJoin**: 支持 Inner Join（Build-Probe 机制）。
- **TPC-H 支持**: 内置了 TPC-H Nation 和 Region 表的数据生成与查询支持。

## 🛠️ 项目结构

```text
forkvelox/
├── demo/               # 演示程序 (VeloxIn10MinDemo)
├── folly/              # Folly 库的轻量级兼容桩代码
├── velox/
│   ├── common/         # 基础库 (Exception, Memory)
│   ├── core/           # 核心接口 (PlanNode, QueryCtx, Expressions)
│   ├── exec/           # 执行引擎 (Driver, Operator, Task)
│   ├── expression/     # 表达式求值 (Expr, VectorFunction)
│   ├── parse/          # SQL 解析器
│   ├── tpch/           # TPC-H 数据生成
│   ├── type/           # 类型系统 (Type, Variant, StringView)
│   └── vector/         # 向量结构 (BaseVector, FlatVector)
└── CMakeLists.txt      # 构建配置
```

## 🚀 快速开始

### 环境要求
*   Linux 环境
*   C++23 兼容编译器 (GCC 14+ / Clang 16+)
*   CMake 3.20+
*   Ninja 构建系统

### 编译与运行

1.  **进入项目目录**:
    ```bash
    cd forkvelox
    ```

2.  **构建项目**:
    ```bash
    cmake -GNinja -Bbuild
    cmake --build build
    ```
    或使用 `just`:
    ```bash
    just setup
    just build
    ```

3.  **运行演示**:
    该演示程序完美复刻了 Velox 官方的 "10分钟上手" 教程中的所有查询案例。
    ```bash
    ./VeloxIn10MinDemo
    ```

## 📚 设计参考

本项目是 Meta Velox 的学习性实现，设计上严格参考了 [Meta Velox](https://github.com/facebookincubator/velox) 的源码。主要的差异在于移除了生产环境所需的复杂特性（如 Spill, SIMD 优化, 复杂 IO 适配器等），专注于核心执行流和数据流的正确性与架构清晰度。
