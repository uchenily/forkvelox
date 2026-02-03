# Expression

本文档基于 ForkVelox 现有实现，并对照上游 Velox 表达式体系进行说明，覆盖：
- ForkVelox 当前表达式模块的功能范围
- 与原始实现的差异点
- 后续需要改进的方向

## 概览
ForkVelox 的表达式模块是一个可运行的最小子集，提供：
- 表达式树编译与执行（`Expr` / `ExprSet`）
- 变量引用、常量、函数调用
- 轻量级 lambda 表达式
- 向量函数注册表

目标是支撑 demo 与基础查询执行，而非完整的生产级表达式引擎。

## 已实现组件（功能细节）

**Expr**
作用：表达式节点基类。
功能：
- `eval()` 虚接口，接收 `SelectivityVector` 与 `EvalCtx` 输出结果向量。
- 保存 `type_`、`inputs_`、`name_`。
- `toString()` 生成表达式文本表示。
限制：
- 统计信息为 stub，未追踪真正执行成本。

**ExprSet**
作用：一组表达式的编译与执行容器。
功能：
- 从 `core::ITypedExpr` 编译得到 `Expr` 树。
- `eval()` 逐个表达式求值。
- `toString()` 支持树形打印（无统计）。
限制：
- 未实现共享子表达式缓存、分组评估或 reuse 策略。

**ConstantExpr**
作用：常量表达式。
功能：
- 支持 `INTEGER` 和 `BIGINT` 常量。
- 通过 `FlatVector` 生成输出向量。
限制：
- 其它类型常量未支持（`VARCHAR`、数组、结构等）。
- 无常量复用或 ConstantVector 优化。

**FieldReference**
作用：字段引用表达式。
功能：
- 按列名从输入 `RowVector` 中取列。
限制：
- 线性扫描列名。
- 不支持子字段与嵌套字段访问。

**CallExpr**
作用：函数调用表达式。
功能：
- 递归计算子表达式，调用 `VectorFunction`。
限制：
- 缺少函数签名匹配、隐式类型转换、异常处理与默认空值规则。

**LambdaExpr**
作用：lambda 表达式。
功能：
- 根据签名构造 `FunctionVector`。
- 支持捕获外层列（按列名捕获）。
- 通过 `LambdaCallable` 在每次调用时构建 lambda 行向量。
限制：
- 仅支持 `BIGINT`、`INTEGER`、`VARCHAR` 的捕获与重复向量构造。
- 捕获与行构造为 per-call，缺少复用与优化。

**VectorFunction / FunctionRegistry**
作用：向量函数接口与注册表。
功能：
- 以函数名注册 `VectorFunction`。
- 运行时按名称分发。
限制：
- 无元数据描述（空值语义、确定性、可折叠等）。
- 无多重签名或重载。

**EvalCtx**
作用：表达式求值上下文。
功能：
- 提供 `ExecCtx`、输入 `RowVector` 与内存池。
限制：
- 缺少错误处理、上下文变量、状态与诊断信息。

## 与原始实现的差异点

- 功能范围
  - 原始实现覆盖丰富的表达式类型、特殊形式（IF/CASE/COALESCE 等）与重写优化；ForkVelox 仅支持常量、字段引用、函数调用和 lambda。

- 类型系统与签名
  - 原始实现支持函数签名匹配、重载、多态与隐式转换；ForkVelox 直接按名称分发函数。

- 空值与错误语义
  - 原始实现有统一的空值传播、异常处理和 `try` 语义；ForkVelox 未实现。

- 性能与优化
  - 原始实现包含字典/常量/懒加载向量剥离、表达式重写、共享子表达式优化与统计信息；ForkVelox 基本缺失。

- Lambda 支持
  - 原始实现包含更完整的 lambda 体系、捕获处理与共享表达式重置；ForkVelox 仅提供最小可运行版本。

## 需要改进的地方（建议优先级）

1. 完善表达式类型覆盖
2. 引入函数签名与重载机制
3. 建立空值与错误语义
4. 增加 `ConstantVector` 与 `DictionaryVector` 支持
5. 实现表达式重写与共享子表达式优化
6. 优化 lambda 的捕获与执行路径
7. 添加统计信息与执行诊断

## 备注

- 目前表达式模块定位为“可运行最小子集”，以演示执行流程为主。
- 如果后续计划引入更多函数与复杂查询，建议优先补齐空值语义、函数签名与常量优化。
