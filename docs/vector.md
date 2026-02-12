# Vector

本文档基于 ForkVelox 现有实现，并对照上游 Velox 实现进行说明，覆盖：
- ForkVelox 当前的 Vector 体系概览
- 与上游实现的主要差异点
- 后续需要改进的方向

## 概览
ForkVelox 的 Vector 体系是一个“最小可用子集”，用于支撑示例与基础执行流程。当前仅实现少量向量类型（`FlatVector<T>`、`ArrayVector`、`RowVector`、`FunctionVector`），`BaseVector` 与 `SimpleVector<T>` 仅保留基本的 `toString`、`compare`、`copy` 框架与空值判断。复杂特性如 Lazy、Dictionary、Constant、Sequence、Map、FlatMap、Decoded 等均未实现。

当前实现位置：
- `src/velox/vector/BaseVector.h`
- `src/velox/vector/SimpleVector.h`
- `src/velox/vector/FlatVector.h`
- `src/velox/vector/ComplexVector.h`
- `src/velox/vector/FunctionVector.h`
- `src/velox/vector/SelectivityVector.h`
- `src/velox/vector/VectorEncoding.h`

## 已实现的 Vector（功能细节）

**BaseVector**
作用：所有向量类型的基类，提供最小共通接口与基础字段。
功能：
- 维护 `type_`、`encoding_`、`nulls_`、`length_` 与 `pool_`。
- `isNullAt()` 基于 `nulls_` 位图判断空值。
- `toSummaryString()` 输出 `[ENCODING TYPE: N elements, X nulls]` 风格摘要。
- `toString()` 输出摘要 + 逐行值。
- `loadedVector()` 直接返回自身（不支持 lazy 语义）。
限制：
- 缺少 `containsNullAt()`、`mayHaveNulls()`、统计信息等能力。
- 空值位图语义尚未严格澄清（详见改进建议）。

**SimpleVector<T>**
作用：标量向量基类，为 `FlatVector<T>` 等提供统一接口。
功能：
- 定义 `valueAt()` 虚接口。
- `compare()` 使用 `valueAt()` 进行逐值比较。
- `toString()` 基于 `valueAt()` 序列化。
限制：
- `copy()` 为占位实现（不写入，依赖子类覆盖）。
- 无统计信息、hash 计算等功能。

**FlatVector<T>**
作用：最常用的“连续内存”值向量。
功能：
- `values_` 持有原始值 buffer，`rawValues_` 指向连续数组。
- `valueAt()` 直接读取 `rawValues_`。
- `copy()` 支持单值复制到目标索引。
- 简化的 `stringBuffers_` 允许追加字符串 buffer。
限制：
- 缺少 `mutableValues()` / `mutableRawValues()` 的安全写入路径。
- `StringView` 生命周期仅做最小假设（无字符串内存托管策略）。
- 无批量填充、SIMD、统计信息等优化。

**ArrayVector**
作用：表示数组列，`offsets_` + `sizes_` + `elements_` 组合。
功能：
- `offsetAt()` / `sizeAt()` 读取子数组边界。
- `toString()` 输出元素列表（截断模式）。
- `compare()` 逐元素比较。
限制：
- `copy()` 未实现。
- 未提供可写路径与空值传播。

**RowVector**
作用：结构化行向量，持有多个子列。
功能：
- `toString()` 输出 `{col1, col2, ...}`，并带 schema 前缀。
- `compare()` 与 `copy()` 按列迭代处理。
- `children()` / `childAt()` 提供子列访问。
限制：
- 无 `containsNullAt()` 递归传播逻辑。
- 无 schema 与子列一致性校验。

**FunctionVector**
作用：表示 lambda/函数列，用于向量化函数应用。
功能：
- 保存 `Callable` 与其适用的 `SelectivityVector`。
- `iterator()` 根据 rows 交集返回可执行条目。
限制：
- `compare()` / `copy()` 未支持。
- 仅支持最基本的 rowSet 管理。

**SelectivityVector**（辅助类型）
作用：行选择位图，非真正数据向量，但在执行与函数中大量使用。
功能：
- 位图存储选中行，支持 `intersect()`、`intersects()`、`applyToSelected()`。
限制：
- 线性扫描为主，缺少范围迭代与优化路径。

**VectorEncoding**（辅助类型）
作用：标记向量编码类型。
功能：
- 枚举值包含 `BIASED`、`CONSTANT`、`DICTIONARY` 等。
限制：
- 多数编码在 ForkVelox 中尚未有实际向量实现。

## 待实现的 Vector（应具备的功能点）

以下类型在 ForkVelox 中尚未实现，但 `VectorEncoding` 已预留编码枚举。建议作为后续补齐的功能清单：

**ConstantVector**
预期功能：
- 表示全列为同一常量值（或全 null）。
- 支持常量与 nulls 的快速判断，减少值 buffer 访问。
价值：
- 常见于表达式常量折叠、投影常量列。

**DictionaryVector**
预期功能：
- 通过 index buffer 对 base vector 做重映射。
- 支持选择子集、过滤与重复引用。
价值：
- 高效表达过滤、排序、投影后的“行重排”。

**LazyVector**
预期功能：
- 持有 loader，在访问时按需加载底层向量。
- `loadedVector()` 返回已加载向量。
价值：
- IO 与计算按需执行，减少无用加载。

**SequenceVector**
预期功能：
- 表示规则数列，如 `start + step * i`。
- 快速生成整数或时间序列。
价值：
- 聚合、窗口、测试与生成类场景。

**MapVector**
预期功能：
- 保存 key/value 子向量 + offsets/sizes。
- 支持空值传播与 key/value 对齐校验。

**FlatMapVector**
预期功能：
- 针对稀疏结构的扁平化 map 存储。
- 维护子字段到向量的映射关系。

**BiasVector**
预期功能：
- 对整数值做偏移编码，减少存储或提升压缩性。

**DecodedVector**
预期功能：
- 将不同编码统一解码为可直接访问的形式。
价值：
- 执行期算子与函数可不关心底层编码。

## 与上游实现的主要差异点

- Vector 类型覆盖范围：上游含 `LazyVector`、`DictionaryVector`、`ConstantVector`、`SequenceVector`、`MapVector`、`FlatMapVector`、`BiasVector` 等，ForkVelox 仅有 `Flat/Array/Row/Function`。
- BaseVector 语义：上游实现了 `mayHaveNulls()`、`containsNullAt()`、统计信息与安全类型转换；ForkVelox 未实现。
- SimpleVector/FlatVector：上游提供更完善的写入路径、值缓冲区复用、统计信息与一致性校验；ForkVelox 仅保留最小读写。
- ComplexVector：上游 `RowVector` 有 child null 传播与更丰富的 toString/summary；ForkVelox 简化。
- FunctionVector：上游具备更完整的 lambda 体系支持与执行集成；ForkVelox 为轻量版本。
- SelectivityVector：上游支持高效位图扫描与范围迭代；ForkVelox 以 O(n) 扫描为主。

## 需要改进的地方（建议优先级）

1. 明确空值语义与位图规则
2. 补齐 BaseVector 基础能力（`mayHaveNulls()`、`containsNullAt()`、统计信息）
3. 完善 `FlatVector<T>` 的可写与内存管理
4. 完成 `SimpleVector<T>::copy()` 抽象语义
5. Array/Row Vector 写入支持与空值传播
6. 补齐常见编码类型（`ConstantVector`、`DictionaryVector`、`LazyVector`、`SequenceVector`）
7. SelectivityVector 与 FlatVector 的性能优化

## 备注

- 以上差异与改进建议均以上游 Velox 作为参考基线，保持“轻量可运行”与“逐步向上游对齐”的平衡。
- 如果后续计划引入更多算子或 IO 格式，建议优先实现 `ConstantVector` 与 `DictionaryVector`，它们是表达式执行与过滤/投影的常见基础类型。


**参考**

https://facebookincubator.github.io/velox/develop/vectors.html
