# FVX 文件格式与功能说明

FVX 是一个参考 Parquet 设计的轻量列式存储格式，面向 ForkVelox 的 DWIO 读写接口。其核心目标是：

- 列式存储，按列块读写，减少无关列 IO。
- 行组(Row Group)级统计信息（min/max），支持常见谓词下推。
- 结构简单，便于在示例项目中理解与扩展。

## 适用场景

- 小型或教学型列式存储实验。
- 需要展示投影/谓词下推机制的 demo。
- 替代 CSV 读取以减少不必要的解析和 IO。

## 设计概览

FVX 文件由三部分组成：

1. 文件头：magic、版本、schema。
2. 行组元数据：每个行组的行数、列块偏移/长度、列统计。
3. 列块数据：按行组、按列顺序写入的列式数据。

### 逻辑结构

- Row Group：一批行，默认由 `rowGroupSize` 控制（写入时设置）。
- Column Chunk：每个行组内每列对应一个列块。
- Stats：每个列块保存 min/max，用于谓词下推。

## 文件布局（逻辑层）

```
| magic + version |
| schema |
| row group metadata |
| column chunks data |
```

### Schema

- 包含列名和类型。
- 当前仅支持：`BIGINT` / `INTEGER` / `VARCHAR`。

### Row Group 元数据

每个行组包含：

- rowCount
- 对每个列块：
  - offset
  - length
  - min/max（按类型存储）

### Column Chunk 数据

- BIGINT/INTEGER：原始定长数组（未压缩）。
- VARCHAR：`totalBytes + offsets + data` 的布局：
  - totalBytes：字符串数据总长度
  - offsets：`(rowCount + 1)` 个偏移
  - data：拼接的字符串字节

## 下推能力

### 投影下推

通过 `RowReaderOptions::setProjectedColumns(...)` 指定读取列，解码时仅加载所需列块。

### 谓词下推

通过 `RowReaderOptions::addFilter(...)` 设置过滤条件：

- `EQ` / `NE` / `LT` / `LE` / `GT` / `GE`
- 在行组级别使用 min/max 判断是否可能匹配
- 不匹配的行组直接跳过

注意：目前谓词下推是行组级的“粗粒度过滤”，不做行级过滤。

## 代码位置

- Writer：`velox/dwio/fvx/FvxWriter.h`、`velox/dwio/fvx/FvxWriter.cpp`
- Reader：`velox/dwio/fvx/FvxReader.h`、`velox/dwio/fvx/FvxReader.cpp`
- 注册：`velox/dwio/fvx/RegisterFvxReader.h`、`velox/dwio/fvx/RegisterFvxReader.cpp`
- 示例：`examples/ScanFvx.cpp`

## 使用示例

示例程序 `ScanFvx.cpp` 展示了：

- 写入 FVX 文件（含 row group 统计）
- 读取时设置投影列
- 添加 `price > 12`、`label != "skip"` 的谓词下推

## 限制与后续扩展

当前实现的限制：

- 仅支持三种类型：`BIGINT` / `INTEGER` / `VARCHAR`
- 不支持 NULL
- 仅使用 min/max 统计
- 不包含压缩、字典、RLE 等编码

可扩展方向：

- 增加 NULL 支持（NULL bitmap）
- 增加更多类型（DOUBLE、BOOLEAN、STRUCT 等）
- 引入编码（字典、RLE、bit-packing）
- 更细粒度的页级统计/谓词下推
- 列块压缩与过滤器（Bloom filter 等）
