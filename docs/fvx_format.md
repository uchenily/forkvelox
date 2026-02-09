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

当前实现采用与 Parquet 相近的“数据在前、Footer 在尾”组织方式（不追求二进制兼容）：

1. 文件起始 magic：`FVX3`
2. 行组列块数据（包含页）
3. Footer（schema + row group/chunk/page 元数据）
4. 文件尾部 trailer：`footerSize(uint32) + magic(FVX3)`

读取流程先从文件尾部读取 trailer，定位 footer，再据 footer 中的 offset/length 精准读取列块与页。

### 逻辑结构

- Row Group：一批行，默认由 `rowGroupSize` 控制。
- Column Chunk：每个行组内每列对应一个列块。
- Page：列块进一步切页（`pageSize`），支持页级统计跳过。
- Stats：列块与页都保存 min/max。

## 文件布局（逻辑层）

```
| prefix magic(FVX3) |
| row group data pages ... |
| footer |
| footer size (uint32) |
| suffix magic(FVX3) |
```

### Footer 内容

- version（当前固定 `3`）
- schema（列名 + 类型）
- row groups
  - rowCount
  - columns
    - chunk offset/length
    - chunk min/max
    - pages
      - startRow
      - rowCount
      - page offset/length
      - uncompressedSize/compressedSize
      - page min/max

### Page 格式

每个 page 写入格式：

1. `pageHeaderSize(uint32)`
2. `pageHeader`
   - `pageType(uint8)`（当前仅 DATA_PAGE=0）
   - `encoding(uint8)`（当前仅 PLAIN=0）
   - `rowCount(uint32)`
   - `uncompressedSize(uint32)`
   - `compressedSize(uint32)`（当前与 uncompressed 相同）
3. `payload`

当前 payload：

- BIGINT/INTEGER：原始定长数组（PLAIN）。
- VARCHAR：`totalBytes(uint32) + offsets(rowCount+1) + data`。

## 下推能力

### 投影下推

通过 `RowReaderOptions::setProjectedColumns(...)` 指定读取列，解码时仅加载所需列块。

### 谓词下推

通过 `RowReaderOptions::addFilter(...)` 设置过滤条件：

- `EQ` / `NE` / `LT` / `LE` / `GT` / `GE`
- 先在行组级别使用 min/max 判断是否可能匹配，不匹配的行组直接跳过
- 对命中的行组，先基于页级 min/max 跳过不可能命中的页
- 对候选页执行行级过滤，确保输出结果严格满足过滤条件

## 代码位置

- Writer：`src/velox/dwio/fvx/FvxWriter.h`、`src/velox/dwio/fvx/FvxWriter.cpp`
- Reader：`src/velox/dwio/fvx/FvxReader.h`、`src/velox/dwio/fvx/FvxReader.cpp`
- 注册：`src/velox/dwio/fvx/RegisterFvxReader.h`、`src/velox/dwio/fvx/RegisterFvxReader.cpp`
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
