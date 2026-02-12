# 动态加载（Dynamic Loading）

本文档描述 ForkVelox 中动态加载 UDF/扩展的实现方式、约定与示例。

## 目标与约定
- 通过共享库在运行时注册函数，无需重新链接主程序。
- 共享库需要提供一个 C 接口入口函数（默认名为 `registerExtensions`）：

```cpp
extern "C" void registerExtensions();
```

- 主程序通过 `facebook::velox::loadDynamicLibrary(path, entryPoint)` 加载并执行注册逻辑。

入口函数位置：`src/velox/common/dynamic_registry/DynamicLibraryLoader.{h,cpp}`。

## 示例（仓库内）
本仓库包含完整示例：
- 共享库：`examples/dynamic/DynamicFunction.cpp`
- 主程序：`examples/DynamicLoading.cpp`

### 共享库代码（注册函数）

```cpp
extern "C" {
void registerExtensions() {
  facebook::velox::exec::registerFunction(
      "dynamic", std::make_shared<facebook::velox::dynamic_example::DynamicFunction>());
}
}
```

### 主程序代码（加载库）

```cpp
const std::string libraryPath = VELOX_DYNAMIC_EXAMPLE_LIB_PATH;
loadDynamicLibrary(libraryPath);
```

### 构建与运行
```bash
just build
./build/examples/DynamicLoading
```

## 构建注意事项

### 1. 共享库与注册表的一致性
在 ForkVelox 中，函数注册表位于 `velox_common` 静态库内。
如果共享库也静态链接 `velox_common`，会产生 **两份独立的注册表**，
导致 `loadDynamicLibrary` 注册成功但主程序查不到函数。

本仓库的处理方式：
- 共享库 **不链接** `velox_common`。
- 主程序导出符号供共享库解析。

对应 CMake 逻辑：`examples/CMakeLists.txt`。

### 2. 导出主程序符号
Linux 下需要导出主程序符号（使 `dlopen` 的库能解析到 `registerFunction` 等符号）：

- `DynamicLoading` 目标使用 `-Wl,-export-dynamic`。

macOS 下：
- `DynamicLoading` 使用 `-Wl,-export_dynamic`。
- 共享库使用 `-Wl,-undefined,dynamic_lookup`。

### 3. 共享库必须使用 PIC
如果遇到 `recompile with -fPIC` 的报错，需要确保静态库使用 PIC。
本仓库已在 `src/CMakeLists.txt` 中设置：

```cmake
set_target_properties(velox_common PROPERTIES POSITION_INDEPENDENT_CODE ON)
```

## 常见错误排查

- **`dynamic() was not registered after loading.`**
  通常是共享库链接了 `velox_common`，导致注册表不一致。
  确保共享库不链接 `velox_common`，并且主程序导出符号。

- **`recompile with -fPIC`**
  说明静态库非 PIC。确保 `velox_common` 设置了 `POSITION_INDEPENDENT_CODE ON`。

## 扩展建议
- 如果需要自定义入口函数名，可调用：
  `loadDynamicLibrary(path, "customEntry")`。
- 如需在表达式/SQL 层使用动态函数，可在加载后通过 `exec::getVectorFunction` 或表达式系统进行调用。
