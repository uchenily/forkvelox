# Project Context: MiniVelox

## Project Overview
**MiniVelox** is a lightweight, educational re-implementation of the **Meta Velox** database execution engine. It allows developers to understand the internal architecture of a vectorized execution engine by stripping away heavy dependencies (like Folly, Thrift, ProtoBuf) and focusing on the core logic using modern C++23.

It implements a vertical slice of a database engine, including:
*   **Memory Management:** Hierarchical memory pools.
*   **Type System:** SQL types (INTEGER, BIGINT, VARCHAR, ROW) and optimizations like `StringView`.
*   **Vectorized Data:** `FlatVector`, `RowVector`, and `SelectivityVector` for batch processing.
*   **Expression Engine:** A recursive descent SQL parser and a vectorized expression evaluator (`ExprSet`, `VectorFunction`).
*   **Execution Engine:** A driver-operator pipeline supporting filtering, projection, aggregation (global & groupBy), sorting (`OrderBy`), `TopN`, and `HashJoin`.

## Architecture & Structure
The project structure mirrors the original Velox codebase to facilitate learning:

*   `minivelox/`
    *   `demo/`: Contains the main entry point `VeloxIn10MinDemo.cpp`, which replicates the official Velox tutorial queries.
    *   `folly/`: Minimal shims/stubs for Facebook's Folly library to avoid external dependencies.
    *   `velox/`
        *   `common/`: Basic utilities (Memory, Exceptions).
        *   `core/`: Core interfaces (`PlanNode`, `QueryCtx`, `ITypedExpr`).
        *   `exec/`: Execution logic (`Driver`, `Operator`, `Task`).
        *   `expression/`: Expression evaluation logic (`Expr`, `VectorFunction`).
        *   `parse/`: A hand-written recursive descent SQL parser.
        *   `tpch/`: TPC-H data generation helpers.
        *   `type/`: Type system definitions and `Variant`.
        *   `vector/`: Vector data layout implementations.

## Building and Running

The project uses **CMake** (3.20+) and defaults to **C++23**. **Ninja** is the recommended build generator.

### Prerequisites
*   Linux environment
*   C++23 compatible compiler (GCC 14+ / Clang 16+)
*   CMake & Ninja

### Build Commands
Run the following from the `minivelox` directory:

```bash
mkdir -p build && cd build
cmake -GNinja ..
ninja
```

### Running the Demo
The primary artifact is the demo executable, which runs a series of TPC-H inspired queries (Filter, Aggregation, Join, Sort, TopN).

```bash
# From the build directory
./VeloxIn10MinDemo
```

## Development Conventions
*   **Standard:** Strict C++23 usage.
*   **No External Deps:** Do not add heavy libraries. Use the provided `folly` stubs or standard library equivalents.
*   **Memory Safety:** Use `MemoryPool` for allocations. Use `StringView` carefully (it is a non-owning reference).
*   **Vectorization:** Operations should process data in batches (`RowVector` / `FlatVector`).
*   **Operator Contract:** Operators must implement `addInput()`, `getOutput()`, `noMoreInput()`, `isFinished()`, and `needsInput()` correctly to support the pull-based `ExecutionDriver`.
