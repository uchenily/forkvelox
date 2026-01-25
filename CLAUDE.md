# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This repository contains two related projects:

1. **velox/** - The upstream Meta Velox execution engine (reference only, do NOT modify)
2. **src/** - MiniVelox/ForkVelox - A lightweight educational re-implementation in C++23

### MiniVelox (src/)

MiniVelox is a simplified but architecturally faithful re-implementation of Meta Velox's execution engine. Key points:
- Written in modern C++23 (no Folly, Thrift, or ProtoBuf dependencies)
- Maintains Velox's core architectural patterns (Driver-Operator model, vectorized execution, memory pool hierarchy)
- Uses `justfile` for build commands (located at project root)

**IMPORTANT**: Never simplify core data structures for convenience. For example:
- Use `StringView` (non-owning reference with inline optimization), NOT `std::string`
- Maintain proper vector encodings (Flat, Dictionary, Constant, Sequence)
- Follow the original Driver-Operator contract precisely (neither pure push nor pure pull)

### Upstream Velox (velox/)

Reference implementation. Read for understanding architecture, but **never modify files in this directory**.

## Build Commands

### MiniVelox (src/)

```bash
# Setup (configure CMake with Ninja)
just setup

# Build
just build
# OR equivalently:
cmake -GNinja -B build
cmake --build build

# Run the demo
./build/src/VeloxIn10MinDemo
```

### Upstream Velox (velox/)

```bash
cd velox

# Build debug version
make debug

# Build release version
make release

# Build and run unit tests
make unittest

# Run a specific test
cd _build/debug
ctest -R <TestName> -VV

# Run expression fuzzer
make fuzzertest

# Minimal builds (faster compile, fewer components)
make minimal          # Release minimal build
make min_debug        # Debug minimal build
make dwio             # Minimal with DWIO enabled
```

## Architecture Overview

### Core Components

| Component | Location (src/) | Purpose |
|-----------|-----------------|---------|
| **Memory** | `velox/common/memory/` | Hierarchical MemoryPool with tracking |
| **Type System** | `velox/type/` | SQL types, Variant, StringView (inline-optimized) |
| **Vectors** | `velox/vector/` | BaseVector, FlatVector, RowVector, SelectivityVector |
| **Expressions** | `velox/expression/` | Expr, ExprSet, VectorFunction framework |
| **Parser** | `velox/parse/` | Recursive descent SQL parser |
| **Operators** | `velox/exec/` | Filter, Project, Aggregation, OrderBy, TopN, HashJoin |
| **Driver** | `velox/exec/Driver.h` | Execution thread pulling from operator pipeline |
| **Task** | `velox/exec/Task.h` | Manages multiple Drivers for a PlanFragment |

### Driver-Operator Model

Velox uses a hybrid execution model (neither pure push nor pure pull):

```
Task: Contains multiple Drivers executing a PlanFragment
  |
  +-- Driver: Single thread executing an operator pipeline
       |
       +-- Operator chain: Op1 -> Op2 -> Op3 -> ...
            Each operator implements:
            - addInput(Input): Receive data from upstream
            - getOutput(): Produce result batch
            - needsInput(): Signal readiness for more data
            - noMoreInput(): Signal end of stream
            - isFinished(): Check completion
```

Key Velox states from `velox/exec/Driver.h`:
- `kNone`: Keep running
- `kPause`: Go off thread, don't reschedule
- `kTerminate`: Stop and free (first receiver frees state)
- `kYield`: Go off thread, requeue to runnable queue
- `kBlock`: Wait for external event (I/O, memory, etc.)
- `kAtEnd`: No more data to produce

### Vector Encodings

Located in `velox/vector/vector/src/vector/`:
- **FlatVector**: Contiguous array with optional nulls
- **DictionaryVector**: Index-based deduplication
- **ConstantVector**: Single value repeated
- **SequenceVector**: RLE/run-length encoded
- **RowVector**: Struct of child vectors
- **ArrayVector/MapVector**: Nested collections

### Expression Evaluation

Pipeline: SQL string → ParsedExpr → TypedExpr → ExprSet → VectorPtr

1. **Parser**: `velox/parse/` produces untyped AST
2. **Type Inference**: `core::Expressions::inferTypes()` produces TypedExpr
3. **Compilation**: `exec::ExprSet` compiles TypedExpr for vectorized execution
4. **Evaluation**: `eval(rows, ctx, results)` processes batches

### Memory Management

- `memory::MemoryPool`: Hierarchical, tracked allocations
- `StringView`: 12-byte struct with inline storage (≤12 chars embedded inline)
- `Arena`/`HashStringAllocator`: Efficient temporary string storage

## Testing

### Upstream Velox Tests

Test files follow pattern: `velox/velox/<module>/tests/*Test.cpp`

Common test patterns:
- `VectorTestBase`: Base for vector tests with makers
- `OperatorTestBase`: Base for operator tests
- `PlanBuilder`: Fluent API for constructing query plans

Example test structure:
```cpp
class MyTest : public OperatorTestBase {
  void testSomething() {
    auto plan = PlanBuilder()
        .values(vectors)
        .filter("a > 5")
        .project({"a + b AS c"})
        .planNode();

    AssertQueryBuilder(plan).assertResults(expected);
  }
};
```

### Running Specific Tests

```bash
# From velox/ directory
cd _build/debug

# Run specific test
ctest -R MyTestName -VV

# Run all tests in a directory
ctest -R vector -VV

# List all tests
ctest -N
```

## Development Guidelines

### Coding Style (from velox/CODING_STYLE.md)

- **Naming**:
  - PascalCase for types and files
  - camelCase for functions/variables
  - camelCase_ for private/protected members
  - snake_case for namespaces
  - kPascalCase for constants

- **Constants**: Use `constexpr std::string_view` for string literals, NOT `std::string`

- **Error handling**:
  - `VELOX_CHECK_*` for internal errors
  - `VELOX_USER_CHECK_*` for user errors
  - `VELOX_FAIL()`/`VELOX_USER_FAIL()` for throwing
  - `VELOX_UNREACHABLE()` for unreachable code

- **Function arguments**:
  - `const T&` for input (non-trivial types)
  - `T&&` (by value, not rvalue ref) for sink parameters
  - `std::string_view` instead of `const std::string&`

### Commit Message Format (from velox/CONTRIBUTING.md)

Follow conventional commits:
```
<type>[(scope)]: <description>

[Body]

[Footer]
```

Types: `feat`, `fix`, `perf`, `build`, `test`, `docs`, `refactor`, `misc`

Example: `feat(expr): Add support for modulo operator`

### MiniVelox Development Principles

1. **No external dependencies** - Use standard library or provided folly stubs
2. **Architectural fidelity** - Match upstream Velox design patterns
3. **C++23 idioms** - Use modern features where appropriate
4. **Extensive logging** - Show execution pipeline flow for debugging
5. **Incremental commits** - Keep changes small (~300 lines per commit)

## Key Files for Understanding

| Purpose | File |
|---------|------|
| Demo/tutorial | `velox/velox/exec/tests/VeloxIn10MinDemo.cpp` |
| Driver execution | `velox/velox/exec/Driver.h` |
| Task management | `velox/velox/exec/Task.h` |
| Operator base | `velox/velox/exec/Operator.h` |
| Plan nodes | `velox/velox/core/PlanNode.h` |
| Test builder | `velox/velox/exec/tests/utils/PlanBuilder.h` |
| Expression eval | `velox/velox/expression/Expr.h` |
| Memory system | `velox/velox/common/memory/MemoryAllocator.h` |
