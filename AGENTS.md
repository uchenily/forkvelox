# Repository Guidelines

## Project Structure & Module Organization
- `src/`: ForkVelox implementation and demos.
  - `src/velox/`: core engine modules (exec, type, vector, dwio, common).
  - `src/demo/`: runnable demos (e.g., `TaskParallelDemo`, `PipelineSplitDemo`).
- `velox/`: upstream Velox reference sources and docs (read-only reference).
- `build/`: CMake/Ninja build output.
- `docs/`: project documentation (e.g., `docs/exec_pipeline.md`).

## Build, Test, and Development Commands
- Configure build (from `build/`):
  ```bash
  cmake -GNinja ..
  ```
- Build targets:
  ```bash
  ninja
  ninja TaskParallelDemo
  ninja PipelineSplitDemo
  ```
- Run demos:
  ```bash
  ./build/src/TaskParallelDemo
  ./build/src/PipelineSplitDemo
  ```
- There is no dedicated test runner in this repo; demos serve as validation.

## Coding Style & Naming Conventions
- Language: C++23.
- Indentation: 2 spaces for C++ code in `src/`.
- Naming: `CamelCase` for types, `lowerCamelCase` for methods, `kConstant` for constants.
- Prefer small, focused files; keep headers in `src/velox/**` and demos in `src/demo/`.

## Testing Guidelines
- No formal test framework wired in this repo.
- Add or update demos under `src/demo/` to validate new features.
- Use `PipelineSplitDemo` for pipeline split/driver parallelism checks and `TaskParallelDemo` for single-pipeline parallelism.

## Commit & Pull Request Guidelines
- No explicit commit message convention is documented in this repo.
- For PRs (if used), include: summary of changes, demo output (if applicable), and any new build/run steps.

## Architecture Notes
- Execution uses a Velox-style model: PlanNodes -> pipelines -> drivers.
- Blocking operators (e.g., `OrderBy`) create pipeline boundaries; LocalExchange connects pipelines.
