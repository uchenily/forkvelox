# Repository Guidelines

## Project Structure & Module Organization
- `src/`: ForkVelox implementation and examples.
  - `src/velox/`: core engine modules (exec, type, vector, dwio, common).
- `velox/`: upstream Velox reference sources and docs (read-only reference).
- `examples/`: runnable examples.
- `build/`: CMake/Ninja build output.
- `docs/`: project documentation.

## Build, Test, and Development Commands
- Configure build (from `build/`):
  ```bash
  just setup
  ```
- Build targets:
  ```bash
  just build
  ```
- Run all examples:
  ```bash
  just examples
  ```

## Coding Style & Naming Conventions
- Language: C++23.
- Indentation: 2 spaces for C++ code in `src/`.
- Naming: `CamelCase` for types, `lowerCamelCase` for methods, `kConstant` for constants.
- Prefer small, focused files; keep headers in `src/velox/**` and examples in `examples/`.

## Testing Guidelines
- No formal test framework wired in this repo.
- Add or update examples under `examples/` to validate new features.

## Commit & Pull Request Guidelines
- No explicit commit message convention is documented in this repo.
- For PRs (if used), include: summary of changes, demo output (if applicable), and any new build/run steps.

## Architecture Notes
Refer to Velox
