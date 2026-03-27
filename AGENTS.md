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
  cmake --build build -j 32
  ```
- Run all examples:
  ```bash
  just examples
  ```

## Coding Style
- Language: C++23.
- Indentation: 2 spaces for C++ code in `src/`.
- Prefer small, focused files; keep headers in `src/velox/**` and examples in `examples/`.

## Naming Conventions

- **PascalCase** for types and file names.
- **camelCase** for functions, member and local variables.
- **camelCase_** for private and protected member variables.
- **snake_case** for namespace names and build targets.
- **UPPER_SNAKE_CASE** for macros.
- **kPascalCase** for static constants and enumerators.
- Do not abbreviate. Use full, descriptive names. Well-established abbreviations (`id`, `url`, `sql`, `expr`) are acceptable.
- Prefer `numXxx` over `xxxCount` (e.g. `numRows`, `numKeys`).
- Never name a file or class `*Utils`, `*Helpers`, or `*Common`. These generic
  names attract unrelated functions over time and lose cohesion. Name files and
  classes after the concept they represent. Use a class with static methods to
  group related operations, and shorten method names since the class name
  provides context

## Variables

- Prefer value types, then `std::optional`, then `std::unique_ptr`.
- Prefer `std::string_view` over `const std::string&` for function parameters.
- Use uniform initialization: `size_t size{0}` over `size_t size = 0`.
- Declare variables in the smallest scope, as close to usage as possible.
- Use digit separators (`'`) for numeric literals with 4 or more digits: `10'000`, not `10000`.
- Use trailing commas in multi-line initializer lists, enum definitions, and
  function-call argument lists that span multiple lines. This produces cleaner
  diffs when items are added or reordered.

## API Design

- Keep the public API surface small.
- Prefer free functions in `.cpp` (anonymous namespace) over private/static class methods.
- Define free functions close to where they are used, not grouped together at the top or bottom of the file.
- Keep method implementations in `.cpp` except for trivial one-liners.
- Avoid default arguments when all callers can pass values explicitly.
- Never use `friend`, `FRIEND_TEST`, or any friend declarations. If a test needs access to private members, redesign the API or test through public methods instead.

## Design Documents

Design (including proposals) live in `docs/designs/`.  When creating new
designs, place them there with a descriptive filename

## Testing Guidelines
- No formal test framework wired in this repo.
- Add or update examples under `examples/` to validate new features.

## Commit & Pull Request Guidelines
- No explicit commit message convention is documented in this repo.
- For PRs (if used), include: summary of changes, demo output (if applicable), and any new build/run steps.

## Architecture Notes
- io_uring 处理异步IO
- stdexec 实现异步运行时
- `velox/` 目录是上游参考代码，默认按只读参考处理
