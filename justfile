build:
    cmake --build build
setup:
    cmake -G Ninja -B build

examples:
    cmake --build build --target run-examples
    # ctest --test-dir build --output-on-failure
