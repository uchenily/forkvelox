default: examples

build:
    cmake --build build -j 32
setup:
    cmake -G Ninja -B build

examples:
    cmake --build build -j 32 --target run-examples
    # ctest --test-dir build --output-on-failure

q6:
    cmake --build build -j 32 --target tpch_q6_check
