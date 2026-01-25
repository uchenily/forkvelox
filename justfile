build:
    cmake --build build
setup:
    cmake -G Ninja -B build

run:
    ./build/src/VeloxIn10MinDemo
