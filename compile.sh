!/bin/bash

rm -rf CMakeFiles/ CMakeCache.txt 
cmake -DCMAKE_BUILD_TYPE=Debug 
# -DCMAKE_CXX_FLAGS=-pg
make -j 8

# ./sync.bash
