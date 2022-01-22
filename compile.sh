#!/bin/bash

rm -rf CMakeFiles/ CMakeCache.txt 
cmake -DCMAKE_BUILD_TYPE=Debug
make -j 8
