# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.13

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/local/bin/cmake

# The command to remove a file.
RM = /usr/local/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/zqs/star

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/zqs/star

# Include any dependencies generated for this target.
include test/CMakeFiles/augmented_nn_test.dir/depend.make

# Include the progress variables for this target.
include test/CMakeFiles/augmented_nn_test.dir/progress.make

# Include the compile flags for this target's objects.
include test/CMakeFiles/augmented_nn_test.dir/flags.make

test/CMakeFiles/augmented_nn_test.dir/brain/augmented_nn_test.cpp.o: test/CMakeFiles/augmented_nn_test.dir/flags.make
test/CMakeFiles/augmented_nn_test.dir/brain/augmented_nn_test.cpp.o: test/brain/augmented_nn_test.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/zqs/star/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/CMakeFiles/augmented_nn_test.dir/brain/augmented_nn_test.cpp.o"
	cd /home/zqs/star/test && /usr/bin/g++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/augmented_nn_test.dir/brain/augmented_nn_test.cpp.o -c /home/zqs/star/test/brain/augmented_nn_test.cpp

test/CMakeFiles/augmented_nn_test.dir/brain/augmented_nn_test.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/augmented_nn_test.dir/brain/augmented_nn_test.cpp.i"
	cd /home/zqs/star/test && /usr/bin/g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/zqs/star/test/brain/augmented_nn_test.cpp > CMakeFiles/augmented_nn_test.dir/brain/augmented_nn_test.cpp.i

test/CMakeFiles/augmented_nn_test.dir/brain/augmented_nn_test.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/augmented_nn_test.dir/brain/augmented_nn_test.cpp.s"
	cd /home/zqs/star/test && /usr/bin/g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/zqs/star/test/brain/augmented_nn_test.cpp -o CMakeFiles/augmented_nn_test.dir/brain/augmented_nn_test.cpp.s

# Object files for target augmented_nn_test
augmented_nn_test_OBJECTS = \
"CMakeFiles/augmented_nn_test.dir/brain/augmented_nn_test.cpp.o"

# External object files for target augmented_nn_test
augmented_nn_test_EXTERNAL_OBJECTS =

test/augmented_nn_test: test/CMakeFiles/augmented_nn_test.dir/brain/augmented_nn_test.cpp.o
test/augmented_nn_test: test/CMakeFiles/augmented_nn_test.dir/build.make
test/augmented_nn_test: libcommon.a
test/augmented_nn_test: test/libpeloton-test-common.a
test/augmented_nn_test: /usr/local/lib/libglog.a
test/augmented_nn_test: /usr/local/lib/libgflags.so
test/augmented_nn_test: /usr/local/lib/libtensorflow.so.bak
test/augmented_nn_test: /usr/lib/libtensorflow_framework.so
test/augmented_nn_test: /usr/local/lib64/libgmock.a
test/augmented_nn_test: /usr/local/lib64/libgtest.a
test/augmented_nn_test: test/CMakeFiles/augmented_nn_test.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/zqs/star/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable augmented_nn_test"
	cd /home/zqs/star/test && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/augmented_nn_test.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
test/CMakeFiles/augmented_nn_test.dir/build: test/augmented_nn_test

.PHONY : test/CMakeFiles/augmented_nn_test.dir/build

test/CMakeFiles/augmented_nn_test.dir/clean:
	cd /home/zqs/star/test && $(CMAKE_COMMAND) -P CMakeFiles/augmented_nn_test.dir/cmake_clean.cmake
.PHONY : test/CMakeFiles/augmented_nn_test.dir/clean

test/CMakeFiles/augmented_nn_test.dir/depend:
	cd /home/zqs/star && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/zqs/star /home/zqs/star/test /home/zqs/star /home/zqs/star/test /home/zqs/star/test/CMakeFiles/augmented_nn_test.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/CMakeFiles/augmented_nn_test.dir/depend

