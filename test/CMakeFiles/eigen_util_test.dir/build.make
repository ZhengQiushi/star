# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.16

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
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/star

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/star

# Include any dependencies generated for this target.
include test/CMakeFiles/eigen_util_test.dir/depend.make

# Include the progress variables for this target.
include test/CMakeFiles/eigen_util_test.dir/progress.make

# Include the compile flags for this target's objects.
include test/CMakeFiles/eigen_util_test.dir/flags.make

test/CMakeFiles/eigen_util_test.dir/brain/eigen_util_test.cpp.o: test/CMakeFiles/eigen_util_test.dir/flags.make
test/CMakeFiles/eigen_util_test.dir/brain/eigen_util_test.cpp.o: test/brain/eigen_util_test.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/star/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/CMakeFiles/eigen_util_test.dir/brain/eigen_util_test.cpp.o"
	cd /home/star/test && /usr/bin/g++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/eigen_util_test.dir/brain/eigen_util_test.cpp.o -c /home/star/test/brain/eigen_util_test.cpp

test/CMakeFiles/eigen_util_test.dir/brain/eigen_util_test.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/eigen_util_test.dir/brain/eigen_util_test.cpp.i"
	cd /home/star/test && /usr/bin/g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/star/test/brain/eigen_util_test.cpp > CMakeFiles/eigen_util_test.dir/brain/eigen_util_test.cpp.i

test/CMakeFiles/eigen_util_test.dir/brain/eigen_util_test.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/eigen_util_test.dir/brain/eigen_util_test.cpp.s"
	cd /home/star/test && /usr/bin/g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/star/test/brain/eigen_util_test.cpp -o CMakeFiles/eigen_util_test.dir/brain/eigen_util_test.cpp.s

# Object files for target eigen_util_test
eigen_util_test_OBJECTS = \
"CMakeFiles/eigen_util_test.dir/brain/eigen_util_test.cpp.o"

# External object files for target eigen_util_test
eigen_util_test_EXTERNAL_OBJECTS =

test/eigen_util_test: test/CMakeFiles/eigen_util_test.dir/brain/eigen_util_test.cpp.o
test/eigen_util_test: test/CMakeFiles/eigen_util_test.dir/build.make
test/eigen_util_test: libcommon.a
test/eigen_util_test: test/libpeloton-test-common.a
test/eigen_util_test: /usr/local/lib/libglog.a
test/eigen_util_test: /usr/local/lib/libgflags.so
test/eigen_util_test: /usr/local/lib/libtensorflow.so
test/eigen_util_test: /usr/local/lib/libtensorflow_framework.so
test/eigen_util_test: /usr/local/lib/libgmock.a
test/eigen_util_test: /usr/local/lib/libgtest.a
test/eigen_util_test: test/CMakeFiles/eigen_util_test.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/star/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable eigen_util_test"
	cd /home/star/test && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/eigen_util_test.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
test/CMakeFiles/eigen_util_test.dir/build: test/eigen_util_test

.PHONY : test/CMakeFiles/eigen_util_test.dir/build

test/CMakeFiles/eigen_util_test.dir/clean:
	cd /home/star/test && $(CMAKE_COMMAND) -P CMakeFiles/eigen_util_test.dir/cmake_clean.cmake
.PHONY : test/CMakeFiles/eigen_util_test.dir/clean

test/CMakeFiles/eigen_util_test.dir/depend:
	cd /home/star && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/star /home/star/test /home/star /home/star/test /home/star/test/CMakeFiles/eigen_util_test.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/CMakeFiles/eigen_util_test.dir/depend

