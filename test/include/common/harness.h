//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// harness.h
//
// Identification: test/include/common/harness.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <vector>
#include <thread>
#include <functional>
#include <iostream>
#include <atomic>

#include "common/macros.h"
// #include "common/init.h"
#include "common/internal_types.h"
// #include "gc/gc_manager_factory.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

// #include <google/protobuf/stubs/common.h>
#include <gflags/gflags.h>

namespace peloton {

namespace type{
class AbstractPool;
}

namespace test {

//===--------------------------------------------------------------------===//
// Test Harness (common routines)
//===--------------------------------------------------------------------===//

#define MAX_THREADS 1024

//===--------------------------------------------------------------------===//
// Peloton Test
//===--------------------------------------------------------------------===//

// All tests inherit from this class
class PelotonTest : public ::testing::Test {
 protected:

  virtual void SetUp() {

    // turn off gc under test mode
    // gc::GCManagerFactory::GetInstance().StopGC();
    // gc::GCManagerFactory::Configure(0);

  }

  virtual void TearDown() {

    // shutdown protocol buf library
    // google::protobuf::ShutdownProtobufLibrary();

    // Shut down GFLAGS.
    ::google::ShutDownCommandLineFlags();
  }
};

}  // namespace test
}  // namespace peloton
