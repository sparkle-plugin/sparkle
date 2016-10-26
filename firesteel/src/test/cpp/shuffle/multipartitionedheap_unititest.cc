/*
 * (c) Copyright 2016 Hewlett Packard Enterprise Development LP
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <glog/logging.h>


#include <iostream>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>

#include "SharedMemoryManager.h"
#include "SharedMemoryAllocator.h"

#include "gtest/gtest.h"

namespace fs=boost::filesystem;

TEST(SharedMemoryAllocator, MultiPartitionedRegions) {
  string sharedmemoryPathName = "/dev/shm/nvm/global0";
  fs::path sharedMemoryPath (sharedmemoryPathName);
  EXPECT_EQ(fs::exists(sharedMemoryPath), false);

  string commonPartitionName = sharedMemoryPath.filename().string();

  fs::path parentPath = sharedMemoryPath.parent_path();
  EXPECT_EQ(fs::exists(parentPath), true);
  
  vector<fs::path>  partitionRelatedPaths;

  for(auto it = fs::directory_iterator(parentPath); it != fs::directory_iterator(); ++it) {
    if (!it->path().has_extension()) {
     string filename = it->path().filename().string();
     std::string::size_type n=filename.find(commonPartitionName);
     if (n != std::string::npos) {
       partitionRelatedPaths.push_back(*it);
     }
    }
  }

  EXPECT_GT(partitionRelatedPaths.size(), 0);

  for (size_t i=0;i<partitionRelatedPaths.size(); i++) {
    LOG(INFO) << partitionRelatedPaths[i];
  }

  vector<string> orderedPartitionedPaths;
  //now create the partition files in assending order.
  for (size_t i=0;i<partitionRelatedPaths.size(); i++) {
     std::stringstream ss;
     ss << sharedmemoryPathName << "-" << i;
     orderedPartitionedPaths.push_back(ss.str());
  }

  LOG(INFO) << "final ordered partition files are the following";

  for (size_t i=0;i<orderedPartitionedPaths.size(); i++) {
    LOG(INFO) << orderedPartitionedPaths[i];
  }
}


// Step 3. Call RUN_ALL_TESTS() in main().
int main(int argc, char **argv) {
  //with main, we can attach some google test related hooks.
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler() ;

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

//
// We do this by linking in src/gtest_main.cc file, which consists of
// a main() function which calls RUN_ALL_TESTS() for us.
//
// This runs all the tests you've defined, prints the result, and
// returns 0 if successful, or 1 otherwise.
//
// Did you notice that we didn't register the tests?  The
// RUN_ALL_TESTS() macro magically knows about all the tests we
// defined.  Isn't this convenient?
