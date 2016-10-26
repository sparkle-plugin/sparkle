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

#include "MapShuffleStoreManager.h"
#include "MapShuffleStoreWithIntKeys.h"
#include "gtest/gtest.h"

TEST(MapShuffleStoreManagerTest, CreateRemoveIntKeyStore) {
  MapShuffleStoreManager *manager = new MapShuffleStoreManager();

  manager->initialize();

  int shuffleId = 0;
  int mapId = 20;
  enum KValueTypeId tid = KValueTypeId::Int;
  bool ordering=true;

  MapShuffleStoreWithIntKey *store =
             (MapShuffleStoreWithIntKey*) manager->createStore(shuffleId, mapId, tid, ordering);
 
  manager->stopShuffleStore((GenericMapShuffleStore *)store);
  manager->shutdownShuffleStore((GenericMapShuffleStore *)store);

  manager->shutdown();
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
