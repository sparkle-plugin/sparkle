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

#include <iostream>
#include <glog/logging.h>

#include "ShuffleStoreManager.h"
#include "MapShuffleStoreManager.h"
#include "MapShuffleStoreWithIntKeys.h"
#include "ShuffleDataSharedMemoryManager.h"
#include "SharedMemoryManager.h"
#include "SharedMemoryAllocator.h"

#include "gtest/gtest.h"

/*
TEST(ShuffleStoreManagerTest, CreateShuffleStoreManager) {
  ShuffleStoreManager *manager =  ShuffleStoreManager::getInstance();
  int executorId = 0;
  string heapName = "/dev/shm/nvm/global0";

  manager->initialize(heapName, executorId);
  
  MapShuffleStoreManager *mapShuffleManager = manager->getMapShuffleStoreManager();

  int shuffleId = 0;
  int mapId = 20;
  enum KValueTypeId tid = KValueTypeId::Int;
  bool ordering=true;

  MapShuffleStoreWithIntKey *store =
             (MapShuffleStoreWithIntKey*) mapShuffleManager->createStore(shuffleId, mapId, tid, ordering);
 
  mapShuffleManager->stopShuffleStore((GenericMapShuffleStore *)store);
  mapShuffleManager->shutdownShuffleStore((GenericMapShuffleStore *)store);

  mapShuffleManager->shutdown();

  manager->shutdown();
}

TEST(ShuffleStoreManagerTest, SharedMemoryManagerRetrivealTest) {
  ShuffleStoreManager *manager =  ShuffleStoreManager::getInstance();
  int executorId = 0;
  string heapName = "/dev/shm/nvm/global0";

  manager->initialize(heapName, executorId);
  ShuffleDataSharedMemoryManager* sharedMemoryManager = manager->getShuffleDataSharedMemoryManager();
  string retrieved_heapname = sharedMemoryManager->get_heapname();
  EXPECT_EQ(retrieved_heapname, heapName);

  pair<size_t, size_t> virtual_memory_map = sharedMemoryManager->get_pmap();
  EXPECT_NE (virtual_memory_map.first, -1);
  EXPECT_NE (virtual_memory_map.second, -1);

  uint64_t generationId = sharedMemoryManager->getGenerationId();
  EXPECT_NE (generationId, -1);

  sharedMemoryManager->shutdown();  

  manager->shutdown();
}
*/

TEST(ShuffleStoreManagerTest, SharedMemoryAlloateAndFree) {
  ShuffleStoreManager *manager =  ShuffleStoreManager::getInstance();
  int executorId = 0;
  string heapName = "/dev/shm/nvm/global0";

  manager->initialize(heapName, executorId);
  ShuffleDataSharedMemoryManager* sharedMemoryManager = manager->getShuffleDataSharedMemoryManager();
  string retrieved_heapname = sharedMemoryManager->get_heapname();
  EXPECT_EQ(retrieved_heapname, heapName);

  size_t allocation_size = 1024*1024*100;
  RRegion::TPtr<void> global_data_ptr = sharedMemoryManager-> allocate_datachunk (allocation_size);
  RRegion::TPtr<void> global_null_ptr;
  EXPECT_NE (global_data_ptr, global_null_ptr);
  
  unsigned char* local_data_ptr = (unsigned char*) global_data_ptr.get();
  EXPECT_EQ (manager->check_pmap((void*)local_data_ptr), true);

  sharedMemoryManager-> free_datachunk (global_data_ptr);


  sharedMemoryManager->shutdown();  
  manager->shutdown();
}

/*
TEST(ShuffleStoreManagerTest, SharedMemoryHeapFormat) {
  ShuffleStoreManager *manager =  ShuffleStoreManager::getInstance();
  int executorId = 0;
  string heapName = "/dev/shm/nvm/global0";

  manager->initialize(heapName, executorId);
  ShuffleDataSharedMemoryManager* sharedMemoryManager = manager->getShuffleDataSharedMemoryManager();
  string retrieved_heapname = sharedMemoryManager->get_heapname();
  EXPECT_EQ(retrieved_heapname, heapName);

  uint64_t generationId = sharedMemoryManager->getGenerationId();
  EXPECT_NE (generationId, -1);

  size_t allocation_size = 1024*1024*100;
  RRegion::TPtr<void> global_data_ptr = sharedMemoryManager-> allocate_datachunk (allocation_size);
  RRegion::TPtr<void> global_null_ptr;
  EXPECT_NE (global_data_ptr, global_null_ptr);

  //then issue the format, without free the pointer.

  //before the format, the heap will have to be closed already.
  sharedMemoryManager->close();  

  LOG(INFO) << "before issuing format_shm. shared-memory manager is closed." << endl;

  //NOTE: temporarily disabled.
  //sharedMemoryManager->format_shm(generationId);

  sharedMemoryManager->shutdown();

  manager->shutdown();
}



TEST(ShuffleStoreManagerTest, DirectSharedMemoryHeapFormat) {
  SharedMemoryManager *sharedMemoryManager = SharedMemoryManager::getInstance();
  string heapName = "/dev/shm/nvm/global0";

  SharedMemoryAllocator* allocator= sharedMemoryManager->registerShmRegion(heapName);

  string retrieved_heapname = allocator->getHeapName();
  EXPECT_EQ(retrieved_heapname, heapName);

  uint64_t generationId = allocator->getGenerationId();
  cout << "******in test code, returned RMB generation id: " << generationId <<endl;

  EXPECT_NE (generationId, -1);

  size_t allocation_size = 1024*1024*100;
  RRegion::TPtr<void> global_data_ptr = allocator->allocate (allocation_size);
  RRegion::TPtr<void> global_null_ptr;
  EXPECT_NE (global_data_ptr, global_null_ptr);

  //then issue the format, without free the pointer.

  //before the format, the heap will have to be closed already.
  allocator->close();  

  //NOTE: the format today only handles the lock release. Not actually erase the data.
  //so if you issue "report" from the RMB region, we will not see the erase of the data.

  //NOTE: temporarily disabled.
  //allocator->format_shm(generationId);
  sharedMemoryManager->shutdown();
}

*/
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
