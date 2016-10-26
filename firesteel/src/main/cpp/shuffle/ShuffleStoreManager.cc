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
#include "ShuffleStoreManager.h"
#include "MapShuffleStoreManager.h"
#include "ReduceShuffleStoreManager.h"
#include "ShuffleConstants.h"
#include "ByteBufferPool.h"
#include "ArrayBufferPool.h"
#include "ShuffleDataSharedMemoryManager.h"

ShuffleStoreManager* ShuffleStoreManager::m_pInstance = nullptr; 

//to initialize the google logger here.
//for more details, refer to: http://google-glog.googlecode.com/svn/trunk/doc/glog.html

//static void init_google_logger() {
  //log directory 
//  FLAGS_log_dir ="/tmp/shmlog";
  //verbose log level, N->...->3->2->1->0 which is different than INFO->WARNING->ERROR->FATAL
  //I choose 2 for my logging level. thus, level =1 and 0 will be logged as well.
//  FLAGS_v = 2; 
  //then invoke the actual google logger initializer.
//  google::InitGoogleLogging(NULL);
//}

ShuffleStoreManager::ShuffleStoreManager() {
       mapShuffleStoreManager = new MapShuffleStoreManager();
       reduceShuffleStoreManager = new ReduceShuffleStoreManager();
 
       //for bytebuffer pool
       if (SHMShuffleGlobalConstants::BYTEBUFFER_POOL_ON) {
	 byteBufferPool =
           new QueueBasedByteBufferPool(SHMShuffleGlobalConstants::MAXIMUM_BYTEBUFFER_POOL_SIZE);
       }
       else {
  	  byteBufferPool = new NullByteBufferPool();
       }

       //for keybuffer pool at the map side. and for each key type that we defined.
       if (SHMShuffleGlobalConstants::KEYBUFFER_POOL_ON) {
         for (size_t i=0; i<TOTAL_PREDEFINED_KVALUETYPE_IDS;i++) {
  	   ArrayBufferPool *keyBufferPool =
             new QueueBasedArrayBufferPool(SHMShuffleGlobalConstants::MAXIMUM_KEYBUFFER_POOL_SIZE);
           keyBufferPools.push_back(keyBufferPool);
	 }
       }
       else {
         for (size_t i=0; i<TOTAL_PREDEFINED_KVALUETYPE_IDS;i++) {
  	    ArrayBufferPool *keyBufferPool = new NullArrayBufferPool();
            keyBufferPools.push_back(keyBufferPool);
	 }
       }

       //for linked value buffer pool at the reduce side, when hash-merge is used.
       if (SHMShuffleGlobalConstants::LINKVALUEBUFFER_POOL_ON) {
          linkedValueBufferPool = 
            new QueueBasedArrayBufferPool(SHMShuffleGlobalConstants::MAXIMUM_LINKVALUEBUFFER_POOL_SIZE);
       }
       else {
	  linkedValueBufferPool = new NullArrayBufferPool();
       }

       //for NVM memory manage
       if (SHMShuffleGlobalConstants::USING_RMB) {
	 shuffleDataSharedMemoryManager = new RMBShuffleDataSharedMemoryManager ();
       }
       else {
          shuffleDataSharedMemoryManager = new LocalShuffleDataSharedMemoryManager ();
       }
 }

//Item 7: declare a virtual destructor if and only if the class contains at least one virtual function
ShuffleStoreManager::~ShuffleStoreManager() {
       delete mapShuffleStoreManager;
       mapShuffleStoreManager = nullptr;
      
       delete reduceShuffleStoreManager; 
       reduceShuffleStoreManager = nullptr;
       
       delete byteBufferPool; 
       byteBufferPool = nullptr; 

       for (size_t i=0; i<TOTAL_PREDEFINED_KVALUETYPE_IDS;i++) {
	 ArrayBufferPool *pool= keyBufferPools[i];
         if (pool != nullptr) {
  	    pool->shutdown();
            delete pool;
	 }
       }
       keyBufferPools.clear();

       delete linkedValueBufferPool;
       linkedValueBufferPool = nullptr;

       delete shuffleDataSharedMemoryManager;
       shuffleDataSharedMemoryManager = nullptr;
       
}


void ShuffleStoreManager::initialize(const string& heapName, int exeId) {
       mapShuffleStoreManager->initialize() ;
       reduceShuffleStoreManager->initialize() ;
       byteBufferPool->initialize();

       for (size_t i=0; i<TOTAL_PREDEFINED_KVALUETYPE_IDS;i++) {
	 ArrayBufferPool *pool= keyBufferPools[i];
         if (pool != nullptr) {
	   pool->initialize();
	 }
       }

       //initialize the memory manager   
       globalHeapName = heapName; 
       executorId = exeId;
       
       shuffleDataSharedMemoryManager->initialize(globalHeapName);
       //delegate the generation id management to shuffle data shared memory manager.
       generationId = shuffleDataSharedMemoryManager->getGenerationId();
       processMap = shuffleDataSharedMemoryManager->get_pmap();

       //we can do it via environmental setting, except the log directory.
       //init_google_logger();
       LOG(INFO) << "shuffle store manager initialized with executor id " << executorId 
                 << " and generation id: " << generationId;
}

void ShuffleStoreManager::shutdown() {
       mapShuffleStoreManager->shutdown() ;
       reduceShuffleStoreManager->shutdown() ;
       byteBufferPool->shutdown();

       for (size_t i=0; i<TOTAL_PREDEFINED_KVALUETYPE_IDS;i++) {
	 ArrayBufferPool *pool= keyBufferPools[i];
         if (pool != nullptr) {
  	    pool->shutdown();
	 }
       }
       keyBufferPools.clear();

       shuffleDataSharedMemoryManager->shutdown();
       LOG(INFO) << "shuffle store manager shutdown";
}

//NOTE: the clean up will be initiated from Java, and go through store by store.
//we will ignore the second parameter, as this parameter is for global cleanup, not 
//for this particular executor-based shuffle store manager clean up, as some of the
//map ids may not show up in this executor
//void ShuffleStoreManager::cleanup (int shuffleId, int numOfMaps){
//     LOG(INFO) << "shuffle store manager clean up for shuffle id: " << shuffleId << endl;
//     mapShuffleStoreManager->cleanup(shuffleId);
//     //NOTE: no reducer store needs to clean up, as it does not contain NVM resource.
//}

void ShuffleStoreManager::format_shm (){
     if (SHMShuffleGlobalConstants::USING_RMB) {
        LOG(INFO) << "format global heap: " << globalHeapName << " with generation id: " << generationId;
        //generation id is per-process and it can be retrieved by querying RMB.
        shuffleDataSharedMemoryManager->format_shm (generationId);
     }
     else {
        LOG(WARNING) << "no global heap support when choosing local memory allocator";
     }
}

//so that a new instance of the heap is created.
void ShuffleStoreManager::register_new_shm() {
      shuffleDataSharedMemoryManager->shutdown();

      //for NVM memory manage
      if (SHMShuffleGlobalConstants::USING_RMB) {
	 shuffleDataSharedMemoryManager = new RMBShuffleDataSharedMemoryManager ();
      }
      else {
          shuffleDataSharedMemoryManager = new LocalShuffleDataSharedMemoryManager ();
      }

       shuffleDataSharedMemoryManager->initialize(globalHeapName);
       //delegate the generation id management to shuffle data shared memory manager.
       generationId = shuffleDataSharedMemoryManager->getGenerationId();

}
