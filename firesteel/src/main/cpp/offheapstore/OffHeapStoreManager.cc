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
#include <immintrin.h>
#include "OffHeapStoreManager.h"
#include "OffHeapStoreConstants.h"
#include "ShmAddress.h"
#include "AttributeHashTable.h"
#include "ShuffleDataSharedMemoryManager.h"

OffHeapStoreManager* OffHeapStoreManager::m_pInstance = nullptr; 

OffHeapStoreManager::OffHeapStoreManager(): 
                         memoryManager(nullptr) {
}

OffHeapStoreManager::~OffHeapStoreManager() {

    if(initMemory) {
      if (memoryManager != nullptr ){ 
        delete memoryManager;
        memoryManager = nullptr;
      }
    }
}


void OffHeapStoreManager::initialize(const string& heapName, int exeId) {
       //initialize the memory manager   
       LOG(INFO) << "initialize off-heap store manager with executor id " << executorId;
       globalHeapName = heapName; 
       executorId = exeId;

       initMemoryManager();
}


void OffHeapStoreManager::initMemoryManager() {
       
       if (OffHeapStoreGlobalConstants::USING_RMB) {
           memoryManager = new RMBShuffleDataSharedMemoryManager ();
       }
       else {
           memoryManager = new LocalShuffleDataSharedMemoryManager ();
       }
       memoryManager->initialize(globalHeapName);
       //delegate the region id management to shared memory manager.
       generationId = memoryManager->getGenerationId();
       processMap = memoryManager->get_pmap();

       initMemory = true;

       //we can do it via environmental setting, except the log directory.
       //init_google_logger();
       LOG(INFO) << "off-heap store manager initialized with executor id " << executorId 
                 << " and generation id: " << generationId
                 << " with heap name: " << globalHeapName;
}

void OffHeapStoreManager::createAttributeTable(long addrTable, int size, ShmAddress& shmAddr) {

       int generationId = getGenerationId();

       LOG(INFO) << "memoryManager ptr=" << (long)memoryManager;

       RRegion::TPtr<void> global_null_ptr;
       RRegion::TPtr<void> dataChunkGlobalPointer = memoryManager->allocate_datachunk (size);

	unsigned char *dataChunkOffset = nullptr;
  	if (OffHeapStoreGlobalConstants::USING_RMB) {
	    if (dataChunkGlobalPointer != global_null_ptr) {
      		dataChunkOffset = (unsigned char*) dataChunkGlobalPointer.get();
      		CHECK (check_pmap((void*)dataChunkOffset))
        		<< "address: " << (void*) dataChunkOffset << " not in range: ("
        		<<  reinterpret_cast <void* > (processMap.first)
        		<< " ,"
        		<<  reinterpret_cast <void* > (processMap.second);
    	    } else {
      		dataChunkOffset = nullptr;
      		LOG(ERROR)<< "allocate index chunk returns global null pointer " << " for size: " << size
       				  << " generation id: " << generationId;
    	    }   
	} else {
		dataChunkOffset = reinterpret_cast<unsigned char*> (dataChunkGlobalPointer.offset());
	}

	memcpy(dataChunkOffset, (unsigned char*)addrTable, size);

	shmAddr.regionId = dataChunkGlobalPointer.region_id();
	shmAddr.offset_attrTable = dataChunkGlobalPointer.offset(); 
        
        LOG(INFO) << "createAttributeTable done";
}

long OffHeapStoreManager::getAttributeTable(ShmAddress& shmAddr){

	RRegion::TPtr<void> global_null_ptr;

   	uint64_t gregion_id = shmAddr.regionId;
   	uint64_t goffset = shmAddr.offset_attrTable;

   	unsigned char *dataChunkOffset = nullptr;
        if (OffHeapStoreGlobalConstants::USING_RMB){
              RRegion::TPtr<void> globalpointer(gregion_id, goffset);
     	      if (globalpointer != global_null_ptr) {
      		dataChunkOffset = (unsigned char*) globalpointer.get();
     	      }
     	      //else it is still nullptr;
  	}
   	else {
     	     //local memory allocator
     	     dataChunkOffset = reinterpret_cast<unsigned char *>(goffset);
   	}

   	CHECK (dataChunkOffset != nullptr); 

        _mm_prefetch((char*)&dataChunkOffset[0], _MM_HINT_T0);
        _mm_prefetch((char*)&dataChunkOffset[64], _MM_HINT_T0);
        // _mm_prefetch((char*)&dataChunkOffset[128], _MM_HINT_T0);
        // _mm_prefetch((char*)&dataChunkOffset[192], _MM_HINT_T0);
        // _mm_prefetch((char*)&dataChunkOffset[256], _MM_HINT_T0);
        // _mm_prefetch((char*)&dataChunkOffset[320], _MM_HINT_T0);

	return (long)dataChunkOffset;
}

void OffHeapStoreManager::createAttributeHashTable(long addrTable, int size, int valuesize, ShmAddress& shmAddr) {

    createAttributeTable(addrTable, size, shmAddr);

    AttributeHashTable attrHashTable; 
    int i = 0;
    while(true) {
	long keyoffset = (8L+valuesize)*i;
	if(keyoffset >= size)
		break;
	long key = *(long*)(addrTable+keyoffset);
	//long valueoffset = keyoffset+8L;
	attrHashTable.put(key, i); // use ordinal as offset
	i++; 
     }
     attrHashTable.create2tables(shmAddr.regionId);

     size_t bucket_cnt = attrHashTable.getBucketCnt();
     size_t arr2ndSize = attrHashTable.getArr2ndSize();
     long arr1stTable = attrHashTable.getArr1stTable();
     long arr2ndTable = attrHashTable.getArr2ndTable();

     int generationId = getGenerationId();

     RRegion::TPtr<void> global_null_ptr;
     RRegion::TPtr<void> dataChunkGlobalPointer = memoryManager->allocate_datachunk (bucket_cnt*sizeof(int));

     unsigned char *dataChunkOffset = nullptr;
     if (OffHeapStoreGlobalConstants::USING_RMB) {
		if (dataChunkGlobalPointer != global_null_ptr) {
      		  dataChunkOffset = (unsigned char*) dataChunkGlobalPointer.get();
      		  CHECK (OffHeapStoreManager::getInstance()->check_pmap((void*)dataChunkOffset))
        		<< "address: " << (void*) dataChunkOffset << " not in range: ("
        		<<  reinterpret_cast <void* > (processMap.first)
        		<< " ,"
        		<<  reinterpret_cast <void* > (processMap.second);
                }
                else {
      	   	   dataChunkOffset = nullptr;
      		   LOG(ERROR)<< "allocate index chunk returns global null pointer " << " for size: " << bucket_cnt*sizeof(int)
       	    		     << " generation id: " << generationId;
               }
     } 
     else {
		dataChunkOffset = reinterpret_cast<unsigned char*> (dataChunkGlobalPointer.offset());
     }
    
     memcpy(dataChunkOffset, (unsigned char*)arr1stTable, bucket_cnt*sizeof(int));
     free((unsigned char*)arr1stTable);
     shmAddr.offset_hash1stTable = dataChunkGlobalPointer.offset(); 
     shmAddr.bucket_cnt = bucket_cnt; 

     dataChunkGlobalPointer = memoryManager->allocate_datachunk (arr2ndSize*sizeof(long));

     if (OffHeapStoreGlobalConstants::USING_RMB) {
        if (dataChunkGlobalPointer != global_null_ptr) {
            dataChunkOffset = (unsigned char*) dataChunkGlobalPointer.get();
            CHECK (check_pmap((void*)dataChunkOffset))
                << "address: " << (void*) dataChunkOffset << " not in range: ("
                <<  reinterpret_cast <void* > (processMap.first)
                << " ,"
                <<  reinterpret_cast <void* > (processMap.second);
        } else {
            dataChunkOffset = nullptr;
            LOG(ERROR)<< "allocate index chunk returns global null pointer " << " for size: " << arr2ndSize*sizeof(long)
                      << " generation id: " << generationId;
        }
    }
    else {
        dataChunkOffset = reinterpret_cast<unsigned char*> (dataChunkGlobalPointer.offset());
    }

    memcpy(dataChunkOffset, (unsigned char*)arr2ndTable, arr2ndSize*sizeof(long));
    free((unsigned char*)arr2ndTable);
    shmAddr.offset_hash2ndTable = dataChunkGlobalPointer.offset(); 

}

long OffHeapStoreManager::getAttribute(ShmAddress& shmAddr, long key) {

	RRegion::TPtr<void> global_null_ptr;

   	unsigned char *offset1 = nullptr;
   	unsigned char *offset2 = nullptr;
        if (OffHeapStoreGlobalConstants::USING_RMB){
     	RRegion::TPtr<void> globalpointer1(shmAddr.regionId, shmAddr.offset_hash1stTable);
     	if (globalpointer1 != global_null_ptr) {
      		offset1 = (unsigned char*) globalpointer1.get();
     	}
     	RRegion::TPtr<void> globalpointer2(shmAddr.regionId, shmAddr.offset_hash2ndTable);
     	if (globalpointer2 != global_null_ptr) {
      		offset2 = (unsigned char*) globalpointer2.get();
     	}
        //else it is still nullptr;
   	}
   	else {
                //local memory allocator
		offset1 = reinterpret_cast<unsigned char *>(shmAddr.offset_hash1stTable);
		offset2 = reinterpret_cast<unsigned char *>(shmAddr.offset_hash2ndTable);
   	}

   	CHECK (offset1 != nullptr && offset2 != nullptr); //

        AttributeHashTable hashTable; 
	hashTable.setAttrTable(reinterpret_cast<int*>(offset1), reinterpret_cast<long*>(offset2));
	hashTable.setBucketCnt(shmAddr.bucket_cnt);

	return hashTable.get(key);
}

void OffHeapStoreManager::shutdown() {

       memoryManager->shutdown();
       LOG(INFO) << "off-heap store manager shutdown";
}

void OffHeapStoreManager::format_shm (){
     if (OffHeapStoreGlobalConstants::USING_RMB) {
        LOG(INFO) << "format global heap: " << globalHeapName << " with generation id: " << generationId;
        //generation id is per-process and it can be retrieved by querying RMB.
        memoryManager->format_shm (generationId);
     }
     else {
        LOG(WARNING) << "no global heap support when choosing local memory allocator";
     }
}
