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
#include "SharedMemoryAllocator.h"
#include "SharedMemoryManager.h"


SharedMemoryManager* SharedMemoryManager::m_pInstance = nullptr; 
mutex SharedMemoryManager::creationMutex;

//there is no locking here. this method is desigend to be call from Java during each package
//such as shuffle engine and off-heap memory store's inialization phase via a synchronized call.
//we have to make sure that the first call to this method is via a synchronized Java
SharedMemoryManager* SharedMemoryManager::getInstance() {
     if(m_pInstance == nullptr) {
         lock_guard<mutex> g{creationMutex};         
         //double-checked locking
         if (m_pInstance == nullptr) {
   	     m_pInstance = new SharedMemoryManager();
	 }
     }

     return m_pInstance;
};

SharedMemoryManager::SharedMemoryManager() {
   //do nothing.
}

//to clean up the registered memory allocators, if they are some there.
SharedMemoryManager::~SharedMemoryManager() {
   shutdown();
}

SharedMemoryAllocator* SharedMemoryManager::registerShmRegion(const string &globalName) {
     lock_guard<mutex> g{creationMutex};         

     unordered_map<string, SharedMemoryAllocator*>::const_iterator
                                             iterator =allocatorTable.find (globalName);
     SharedMemoryAllocator *allocator = nullptr; 

     if (iterator ==  allocatorTable.end()){
          //we can not find the allocator, create and register the allocator corresponding to
          //the heap name.
          allocator = new SharedMemoryAllocator (globalName);
          allocator->initialize(); //need to initialize it.
	  allocatorTable.insert (make_pair(globalName, allocator));
          
          LOG(INFO) << "shared-memory manager registered heap: " <<globalName;
     }
     else {
          allocator = iterator->second;
          LOG(INFO) << "shared-memory manager already registered heap: " <<globalName;
     }
     
     return allocator;
}

void SharedMemoryManager::unRegisterShmRegion(const string &globalName) {
     lock_guard<mutex> g{creationMutex}; 
     unordered_map<string, SharedMemoryAllocator*>::const_iterator
                                             iterator =allocatorTable.find (globalName);
     if (iterator !=  allocatorTable.end()){
         SharedMemoryAllocator *allocator = iterator->second;
         allocator->close();
         delete allocator; 

         allocatorTable.erase (globalName);

         LOG(INFO) << "shared-memory manager unregistered shm-region: " <<globalName;
     }
     else{
         LOG(ERROR) << "shared-memory manager already unregistered shm-region: " <<globalName;
     }
}

//to clean up the registered memory allocators, if they are some there.
void SharedMemoryManager::shutdown() {
  //close all of the registered allocators. no involvement of the guard.
  LOG(INFO) << "SharedMemoryManager allocator size: " << allocatorTable.size();

  for (auto it= allocatorTable.begin(); it != allocatorTable.end(); ++it) {
       SharedMemoryAllocator* allocator = it->second;
       string heapName = allocator->getHeapName();

       allocator->close();
       LOG(INFO) << "SharedMemoryManager closed allocator on heap: " << heapName;

       delete allocator; 

       LOG(INFO) << "SharedMemoryManager closed and deleted allocator on heap: " << heapName;
  }
 
  allocatorTable.clear();

  LOG(INFO) << "SharedMemoryManager clears all allocators";
}
