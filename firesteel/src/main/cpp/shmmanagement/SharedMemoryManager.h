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

#ifndef SHARED_MEMORY_MANAGER_H_
#define SHARED_MEMORY_MANAGER_H_

#include <string>
#include <mutex>
#include <unordered_map>

using namespace std;

//the allocator to malloc/free memory acquired the shared-memory region
class SharedMemoryAllocator;


class SharedMemoryManager {
 private:
     static SharedMemoryManager* m_pInstance; 
     static mutex creationMutex; 

     //each global heap name comes with a memory allocator. 
     unordered_map <string, SharedMemoryAllocator*> allocatorTable;
      
 public: 

      SharedMemoryManager();

      //due to  class forwarding, put to the implementation file
      ~SharedMemoryManager();

     //there is no locking here. this method is desigend to be call from Java during each package
     //such as shuffle engine and off-heap memory store's inialization phase via a synchronized call.
     //we have to make sure that the first call to this method is via a synchronized Java
     static SharedMemoryManager* getInstance() ;    
    
     //create a region allocator per global heap, if it has not been created.
     //locking is OK, as that is mostly at the intialization time
     SharedMemoryAllocator* registerShmRegion(const string &globalName);

     //to unregister the region allocator, so that the region allocator can be properly closed.
     void unRegisterShmRegion(const string &globalName);

     //to unregister all of region allocators, so that the region allocators can be properly closed.
     void shutdown();
};


#endif /*SHARED_MEMORY_MANAGER_H_*/
