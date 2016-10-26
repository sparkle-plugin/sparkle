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

#ifndef OFFHEAPSTOREMANAGER_H_
#define OFFHEAPSTOREMANAGER_H_

#include <string>
#include <vector>
#include "ShmAddress.h" 

using namespace std;

//the shared memory manager
class ShuffleDataSharedMemoryManager;

class OffHeapStoreManager {
 private:
     static OffHeapStoreManager* m_pInstance; 

 private: 
     //NVM memory manager 
     //OffHeapStoreSharedMemoryManager *memoryManager; 
     ShuffleDataSharedMemoryManager *memoryManager; 
     //The NVM memory manager related information 
     string globalHeapName;  //global heap name
     int executorId;  //not the region id anymore. they are seperated.
     //generation id in RMB is designed for future allocated shared memory blocks clean up
     uint64_t generationId;

     pair<size_t, size_t> processMap;
     bool initMemory = false;
     
 public: 

      //due to  class forwarding, put to the implementation file
      OffHeapStoreManager();

      //due to  class forwarding, put to the implementation file
      ~OffHeapStoreManager();

     static OffHeapStoreManager* getInstance() {
       if(m_pInstance==nullptr) {
  	 m_pInstance = new OffHeapStoreManager();
       }
       return m_pInstance;
     };

     void initMemoryManager();

     ShuffleDataSharedMemoryManager *getOffHeapStoreSharedMemoryManager() {
        return memoryManager; 
     }

     //pass-in global heap name and executor id
     void initialize(const string& heapName, int exeId);

     void createAttributeTable(long addrTable, int size, ShmAddress& shmAddr);
     long getAttributeTable(ShmAddress& shmAddr);
     void createAttributeHashTable(long addrTable, int size, int offset, ShmAddress& shmAddr);
     long getAttribute(ShmAddress& shmAddr, long key);

     //this is passed in at the iniitialiation time
     const string&  getGlobalHeapName () const {
       return globalHeapName; 
     }

     //this is passed in at the iniitialiation time
     int getExecutorId () const {
       return  executorId;
     }

     pair<size_t, size_t> getProcessMap() const {
       return  processMap;
     }

     uint64_t getGenerationId() const {
       return  generationId;
     }

     //format the shared-memory, with the path to the in-memory file system, and
     //instance id captured when the heap is opened.
     void format_shm ();

     void shutdown();

     bool check_pmap (void *p) const {
       size_t addr = reinterpret_cast <size_t> (p);
       bool result = (addr >= processMap.first) && (addr < processMap.second);
       return result;
     }

};


#endif 
