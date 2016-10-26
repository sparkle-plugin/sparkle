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

#ifndef SHUFFLE_DATA_SHARED_MEMORY_MANAGER_H
#define SHUFFLE_DATA_SHARED_MEMORY_MANAGER_H

#include <glog/logging.h>
#include <stdlib.h>
#include <string>

#include "SharedMemoryManager.h"
#include "SharedMemoryAllocator.h"

using namespace std;
using namespace alps;

//the interface to support the default memory allocator and the one from RMB
class ShuffleDataSharedMemoryManager {

 public:

     //to do initialization for the memory manager.
     virtual void initialize (const string &heapName) =0;
     //only close the heap, but not shut it down.
     virtual void close () =0;
     virtual void shutdown() = 0;

     //to allocate the index chunk for the shuffle data.
     virtual RRegion::TPtr<void> allocate_indexchunk(size_t size) =0;

     //to allocate the data chunk for the shuffle data.
     virtual RRegion::TPtr<void> allocate_datachunk(size_t size) =0;

     virtual void free_indexchunk(RRegion::TPtr<void>& p) =0;
     
     virtual void free_datachunk(RRegion::TPtr<void>& p) =0;

     virtual string get_heapname() const =0;

     //to retrieve the generation id associated with the current executor process.
     //for future clean-up purpose
     virtual uint64_t getGenerationId() const = 0; 

     //to retrieve the mapped virtual address range in this process for the shm region
     virtual pair<size_t, size_t> get_pmap() = 0;

     //for testing purpose
     virtual void format_shm (GlobalHeap::InstanceId generationId) =0;
    
     virtual ~ShuffleDataSharedMemoryManager () {
        //do nothing.
     }

};


class LocalShuffleDataSharedMemoryManager: public ShuffleDataSharedMemoryManager {
   private: 
       string heapName;
   
   public:
      LocalShuffleDataSharedMemoryManager() {
	//do nothing.
      }

      virtual ~LocalShuffleDataSharedMemoryManager() {
	//do nothing.
      }

      void initialize (const string &heapname) override {
        heapName = heapname;
      }

      void close () override {
	//do nothing
      }

      void shutdown() override {
        //do nothing
      }

      RRegion::TPtr<void> allocate_indexchunk (size_t size) override {
     	  void* nptr =  malloc(size);
          return RRegion::TPtr<void> (-1, reinterpret_cast<uint64_t> (nptr));
      }

      RRegion::TPtr<void> allocate_datachunk (size_t size) override {
  	 void* nptr= malloc(size);
         return RRegion::TPtr<void> (-1, reinterpret_cast<uint64_t> (nptr));
      }

      void free_indexchunk (RRegion::TPtr<void>& p)  override {
	void *nptr = reinterpret_cast <void*>(p.offset());
        free(nptr);
      }

      void free_datachunk (RRegion::TPtr<void>& p)  override {
	void *nptr = reinterpret_cast <void*>(p.offset());
        free(nptr);  
      }

      string get_heapname() const override {
        return heapName;
      }

      uint64_t getGenerationId() const override {
	 LOG(WARNING) << "local memory allocator only supports generion id = 0";
         return 0; 
      }

      void format_shm (GlobalHeap::InstanceId generationId) override {
	 LOG(WARNING) << "local memory allocator does not support formating of shm";
      }

      pair<size_t, size_t> get_pmap() override {
         LOG(WARNING) << "local memory allocator does not support memory map retrieval of shm";
         pair<size_t, size_t> result (-1, -1);
         return result;
      }

     private: 
      //nothing
};


class RMBShuffleDataSharedMemoryManager: public ShuffleDataSharedMemoryManager {
   private:
      SharedMemoryAllocator *allocator; 
      string heapName;
    
   public: 
      RMBShuffleDataSharedMemoryManager (): 
           allocator(nullptr) {
	 //invoke the singleton object creation.
	 SharedMemoryManager::getInstance();
      }

      virtual ~RMBShuffleDataSharedMemoryManager () {
	 shutdown();
      }
      
      // the passed-in parameter heap name should be the full-path on the in-memory file system 
      void initialize (const string &heapname) override {
	 heapName = heapname;
	 allocator = SharedMemoryManager::getInstance()->registerShmRegion(heapName);
         bool heapOpen = allocator->isHeapOpen();
         if (heapOpen) {
	   LOG(INFO) << "shuffle engine has opened heap: " << heapName; 
         }
         else {
	   LOG(ERROR) << "shuffle engine fails to open heap: " << heapName; 
	 }
      } 

      //just close the heap, but the memory allocator is still valid
      void close () override {
	 allocator->close();
      }

      void shutdown() override {
        SharedMemoryManager::getInstance()->unRegisterShmRegion(heapName);
        LOG(INFO) << "shuffle engine shutdown heap: "<< heapName;
      }


      RRegion::TPtr<void> allocate_indexchunk(size_t size) override {
	 return (allocator->allocate(size));
      }

      //what is returned is the full virtual address on the current process
      RRegion::TPtr<void> allocate_datachunk (size_t size) override {
	 return (allocator->allocate(size));
      }

      void free_indexchunk (RRegion::TPtr<void>& p) override {
	 allocator->free(p);
      }

      void free_datachunk (RRegion::TPtr<void>& p) override {
	 allocator->free(p);
      }

      string get_heapname() const override {
        return allocator->getHeapName();
      }

      uint64_t getGenerationId() const override {
	return (allocator->getGenerationId());
      } 

      //NOTE: an open heap can not be formated, as it contains the volatile data structures
      //cached from the persistent heap metadata store. 
      //but once the heap is closed, generation id can not be queried. So generation id has to
      //be passed in, or cached earlier before the heap is closed.
      void format_shm (GlobalHeap::InstanceId generationId) override {
	 allocator->format_shm(generationId);
      }

      //retrieve the map only when the heap is open 
      pair<size_t, size_t> get_pmap() override {
	return (allocator->get_pmap());
      }
};
#endif  /*SHUFFLE_DATA_SHARED_MEMORY_MANAGER_H*/
