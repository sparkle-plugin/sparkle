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

#ifndef SHARED_MEMORY_ALLOCATOR_H_
#define SHARED_MEMORY_ALLOCATOR_H_

#include <glog/logging.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <boost/filesystem.hpp>
#include "common/os.hh"
#include "globalheap/globalheap.hh"
#include "pegasus/pegasus_options.hh"
#include "pegasus/pegasus.hh"

using namespace std;
using namespace alps;

class SharedMemoryAllocator {
   private:
      GlobalHeap* heap = nullptr;
      bool heapOpen = false;
      string  logLevel; 
      const string DEFAULT_LOG_LEVEL = "info"; 

      string heapName;
      ProcessMap pmap;
      pair<size_t, size_t> pmap_range;

   //to record whether it is a single path or multiple paths, this influce both create and format
   private:
      static const size_t MAXIMUM_NUMBER_OF_PATHS = 256;
      char const* partitioned_paths[MAXIMUM_NUMBER_OF_PATHS];
      bool  single_partition = true;
      size_t partitioned_path_size = 0;

   public: 
      SharedMemoryAllocator (const string &heapname): 
           heapName(heapname),
           pmap_range (-1, -1)  {
        //retrieved the environment variable specified for rmb log level. 
        //the default is info level
	char *logspec = getenv ("rmb_log");
        if (logspec != nullptr) {
	  logLevel = logspec;
          LOG(INFO) << "rmb log level chosen is: " << logLevel; 
	}
        else {
	  logLevel = DEFAULT_LOG_LEVEL;
          LOG(INFO) << "rmb default log level chosen is: " << logLevel; 
	}
        
        PegasusOptions pgopt;
        pgopt.debug_options.log_level = logLevel;
	Pegasus::init(pgopt);
     }

      ~SharedMemoryAllocator  () {
    	//what should we do the clean up.
        LOG(INFO) << "rmb shared memory manager destructor gets called" <<endl;

        if (heap != nullptr) {
            heap->close();
  	    heapOpen = false;
            heap = nullptr;
	}

      }

      
     // the passed-in parameter heap name should be the full-path on the in-memory file system 
     void initialize () {
         VLOG(2) << "rmb shared memory manager initializes heap: " << heapName;
         initialize_global_heap();

         //WARNING: should I do the init? 
         if (heap != nullptr) {
  	    heapOpen = true;
	    LOG(INFO) << "rmb shared memory manager initialized global heap:"<< (void*) heap << endl;

	    pmap_range = pmap.range(heapName);
	 }
         else {
            //Logging a FATAL message terminates the program (after the message is logged)
            LOG(FATAL) << "rmb shared memory manager failed to initialize global heap: "
                       << heapName << endl;
	 }

      } 

      
      bool isHeapOpen() {
	  return heapOpen;
      }

      void close() {
	//what should we do the clean up.
        if (heap != nullptr) {
            heap->close();
  	    heapOpen = false;

	    LOG(INFO) << "rmb shared memory manager closed global heap: "<< (void*) heap 
                      << " with heap name: " << heapName <<endl;
            heap = nullptr;
	}
       
      }

      const string& getHeapName() const {
	return heapName;
      }

      RRegion::TPtr<void> allocate(size_t size) {
        if (heapOpen) {
	   return heap->malloc(size);
	}
        else {
          LOG(ERROR) << "try to allocate from a closed heap: " << heapName;
          RRegion::TPtr<void> null_ptr(-1, -1);
	  return null_ptr;
	}
      }

      void free(RRegion::TPtr<void>& p) {
	if (heapOpen) {
           heap->free(p);
	}
        else {
	   LOG(ERROR) << "try to free global pointer from a closed heap";
	}
      }

      uint64_t getGenerationId() const {
	if (heapOpen) {
	  GlobalHeap::InstanceId instanceId = heap->instance();
          return instanceId;
	}
        else {
           LOG(ERROR) << "try to get generation id from a closed heap";
           return -1;
	}
      } 

      //NOTE: an open heap can not be formated, as it contains the volatile data structures
      //cached from the persistent heap metadata store. 
      //but once the heap is closed, generation id can not be queried. So generation id has to
      //be passed in, or cached earlier before the heap is closed.
      void format_shm (GlobalHeap::InstanceId generationId) {
        if (!heapOpen) {
          if(single_partition) {
	     GlobalHeap::format_instance(heapName.c_str(), generationId);
	  }
          else {
	    GlobalHeap::format_instance(partitioned_paths, partitioned_path_size, generationId);

	  }
	}
        else {
          LOG(ERROR) << "try to format an open heap: " << heapName ; 
	}
      }

      //retrieve the map only when the heap is open 
      // this check assumes the heap file is mapped as a single contiguous memory region
      pair<size_t, size_t> get_pmap() {
	if (heapOpen) {
          return pmap_range;           
	}
        else {
	  LOG(ERROR) << "try to retrieve memory map from a closed heap"; 
	  pair<size_t, size_t> range (-1, -1);
          return range;
	}
      }


   private:

      void initialize_global_heap() {
         fs::path sharedMemoryPath (heapName);
   
         //if this is a single partiton.
         if (fs::exists(sharedMemoryPath)) {
	     single_partition=true;
             partitioned_path_size = 0;

             LOG(INFO) << "rmb shared memory manager detected a single-partitioned shm path: " << heapName;
             GlobalHeap::open(heapName.c_str(), &heap);     
         } 
         else {
           single_partition=false;

           string commonPartitionName = sharedMemoryPath.filename().string();
           fs::path parentPath = sharedMemoryPath.parent_path();

           vector<fs::path>  partitionRelatedPaths;

           for(auto it = fs::directory_iterator(parentPath); it != fs::directory_iterator(); ++it) {
              //ruling out the metadata tracking files such as global0-4.xattr
              if (!it->path().has_extension()) {
                 string filename = it->path().filename().string();
                 std::string::size_type n=filename.find(commonPartitionName);
                 if (n != std::string::npos) {
                      partitionRelatedPaths.push_back(*it);
                 }
             }
           }

          //we need more than one partitions
          CHECK(partitionRelatedPaths.size() > 1) ;

          //we will need the ordered partition files, as each process's memory mapping starts with global0-0
          //then to global0-1.
          vector<string> orderedPartitionedPaths;
          //now create the partition files in assending order.
          for (size_t i=0;i<partitionRelatedPaths.size(); i++) {
             std::stringstream ss;
             ss << heapName << "-" << i;
             orderedPartitionedPaths.push_back(ss.str());
          }

          partitioned_path_size = orderedPartitionedPaths.size();
          CHECK (partitioned_path_size  < MAXIMUM_NUMBER_OF_PATHS);

          for (size_t i=0;i<partitioned_path_size; i++) {
             LOG(INFO) << "check existence of ordered partition file: " << orderedPartitionedPaths[i];

             fs::path tshmPath (orderedPartitionedPaths[i]);
             //make sure that each file exists.
             CHECK(fs::exists(tshmPath));

             partitioned_paths[i]=orderedPartitionedPaths[i].c_str();
          }

          GlobalHeap::open(partitioned_paths, partitioned_path_size,  &heap);     
     }

   }

   
};

#endif  /*SHARED_MEMORY_ALLOCATOR_H_*/
