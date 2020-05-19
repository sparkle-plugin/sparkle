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
#include <algorithm>
#include <stdlib.h>

#include <glog/logging.h>

#include "ShuffleStoreManager.h"
#include "MapShuffleStoreWithLongKeys.h"
#include "ShuffleConstants.h"
#include "MapShuffleStoreManager.h"
#include "ShuffleDataSharedMemoryManager.h"
#include "ShuffleDataSharedMemoryManagerHelper.h"
#include "ShuffleDataSharedMemoryWriter.h"
#include "ArrayBufferPool.h"
#include "MapStatus.h"
#include "SimpleUtils.h"

//for long key comparision
struct MapShuffleComparatorWithLongKey {
  inline bool operator()(const LongKeyWithValueTracker &a, const LongKeyWithValueTracker &b) {
    if (a.partition == b.partition){
      //when equival, if the key as comparation.
      return (a.key < b.key);
    } else {
      return (a.partition < b.partition);
    }
  }
} LongKeyWithValueTrackerComparator;

//please refer to C++ 11 reference book page. 194
//design for testing purpose
MapShuffleStoreWithLongKey::MapShuffleStoreWithLongKey (int bufsize, int mId, bool ordering): 
      bufferMgr(bufsize), mapId (mId),
      kvTypeDefinition(KValueTypeId::Long),
      orderingRequired (ordering)  {
      //will be set at the sort call.
      totalNumberOfPartitions =0;

      //struct timespec before, after;
      //clock_gettime(CLOCK_MONOTONIC,&before);
      ArrayBufferElement bElement =
          ShuffleStoreManager::getInstance()->getKeyBufferPool(KValueTypeId::Long)->getBuffer();
      if (bElement.capacity == 0) {
        //ask OS to give me the key buffer.
        keys =
          (LongKeyWithValueTracker*) malloc(sizeof(LongKeyWithValueTracker)*
          SHMShuffleGlobalConstants::MAPSHUFFLESTORE_KEY_BUFFER_SIZE);
        currentKeyBufferCapacity=SHMShuffleGlobalConstants::MAPSHUFFLESTORE_KEY_BUFFER_SIZE;
      } else {
        keys = (LongKeyWithValueTracker*)bElement.start_address;
        currentKeyBufferCapacity=bElement.capacity;
      }

      //clock_gettime(CLOCK_MONOTONIC,&after);

      //timespec htime= TimeUtil::diff (before, after);
      //LOG(INFO) << "malloc on key buffer size: " << SHMShuffleGlobalConstants::MAPSHUFFLESTORE_KEY_BUFFER_SIZE
      //          <<  " takes " << htime.tv_sec  << "(sec)" << " and " << htime.tv_nsec/1000 << "(us)" <<endl ;

      sizeTracker =0;
}

//please refer to C++ 11 reference book page. 194
MapShuffleStoreWithLongKey::MapShuffleStoreWithLongKey(int mId, bool ordering) : 
          bufferMgr(SHMShuffleGlobalConstants::BYTEBUFFER_HOLDER_SIZE),
          mapId(mId),
          kvTypeDefinition(KValueTypeId::Long),
          orderingRequired (ordering) {
      //will be set at the sort call.
      totalNumberOfPartitions =0;

      //struct timespec before, after;
      //clock_gettime(CLOCK_MONOTONIC,&before);

      ArrayBufferElement bElement =
          ShuffleStoreManager::getInstance()->getKeyBufferPool(KValueTypeId::Long)->getBuffer();
      if (bElement.capacity == 0) {
        //ask OS to give me the key buffer.
        keys =
          (LongKeyWithValueTracker*) malloc(sizeof(LongKeyWithValueTracker)*
          SHMShuffleGlobalConstants::MAPSHUFFLESTORE_KEY_BUFFER_SIZE);
        currentKeyBufferCapacity=SHMShuffleGlobalConstants::MAPSHUFFLESTORE_KEY_BUFFER_SIZE;
      }
      else {
        keys = (LongKeyWithValueTracker*)bElement.start_address;
        currentKeyBufferCapacity=bElement.capacity;
      }

      //clock_gettime(CLOCK_MONOTONIC,&after);

      //timespec htime= TimeUtil::diff (before, after);
      //LOG(INFO) << "malloc on default key buffer size: " << SHMShuffleGlobalConstants::MAPSHUFFLESTORE_KEY_BUFFER_SIZE
      //          <<  " takes " << htime.tv_sec  << "(sec)" << " and " << htime.tv_nsec/1000 << "(us)" <<endl ; 

      sizeTracker =0;
}

//byte[] inputs
void MapShuffleStoreWithLongKey::storeKVPairsWithLongKeys (
           unsigned char *byteHolder, int voffsets[],
           long kvalues[], int partitions[], int numberOfPairs){
  for (int i=0; i<numberOfPairs; i++) {

    int vStart=0;
    if (i>0) {
      vStart = voffsets[i-1];
    }
    int vEnd=voffsets[i];
    int vLength = vEnd-vStart;

    unsigned char *segment = byteHolder + vStart;

    PositionInExtensibleByteBuffer value_tracker =bufferMgr.append(segment,vLength);

    if (sizeTracker == currentKeyBufferCapacity) {
       LOG(INFO) << "keys " << (void*) keys << " with size tracker: " << sizeTracker 
                 << " reaches buffer size: " << SHMShuffleGlobalConstants::MAPSHUFFLESTORE_KEY_BUFFER_SIZE <<endl;
       //re-allocate then.
       currentKeyBufferCapacity += SHMShuffleGlobalConstants::MAPSHUFFLESTORE_KEY_BUFFER_SIZE;

       //struct timespec before, after;
       //clock_gettime(CLOCK_MONOTONIC,&before);

       keys = (LongKeyWithValueTracker*)realloc(keys, currentKeyBufferCapacity*sizeof(LongKeyWithValueTracker));

       //clock_gettime(CLOCK_MONOTONIC,&after);

       //timespec htime= TimeUtil::diff (before, after);
       //LOG(INFO) << "realloc to reach key buffer size: " << currentKeyBufferCapacity
       //         <<  " takes " << htime.tv_sec  << "(sec)" << " and " << htime.tv_nsec/1000 << "(us)" <<endl ; 
    }

    //direct copy to the array element
    //LongKeyWithValueTracker keyTracker(kvalues[i], partitions[i],value_tracker);

    keys[sizeTracker].key = kvalues[i];
    keys[sizeTracker].partition = partitions[i];
    keys[sizeTracker].value_tracker = value_tracker;

    sizeTracker++;
  }
}

//NOTE: this method will have to be called indepedenent of whether ordering is required or not.
void MapShuffleStoreWithLongKey::sort(int partitions, bool ordering) {
  //NOTE: This number will need to be initialized!!
  totalNumberOfPartitions = partitions;

  if (ordering) {
     std::sort(keys, keys+sizeTracker, LongKeyWithValueTrackerComparator);
  }
}

MapStatus MapShuffleStoreWithLongKey::writeShuffleData() {
  //(1) identify how big the index chunk should be: key type id, size of value class, and 
  // actual value class definition.
  size_t sizeOfVCclassDefinition = vvTypeDefinition.length; //assume this is the value at this time.
  //if the parition size is  0, then value type definition is 0.

  //NOTE: this layout does not support arbitrary key value definition.
  size_t  indChunkSize =
      sizeof(int) // NUMA node id
      + sizeof(int) // integer key value
      + sizeof(int) // the value size record
      + sizeOfVCclassDefinition // actual value class definition in bytes
      + sizeof(int) // the total number of buckets record
      // list of (global pointer PPtr = <region id, offset> + size of the bucket)
      + totalNumberOfPartitions *(sizeof (uint64_t) + sizeof (uint64_t) + sizeof (int));

  //(2) aquite a region.the offset is part of the object data member.
  int generationId =ShuffleStoreManager::getInstance()->getGenerationId();
  ShuffleDataSharedMemoryManager *memoryManager =
                    ShuffleStoreManager::getInstance()->getShuffleDataSharedMemoryManager();

  //what gets returned is the virtual address in the currnt process
  //the representation of a global null pointer. the constructed pointer is (-1, -1)
  RRegion::TPtr<void> global_null_ptr;

  //what gets returned is the virtual address in the currnt process
  RRegion::TPtr<void> indChunkGlobalPointer = memoryManager->allocate_indexchunk (indChunkSize);
  if (SHMShuffleGlobalConstants::USING_RMB) {
    if (indChunkGlobalPointer != global_null_ptr) {
      indChunkOffset = (unsigned char*) indChunkGlobalPointer.get();

      CHECK (ShuffleStoreManager::getInstance()->check_pmap((void*)indChunkOffset))
        << "address: " << (void*) indChunkOffset << " not in range: ("
        <<  reinterpret_cast <void* > (ShuffleStoreManager::getInstance()->getProcessMap().first)
        << " ,"
        <<  reinterpret_cast <void* > (ShuffleStoreManager::getInstance()->getProcessMap().second);
    } else {
      indChunkOffset = nullptr;
      LOG(ERROR)<< "allocate index chunk returns global null pointer " << " for size: " << indChunkSize
                << " generation id: " << generationId;
    }
  } else {
     indChunkOffset = reinterpret_cast<unsigned char*> (indChunkGlobalPointer.offset());
  }

  //record the global pointer version of index chunk offset
  globalPointerIndChunkRegionId = indChunkGlobalPointer.region_id();
  globalPointerIndChunkOffset = indChunkGlobalPointer.offset();

  CHECK (indChunkOffset != nullptr)
          << "allocate index chunk returns null with generation id: "<< generationId;

  VLOG(2) << " allocated index chunk at offset: "
          << (void*) indChunkOffset <<  " with generation id: " << generationId;

  //the tobeserializedchunk offset --> map output tracker, and then --> the reducer side.
  MapStatus mapStatus(indChunkGlobalPointer.region_id(),
                      indChunkGlobalPointer.offset(), totalNumberOfPartitions, mapId);
  ShuffleDataSharedMemoryWriter  writer;

  //(3) write header (only the key at this time)
  int nodeId {OsUtil::getCurrentNumaNode()};
  memcpy(indChunkOffset, &nodeId, sizeof(nodeId));
  indChunkOffset += sizeof(nodeId);

  int keytypeId = KValueTypeId::Long;
  unsigned char *pic = writer.write_indexchunk_keytype_id(indChunkOffset, keytypeId);
  VLOG(2) << " write index chuunk keytype id: " << keytypeId;

  //write value class size
  pic = writer.write_indexchunk_size_valueclass(pic,sizeOfVCclassDefinition);
  VLOG(2) << " write index chunk vclass definition size  with size: " << sizeOfVCclassDefinition;

  //To write value class definition, if the value type definition is not zero, in the case
  //of partition size = 0.
  if (sizeOfVCclassDefinition > 0) {
    pic = writer.write_indexchunk_valueclass(pic, vvTypeDefinition.definition, sizeOfVCclassDefinition);
    VLOG(2) << " write index chunk vclass definition with size: " << sizeOfVCclassDefinition
            << " and value: " << ByteArrayUtil::to_str(vvTypeDefinition.definition, sizeOfVCclassDefinition);
  }

  //write the number of the total buckets.
  unsigned char  *toLogPicBucketSize=pic;
  pic = writer.write_indexchunk_number_total_buckets(pic, totalNumberOfPartitions);
  VLOG(2) << " write index chunk total number of buckets: " << totalNumberOfPartitions
          << " at memory adress: " << (void*)toLogPicBucketSize;

  vector<int> partitionChunkSizes;
  vector <RRegion::TPtr<void>> allocatedDataChunkOffsets;//global pointers.
  vector <unsigned char*> dataChunkOffsets; //local pointers.

  //initialize per-partiton size and offset.
  for (int i =0; i<totalNumberOfPartitions; i++) {
     partitionChunkSizes.push_back(0);
     //PPtr's version of null pointer
     allocatedDataChunkOffsets.push_back(global_null_ptr);
     dataChunkOffsets.push_back(nullptr);
  }

  //NOTE: keys only contain the partitions that have non-zero buckets.  Some partitions can be
  //empty partition identifier will be from 0 to totalNumberOfPartitions-1
  //we will do the first scan to determine how many data chunks and their sizes that we need. 
  size_t longkey_size=sizeof(long);
  size_t vvalue_size = sizeof(int);

  for (size_t p =0; p<sizeTracker; ++p) {
    int partition = keys[p].partition;
    int currentSize = partitionChunkSizes[partition];
    //each bucket is the array of: <k long value, value-size, value-in-byte-array>
    currentSize +=(longkey_size + vvalue_size + keys[p].value_tracker.value_size);
    partitionChunkSizes[partition]=currentSize;
  }

  for (int i=0; i<totalNumberOfPartitions; i++ ) {
    int partitionChunkSize = partitionChunkSizes[i];
    //null_ptr is defined in pegasus/pointer.hh
    RRegion::TPtr<void> data_chunk (null_ptr) ; //it will be allocated as being the global null ptr. 
    if (partitionChunkSize > 0) {
      //data_chunk is the virtual address in current process.
      data_chunk=
          memoryManager->allocate_datachunk (partitionChunkSize);
      if (SHMShuffleGlobalConstants::USING_RMB) {
        CHECK (data_chunk != global_null_ptr)
                    << "allocate data chunk returns null with generation id: "<<generationId; 
      } else {
          CHECK (reinterpret_cast<void*>(data_chunk.offset()) != nullptr)
              << "allocate data chunk returns null with generatino id: "<<generationId;
      }

      allocatedDataChunkOffsets[i]=data_chunk;

      if (VLOG_IS_ON(2)) {
         if (SHMShuffleGlobalConstants::USING_RMB) {
            VLOG(2) << "data chunk for partition: " << i << " with chunk size: " << partitionChunkSize
                    <<" allocated with offset: " << (void*) data_chunk.get() ;
         } else {
            VLOG(2) << "data chunk for partition: " << i << " with chunk size: " << partitionChunkSize
                    <<" allocated with offset: " << (void*) reinterpret_cast<void*>(data_chunk.offset()) ;
         }
      }
    }

   unsigned char  *toLogPic = pic;

   //the global pointer to be written also encodes the local pointer with -1 being the region id
   pic = writer.write_indexchunk_bucket(pic,
                   data_chunk.region_id(), data_chunk.offset(), partitionChunkSize);

   if (VLOG_IS_ON(2)) {
     if (data_chunk != global_null_ptr) {
       VLOG (2) << "write index chunk bucket for bucket: " << i <<  "with chunk size: " << partitionChunkSize
                << " and starting virtual memory of : " << (void*)data_chunk.get()
                << " at memory address: " <<  (void*)toLogPic;
     } else{
       //NOTE: to check whether data_chunk.get() for (-1, -1) leads to the crash for the get() method.                                                           
       VLOG (2) << "write index chunk bucket for bucket: " << i <<  "with chunk size: " << partitionChunkSize
                << " and starting virtual memory of : " << (void*)nullptr
                << " at memory address: " <<  (void*)toLogPic;
     }
   }

   mapStatus.setBucketSize(i, partitionChunkSize);
  }

  //initialize the local pointers using the allocated global pointers.
  for (int i =0; i<totalNumberOfPartitions; i++) {
     RRegion::TPtr<void> datachunk_offset= allocatedDataChunkOffsets[i];
     unsigned char* local_ptr = nullptr;
     if (SHMShuffleGlobalConstants::USING_RMB) {
        if (datachunk_offset != global_null_ptr) {
            local_ptr = (unsigned char*)datachunk_offset.get();

            CHECK (ShuffleStoreManager::getInstance()->check_pmap((void*)local_ptr))
                << "address: " << (void*)local_ptr << " not in range: ("
                <<  reinterpret_cast <void* > (ShuffleStoreManager::getInstance()->getProcessMap().first)
                << " ,"
                <<  reinterpret_cast <void* > (ShuffleStoreManager::getInstance()->getProcessMap().second);
        } else {
            //if datachunk_offset is a global null ptr, it will be resolved as nullptr.
            local_ptr = nullptr;
        }
     } else {
       //if the partition size is empty, it will return offset -1 from intialized global pointer.
       //when allocatedDataChunkOffsets is initialized
       //since local_ptr will not be triggered when the partition size is empty, it should be OK.
       local_ptr = reinterpret_cast<unsigned char*>(datachunk_offset.offset());
     }

     dataChunkOffsets[i]=local_ptr;
  }

  //now we will write data chunk one by one
  for (size_t p =0; p<sizeTracker; ++p) {
    //scan from the beginning to the end
    //change to next partition if necessary
    int current_partition_number = keys[p].partition;
    unsigned char *current_datachunk_offset = dataChunkOffsets[current_partition_number];
    if (current_datachunk_offset != nullptr) {
      unsigned char  *ptr = current_datachunk_offset;
      ptr = writer.write_datachunk_longkey((unsigned char*) ptr, keys[p].key, keys[p].value_tracker.value_size);
      VLOG (2) << "write data chunk long key for key: "
               << keys[p].key << " and value size: " << keys[p].value_tracker.value_size;
      //retrieve data to data chunk from the buffer manager's managed bytebuffers. 
      bufferMgr.retrieve(keys[p].value_tracker, (unsigned char*)ptr);
      VLOG (2) << "buffer manager populated value to data chunk for key: " << keys[p].key 
               << " and value size: " << keys[p].value_tracker.value_size
               << " at memory address: " << (void*) ptr;

      ptr += keys[p].value_tracker.value_size;
      dataChunkOffsets[current_partition_number] = ptr; //take it back for next key.
    }
  }

  //map status returns the information that later we can get back all of the written shuffle data.
  return mapStatus;
}

void MapShuffleStoreWithLongKey::stop(){
  //free(keys); //free the keys, we will design a pool for it;
  ArrayBufferElement element(currentKeyBufferCapacity,(unsigned char*)keys);
  ShuffleStoreManager::getInstance()->getKeyBufferPool(KValueTypeId::Long)->freeBuffer(element);

  bufferMgr.free_buffers(); //free the occupied values

  LOG(INFO) << "map shuffle store with long keys with map id: " << mapId << " stopped"
            << " buffer pool size: "
            <<  ShuffleStoreManager::getInstance()->getByteBufferPool()->currentSize() <<endl;

  LOG(INFO) << "map shuffle store with long keys with map id: " << mapId << " stopped"
            << " key pool size: "
            <<  ShuffleStoreManager::getInstance()->getKeyBufferPool(KValueTypeId::Long)->currentSize() <<endl;
}

void MapShuffleStoreWithLongKey::shutdown(){
  LOG(INFO) << "map shuffle store with long keys with map id: " << mapId << " is shutting down";

  //to clean up the shared memory region that is allocated for index chunk and data chunks.
  if (indChunkOffset!=nullptr) {
    ShuffleDataSharedMemoryManager *memoryManager =
                      ShuffleStoreManager::getInstance()->getShuffleDataSharedMemoryManager();
    //NOTE: indexchunk_offset is the pointer in virtual address in the owner process.
    RRegion::TPtr<void> globalPointer (globalPointerIndChunkRegionId, globalPointerIndChunkOffset);
    ShuffleDataSharedMemoryManagerHelper::free_indexanddata_chunks(indChunkOffset,
                                                                   globalPointer, memoryManager);
  }

  if (vvTypeDefinition.definition!=nullptr) {
      free (vvTypeDefinition.definition);
      vvTypeDefinition.definition = nullptr;
  }
}
