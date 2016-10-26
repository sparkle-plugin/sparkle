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
#include <string.h>
#include "MapShuffleStoreWithByteArrayKeys.h"
#include "ShuffleConstants.h"
#include "MapShuffleStoreManager.h"
#include "ShuffleDataSharedMemoryManager.h"
#include "ShuffleDataSharedMemoryWriter.h"
#include "ShuffleDataSharedMemoryManagerHelper.h"
#include "VariableLengthKeyComparator.h"
#include "ArrayBufferPool.h"
#include  "MapStatus.h"
#include <byteswap.h>

struct MapShuffleComparatorWithByteArrayKey {

private:
	ExtensibleByteBuffers *kBufferMgr;

public:

	MapShuffleComparatorWithByteArrayKey (ExtensibleByteBuffers *kBr):
		  kBufferMgr(kBr){
	}

	inline bool operator() (const  ByteArrayKeyWithValueTracker &a, const  ByteArrayKeyWithValueTracker &b){
  
	  if (a.partition<b.partition) {
		return true; 
	  } 
          else if (a.partition > b.partition) {
	        return false;
          }
	  else {
	       //the case: (a.partition == b.partition)
	       if (a.normalizedKey < b.normalizedKey) {
	           return true;
	       }
               else if (a.normalizedKey > b.normalizedKey) {
                   return false;
	       }
               else {
                 //the case: a.normalizedKey == b.normalizedKey
		 return VariableLengthKeyComparator::less_than(
			     kBufferMgr, a.key_tracker, b.key_tracker);
	       }//end normalized key comparision
          };

	}
};



MapShuffleStoreWithByteArrayKey::MapShuffleStoreWithByteArrayKey (int buffsize,
							  int mId, bool ordering):
  kBufferMgr(buffsize),
  vBufferMgr(buffsize),
  mapId(mId),
  kvTypeDefinition(KValueTypeId::ByteArray),
  orderingRequired(ordering)
{
  //will be set at the sort call
  totalNumberOfPartitions = 0;

  ArrayBufferElement bElement =
    ShuffleStoreManager::getInstance()->getKeyBufferPool(KValueTypeId::ByteArray)->getBuffer();
  if (bElement.capacity == 0) {
    //ask OS to give me the key buffer.
    keysandvals =
      (ByteArrayKeyWithValueTracker*) malloc(sizeof(ByteArrayKeyWithValueTracker)*
					SHMShuffleGlobalConstants::MAPSHUFFLESTORE_KEY_BUFFER_SIZE);
    currentKVBufferCapacity=SHMShuffleGlobalConstants::MAPSHUFFLESTORE_KEY_BUFFER_SIZE;

  }
  else {
    keysandvals  = (ByteArrayKeyWithValueTracker*)bElement.start_address;
    currentKVBufferCapacity=bElement.capacity;

  }
 
  sizeTracker = 0;
}

MapShuffleStoreWithByteArrayKey::MapShuffleStoreWithByteArrayKey (int mId, bool ordering):
  kBufferMgr(SHMShuffleGlobalConstants::BYTEBUFFER_HOLDER_SIZE),
  vBufferMgr(SHMShuffleGlobalConstants::BYTEBUFFER_HOLDER_SIZE),
  mapId(mId),
  kvTypeDefinition(KValueTypeId::ByteArray),
  orderingRequired(ordering)
{
  //will be set at the sort call
  totalNumberOfPartitions = 0;

  ArrayBufferElement bElement =
    ShuffleStoreManager::getInstance()->getKeyBufferPool(KValueTypeId::ByteArray)->getBuffer();
  if (bElement.capacity == 0) {
    //ask OS to give me the key buffer.
    keysandvals =
      (ByteArrayKeyWithValueTracker*) malloc(sizeof(ByteArrayKeyWithValueTracker)*
					SHMShuffleGlobalConstants::MAPSHUFFLESTORE_KEY_BUFFER_SIZE);
    currentKVBufferCapacity=SHMShuffleGlobalConstants::MAPSHUFFLESTORE_KEY_BUFFER_SIZE;

  }
  else {
    keysandvals = (ByteArrayKeyWithValueTracker*)bElement.start_address;
    currentKVBufferCapacity=bElement.capacity;

  }
 
  sizeTracker = 0;

}


//store both the keys and the values into the corresponding extensible buffers.
//NOTE: have the kvalue lengths to be passed in as a parameter.
void MapShuffleStoreWithByteArrayKey::storeKVPairsWithByteArrayKeys (
   unsigned char *byteHolder, int koffsets[], int voffsets[], 
   int partitions[], int numberOfPairs){

   for (int i=0; i<numberOfPairs; i++) {

     int kLength = 0;
     int kStart = 0;
     int kEnd = 0;
     if (i==0) {
       kLength = koffsets[i];
       kStart = 0;
       kEnd = koffsets[i];
     }
     else {
       kStart = voffsets[i-1];
       kEnd = koffsets[i]; //exclusive 
       kLength = kEnd - kStart;
     }


     int vStart=koffsets[i];
     int vEnd=voffsets[i];
     int vLength = vEnd-vStart;
   
     //for key insertion
     unsigned char *ksegment = byteHolder + kStart;
     PositionInExtensibleByteBuffer key_tracker = kBufferMgr.append(ksegment, kLength);

     //for value insertion
     unsigned char *vsegment = byteHolder + vStart;
     PositionInExtensibleByteBuffer value_tracker =vBufferMgr.append(vsegment, vLength);

     if (sizeTracker == currentKVBufferCapacity) {
       LOG(INFO) << "keysandvals: " << (void*) keysandvals << " with size tracker: " << sizeTracker
		 << " reaches current buffer size: " << currentKVBufferCapacity;
       //re-allocate then.
       currentKVBufferCapacity += SHMShuffleGlobalConstants::MAPSHUFFLESTORE_KEY_BUFFER_SIZE;
       keysandvals = (ByteArrayKeyWithValueTracker*)realloc(keysandvals, 
                                currentKVBufferCapacity*sizeof(ByteArrayKeyWithValueTracker));

     }

     keysandvals[sizeTracker].normalizedKey = 0;
     unsigned long tvalue;
     if (kLength < SHMShuffleGlobalConstants::NORMALIZED_KEY_SIZE) {
       memcpy (&tvalue, ksegment, kLength);
     }
     else {
       memcpy (&tvalue, ksegment, SHMShuffleGlobalConstants::NORMALIZED_KEY_SIZE);
     }


     //swap, so that we can do unsigned long comparision on little-endian machine.
     //our machine is little-endian, but the byte[] comparator, the first byte is most                                                 
     //signficant, so we will have to do the byte-swapping and then use unsigned long for comparision                                  
     keysandvals[sizeTracker].normalizedKey = bswap_64(tvalue);

     keysandvals[sizeTracker].partition = partitions[i];
     keysandvals[sizeTracker].key_tracker = key_tracker;
     keysandvals[sizeTracker].value_tracker = value_tracker;


     sizeTracker++;

  }
}

void MapShuffleStoreWithByteArrayKey::sort(int partitions, bool ordering) {
    totalNumberOfPartitions = partitions;
    //NOTE: we can pass in object into sort, not just the class. 
    if (ordering) {
      std::sort(keysandvals, keysandvals+sizeTracker, MapShuffleComparatorWithByteArrayKey(&kBufferMgr));
    }

}

MapStatus MapShuffleStoreWithByteArrayKey::writeShuffleData() {
  //(1) identify how big the index chunk should be: key type id, size of value class, and 
  // actual value class definition. 
  size_t sizeOfVCclassDefinition = vvTypeDefinition.length; //assume this is the value at this time.
  //if the parition size is  0, then value type definition is 0
  //CHECK(sizeOfVCclassDefinition > 0);

  //first one is integer key value, second one is the value size record in one integer,
  //third one is actual value class definition in bytes
  //fourth one is total number of buckets record in one integer.
  //the fifth one is list of (global pointer PPtr = <region id, offset> + size of the bucket)
  //NOTE: this layout does not support arbitrary key value definition. 
  size_t  indChunkSize = sizeof (int) + sizeof(int)  
         + sizeOfVCclassDefinition  
         + sizeof(int)
         + totalNumberOfPartitions *(sizeof (uint64_t) + sizeof (uint64_t) +  sizeof (int));
   
  //(2) aquire a generation.the offset is part of the object data member.
  uint64_t generationId =ShuffleStoreManager::getInstance()->getGenerationId();
  ShuffleDataSharedMemoryManager *memoryManager =
      ShuffleStoreManager::getInstance()->getShuffleDataSharedMemoryManager();
 
  //what gets returned is the virtual address in the currnt process 
  //the representation of a global null pointer. the constructed pointer is (-1, -1)
  RRegion::TPtr<void> global_null_ptr;
  
  //what gets returned is the virtual address in the current process 
  RRegion::TPtr<void> indChunkGlobalPointer = memoryManager->allocate_indexchunk (indChunkSize);
  if (SHMShuffleGlobalConstants::USING_RMB) {
    if (indChunkGlobalPointer != global_null_ptr) {
      indChunkOffset = (unsigned char*) indChunkGlobalPointer.get();
 
      CHECK (ShuffleStoreManager::getInstance()->check_pmap((void*)indChunkOffset))
        << "address: " << (void*) indChunkOffset << " not in range: ("
        <<  reinterpret_cast <void* > (ShuffleStoreManager::getInstance()->getProcessMap().first)
        << " ,"
        <<  reinterpret_cast <void* > (ShuffleStoreManager::getInstance()->getProcessMap().second);

    }
    else {
      indChunkOffset = nullptr;
      LOG(ERROR)<< "allocate index chunk returns global null pointer " << " for size: " << indChunkSize
                << " generation id: " << generationId;

    }
  }
  else {
    indChunkOffset = reinterpret_cast<unsigned char*> (indChunkGlobalPointer.offset());
  }

  //record the global pointer version of index chunk offset
  globalPointerIndChunkRegionId = indChunkGlobalPointer.region_id();
  globalPointerIndChunkOffset = indChunkGlobalPointer.offset();

  CHECK (indChunkOffset != nullptr) 
          << "allocate index chunk returns null with generation id: "<< generationId;
  
  VLOG(2) << " allocated index chunk at offset: " 
          << (void*) indChunkOffset <<  " with generation id: " << generationId;

  //the tobeserializedchunk offset --> map output tracker, and then --> the reducer side
  MapStatus  mapStatus (indChunkGlobalPointer.region_id(),
                        indChunkGlobalPointer.offset(), totalNumberOfPartitions, mapId);
  ShuffleDataSharedMemoryWriter  writer;
  
  //(3) write header (only the key at this time)
  int keytypeId = KValueTypeId::ByteArray;
  unsigned char *pic = writer.write_indexchunk_keytype_id(indChunkOffset, keytypeId);
  VLOG(2) << " write index chunk keytype id: " << keytypeId;

  //write value class size
  pic = writer.write_indexchunk_size_valueclass(pic,sizeOfVCclassDefinition);
  VLOG(2) << " write index chunk vclass definition size  with size: " << sizeOfVCclassDefinition;

  //To write value class definition, if the value type definition is not zero, in the case
  //of partition size = 0.
  if (sizeOfVCclassDefinition > 0) { 
    pic = writer.write_indexchunk_valueclass(pic, vvTypeDefinition.definition, sizeOfVCclassDefinition);
    VLOG(2) << " write index chunk vclass definition with size: " << sizeOfVCclassDefinition;
  }

  //write the number of the total buckets.
  unsigned char  *toLogPicBucketSize=pic;
  pic = writer.write_indexchunk_number_total_buckets(pic, totalNumberOfPartitions);
  VLOG(2) << " write index chunk total number of buckets: " << totalNumberOfPartitions
          << " at memory adress: " << (void*)toLogPicBucketSize; 

  vector<int> partitionChunkSizes; 
  vector <RRegion::TPtr<void>> allocatedDataChunkOffsets;//global pointers.
  vector <unsigned char *> dataChunkOffsets; //local pointers

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

  size_t barraykey_size= sizeof(int); //the length, which is in "int"
  size_t vvalue_size = sizeof(int); //the length of the value in byte array, which is in "int"

  for (size_t p =0; p<sizeTracker; ++p) {
    int partition = keysandvals[p].partition;
    int currentSize = partitionChunkSizes[partition];

    //each bucket is the array of:
    // <byte array key size value, actual key-value-in-byte-array, value-size, value-in-byte-array>
    currentSize +=(barraykey_size + keysandvals[p].key_tracker.value_size 
                                   + vvalue_size + keysandvals[p].value_tracker.value_size);
    partitionChunkSizes[partition]=currentSize; 
  }

  for (int i=0; i<totalNumberOfPartitions; i++ ) {
    int partitionChunkSize = partitionChunkSizes[i];
    //null_ptr is defined in pegasus/pointer.hh
    RRegion::TPtr<void> data_chunk (null_ptr) ; //it will be allocated as being the global null ptr
    if (partitionChunkSize > 0) {
      //data_chunk is the virtual address in current process
      data_chunk= 
              memoryManager->allocate_datachunk (partitionChunkSize);
      if (SHMShuffleGlobalConstants::USING_RMB) {
        CHECK (data_chunk != global_null_ptr) 
                    << "allocate data chunk returns null with generation id: "<<generationId;
      }
      else {
        CHECK (reinterpret_cast<void*>(data_chunk.offset()) != nullptr)
               << "allocate data chunk returns null with generation id: "<<generationId;
      }

      allocatedDataChunkOffsets[i]=data_chunk;

      if (VLOG_IS_ON(2)) {
	if (SHMShuffleGlobalConstants::USING_RMB) {
	  VLOG(2) << "data chunk for partition: " << i << " with chunk size: " << partitionChunkSize
		  <<" allocated with memory address: " << (void*) data_chunk.get() ;
	}
	else {
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
      }
      else {
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
	local_ptr =(unsigned char*) datachunk_offset.get();

	CHECK (ShuffleStoreManager::getInstance()->check_pmap((void*)local_ptr))
	  << "address: " << (void*)local_ptr << " not in range: ("
	  <<  reinterpret_cast <void* > (ShuffleStoreManager::getInstance()->getProcessMap().first)
	  << " ,"
	  <<  reinterpret_cast <void* > (ShuffleStoreManager::getInstance()->getProcessMap().second);
      }
      else {
	//if datachunk_offset is a global null ptr, it will be resolved as nullptr.
        local_ptr = nullptr;
      }
    }
    else {
      //if the partition size is empty, it will return offset -1 from intialized global pointer.
      //when allocatedDataChunkOffsets is initialized
      //since local_ptr will not be triggered when the partition size is empty, it should be OK.
      local_ptr = reinterpret_cast<unsigned char*>(datachunk_offset.offset());
    }

    dataChunkOffsets[i]=local_ptr;
  }

  //NOTE: the following is diffferent int-key map shuffle store.
  //now we will write data chunk one by one 
  for (size_t p =0; p<sizeTracker; ++p) {
    //scan from the beginning to the end
    //change to next partition if necessary 
    int current_partition_number = keysandvals[p].partition; 
    unsigned char *current_datachunk_offset = dataChunkOffsets[current_partition_number];
    if (current_datachunk_offset != nullptr) {
      unsigned char  *ptr = current_datachunk_offset; 
      // <string integer size value, key-value-in-byte-array, value-size, value-in-byte-array>
      //(1) write the length of the key
      int key_length = keysandvals[p].key_tracker.value_size;
      ptr=writer.write_datachunk_value (ptr, (unsigned char*)&key_length, sizeof(key_length));
      //(2)actual key value 
      kBufferMgr.retrieve(keysandvals[p].key_tracker, (unsigned char*) ptr); 
      VLOG (2) << "write data chunk byte array key with key value size: " << keysandvals[p].key_tracker.value_size
          	 << " at memory address: " << (void*)ptr; 
      ptr += keysandvals[p].key_tracker.value_size;

      
      //(3) value size 
      int value_size = keysandvals[p].value_tracker.value_size; 
      ptr=writer.write_datachunk_value (ptr, (unsigned char*)&value_size, sizeof(value_size));
      //(4) actual value.
      vBufferMgr.retrieve(keysandvals[p].value_tracker, (unsigned char*)ptr);
      VLOG (2) << "buffer manager populated value to data chunk for byte array key's corresponding value size: "
               << keysandvals[p].value_tracker.value_size 
               << " at memory address: " << (void*) ptr;
      //for (int i=0; i<p->value_tracker.value_size; i++) { 
      // unsigned char v= *(ptr+i);
      // VLOG(2) << "****NVM write at address: " << (void*) (ptr+i)<<  " with value: " << (int) v;
      //} 

      ptr += keysandvals[p].value_tracker.value_size;
      dataChunkOffsets[current_partition_number] = ptr; //take it back for next key.
    }
  }

  //map status returns the information that later we can get back all of the written shuffle data.
  return mapStatus; 
}


void MapShuffleStoreWithByteArrayKey::stop(){
    //free (keysvals); //free the keys, we will design a pool for it;
    ArrayBufferElement element(currentKVBufferCapacity,(unsigned char*)keysandvals);
    ShuffleStoreManager::getInstance()->getKeyBufferPool(KValueTypeId::ByteArray)->freeBuffer(element);
    
    kBufferMgr.free_buffers(); //free the occupied values for key value buffer manager
    vBufferMgr.free_buffers(); //free the occupied values for value value buffer manager
    
    LOG(INFO) << "map shuffle store with byte array keys with map id: " << mapId << " stopped"
	      << " buffer pool size: "
	      <<  ShuffleStoreManager::getInstance()->getByteBufferPool()->currentSize() <<endl;

    LOG(INFO) << "map shuffle store with byte array keys with map id: " << mapId << " stopped"
	      << " key pool size: "
	      <<  ShuffleStoreManager::getInstance()->getKeyBufferPool(KValueTypeId::ByteArray)->currentSize() <<endl;

}

void MapShuffleStoreWithByteArrayKey::shutdown(){
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


