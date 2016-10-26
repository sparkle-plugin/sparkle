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
#include "MapShuffleStoreWithStringKeys.h"
#include "ShuffleConstants.h"
#include "MapShuffleStoreManager.h"
#include  "ShuffleDataSharedMemoryManager.h"
#include  "ShuffleDataSharedMemoryWriter.h"
#include "ShuffleDataSharedMemoryManagerHelper.h"
#include  "MapStatus.h"

//the sort with the data structure of StringKeyWithValueTracker is relatively heavy.
//for further optimization, we can create a further indirect key vector that holds 
//StringKeyWithValueTracker value and the vector pointer to hold all of the
//StringKeyWithValueTracker values.
struct MapShuffleComparatorWithStringKey {

private:
	ExtensibleByteBuffers *kvBufferMgr;

public:

	MapShuffleComparatorWithStringKey (ExtensibleByteBuffers *kvBr):
		  kvBufferMgr(kvBr){
	}

	bool operator() (const StringKeyWithValueTracker &a, const StringKeyWithValueTracker &b){
  
	  if (a.partition<b.partition) {
		return true; 
	  } 
          else if (a.partition > b.partition) {
	        return false;
          }
	  else if (a.partition == b.partition) {
	       //if the normalized key comparision show the same result, 
	       if (a.normalizedKey < b.normalizedKey) {
	           return true;
	       }
               else if (a.normalizedKey > b.normalizedKey) {
                   return false;
	       }
               else  if (a.normalizedKey == b.normalizedKey) {
                  bool comp_result=false;

		  //get to the actual key, we advance char by char, until it is done.
		  //we may need to get only the first several chars in most cases.
		  bool done=false;
		  int a_buffer=a.key_tracker.start_buffer;
		  int a_position=a.key_tracker.position_in_start_buffer;
		  int a_scanned = 0;

		  int b_buffer=b.key_tracker.start_buffer;
		  int b_position=b.key_tracker.position_in_start_buffer;
		  int b_scanned = 0;

		  int a_buffer_capacity=kvBufferMgr->buffer_at(a_buffer).capacity();
		  int b_buffer_capacity=kvBufferMgr->buffer_at(a_buffer).capacity();
    
		  while (!done) {
		    unsigned char va = kvBufferMgr->buffer_at(a_buffer).value_at(a_position);
		    unsigned char vb = kvBufferMgr->buffer_at(b_buffer).value_at(b_position);
		    a_scanned++;
		    b_scanned++;

		    //we get the winner.
		    if (va < vb) {
		  	 done = true;
			 comp_result=true;
		    }
		    else if (va > vb) {
			done = true;
			comp_result=false;
		    }
		    else {
		      //need to advance to next character.
		      //one of them exhausted, the other still have more
		      if ((a_scanned ==a.key_tracker.value_size) 
				  || (b_scanned == b.key_tracker.value_size)){
			  int a_remained = a.key_tracker.value_size-a_scanned;
			  int b_remained = b.key_tracker.value_size-b_scanned;
			  if (a_remained <b_remained) {
				 //b wins
				 done = true;
				 comp_result=true;
		          }
		          else {
			   //they are truely identical or a wins, in either case,
			   done = true;
			   comp_result = false;
		          }
		       }

			//both can advance to next char
			a_position++;
			b_position++;
        
			if (a_position == a_buffer_capacity) {
  		            a_buffer++;
			    a_position=0;
		        }
         
			if (b_position == b_buffer_capacity) {
  		           b_buffer++;
			   b_position=0;
		        }

		     }//end else
  
		  }//end while 
   
		  return comp_result;
   
	       }//end normalized key comparision
          };

         //to make compiler happy
         return true;
	}
};



//store both the keys and the values into the corresponding extensible buffers.
//NOTE: have the kvalue lengths to be passed in as a parameter.
void MapShuffleStoreWithStringKey::storeKVPairsWithStringKeys (
      unsigned char *byteHolder, int voffsets[],
      char *kvalues[], int kvalueLength[],  int partitions[], int numberOfPairs){
   for (int i=0; i<numberOfPairs; i++) {

     int vStart=0;
     if(i>0) {
        vStart = voffsets[i-1];
     }
     int vEnd=voffsets[i];
     int vLength = vEnd-vStart;
   
     unsigned char *segment = byteHolder + vStart;
     //for value
     PositionInExtensibleByteBuffer value_tracker =kvBufferMgr.append(segment,vLength);
     //for key
     PositionInExtensibleByteBuffer key_tracker =
          kvBufferMgr.append((unsigned char*)kvalues[i], kvalueLength[i]);

     StringKeyWithValueTracker  kvTracker (partitions[i], (unsigned char*)kvalues[i], kvalueLength[i], 
                     key_tracker, value_tracker);
    
     keys.push_back(kvTracker);

     sizeTracker++;

  }
}

void MapShuffleStoreWithStringKey::sort(int partitions) {
    totalNumberOfPartitions = partitions;
    //NOTE: we can pass in object into sort, not just the class. 
    std::sort(keys.begin(), keys.end(), MapShuffleComparatorWithStringKey (&kvBufferMgr));
}


MapStatus MapShuffleStoreWithStringKey::writeShuffleData() {
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
  
  //what gets returned is the virtual address in the currnt process 
  RRegion::TPtr<void> indChunkGlobalPointer = memoryManager->allocate_indexchunk (indChunkSize);
  if (SHMShuffleGlobalConstants::USING_RMB) {
    if (indChunkGlobalPointer != global_null_ptr) {
      indChunkOffset = (unsigned char*) indChunkGlobalPointer.get();
    }
    else {
      indChunkOffset = nullptr;
    }
  }
  else {
    indChunkOffset = reinterpret_cast<unsigned char*> (indChunkGlobalPointer.offset());
  }

  //record the global pointer version of index chunk offset
  globalPointerIndChunkRegionId = indChunkGlobalPointer.region_id();
  globalPointerIndChunkOffset = indChunkGlobalPointer.offset();

  CHECK (indChunkOffset != nullptr) << "allocate index chunk returns null with generation id: "<< generationId;
  VLOG(2) << " allocated index chunk at offset: " 
          << (void*) indChunkOffset <<  " with generation id: " << generationId;

  MapStatus  mapStatus (indChunkGlobalPointer.region_id(),
                        indChunkGlobalPointer.offset(), totalNumberOfPartitions, mapId);
  ShuffleDataSharedMemoryWriter  writer;
  
  //(3) write header (only the key at this time)
  int keytypeId = KValueTypeId::String;
  unsigned char *pic = writer.write_indexchunk_keytype_id(indChunkOffset, keytypeId);
  VLOG(2) << " write index chuunk keytype id: " << keytypeId;

  //write value class size
  pic = writer.write_indexchunk_size_valueclass(pic,sizeOfVCclassDefinition);
  VLOG(2) << " write index chunk vclass definition size  with size: " << sizeOfVCclassDefinition;

  if (sizeOfVCclassDefinition > 0) { 
    pic = writer.write_indexchunk_valueclass(pic,vvTypeDefinition.definition, sizeOfVCclassDefinition);
    VLOG(2) << " write index chunk vclass definition with size: " << sizeOfVCclassDefinition;
  }

  //write the number of the total buckets.
  unsigned char  *toLogPicBucketSize=pic;
  pic = writer.write_indexchunk_number_total_buckets(pic, totalNumberOfPartitions);
  VLOG(2) << " write index chunk total number of buckets: " << totalNumberOfPartitions
          << " at memory adress: " << (void*)toLogPicBucketSize; 

  //the representation of a global null pointer.
  vector<int> partitionChunkSizes; 
  vector <RRegion::TPtr<void>> allocatedDataChunkOffsets;//global pointers.
  vector <unsigned char *> dataChunkOffsets; 

  //initialize per-partiton size and offset.
  for (int i =0; i<totalNumberOfPartitions; i++) {
     partitionChunkSizes.push_back(0);
     //PPtr's version of null pointer 
     allocatedDataChunkOffsets.push_back(global_null_ptr);
     dataChunkOffsets.push_back(nullptr); //zero, basically. 
  }
   
  //NOTE: keys only contain the partitions that have non-zero buckets.  Some partitions can be
  //empty partition identifier will be from 0 to totalNumberOfPartitions-1 
  //we will do the first scan to determine how many data chunks and their sizes that we need. 
  size_t stringkey_size= sizeof(int);
  size_t vvalue_size = sizeof(int);

  //NOTE: the following code segment is a different from int-key map shuffle store. please check.
  for (auto p =keys.begin(); p!=keys.end(); ++p) {
    int partition = p->partition; 
    int currentSize = partitionChunkSizes[partition];
    //each bucket is the array of:
    // <string integer size value, key-value-in-byte-array, value-size, value-in-byte-array>
    currentSize +=(stringkey_size + p->key_tracker.value_size + vvalue_size + p->value_tracker.value_size);
    partitionChunkSizes[partition]=currentSize; 
  }

  for (int i=0; i<totalNumberOfPartitions; i++ ) {
    int partitionChunkSize = partitionChunkSizes[i];
    RRegion::TPtr<void> data_chunk (-1, -1) ;
    if (partitionChunkSize > 0) {
      data_chunk= 
              memoryManager->allocate_datachunk (partitionChunkSize);
      if (SHMShuffleGlobalConstants::USING_RMB) {
        CHECK (data_chunk != global_null_ptr) << "allocate data chunk returns null with generation id: "<<generationId;
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
		  <<" allocated with memory address: " << (void*) reinterpret_cast<void*>(data_chunk.offset()) ;
	}

      }

    }

    unsigned char  *toLogPic = pic;
    //the global pointer to be written also encodes the local pointer with -1 being the region id  
    pic = writer.write_indexchunk_bucket(pic,
					 data_chunk.region_id(), data_chunk.offset(), partitionChunkSize);

    if (VLOG_IS_ON(2)) {
      VLOG (2) << "write index chunk bucket for bucket: " << i <<  "with chunk size: " << partitionChunkSize
	       << " and starting offset of : " << (void*)data_chunk.get()
	       << " at memory address: " <<  (void*)toLogPic;
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
      }
    }
    else {
      local_ptr = reinterpret_cast<unsigned char*>(datachunk_offset.offset());
    }

    dataChunkOffsets[i]=local_ptr;
  }

  //NOTE: the following is diffferent int-key map shuffle store.
  for (auto p=keys.begin(); p!=keys.end(); ++p) {
    //scan from the beginning to the end
    //change to next partition if necessary 
    int current_partition_number = p->partition; 
    unsigned char *current_datachunk_offset = dataChunkOffsets[current_partition_number];
    if (current_datachunk_offset != nullptr) {
      unsigned char  *ptr = current_datachunk_offset; 
      // <string integer size value, key-value-in-byte-array, value-size, value-in-byte-array>
      //(1) write the length of the key
      int key_length = p->key_tracker.value_size;
      ptr=writer.write_datachunk_value (ptr, (unsigned char*)&key_length, sizeof(key_length));
      //(2)actual key value 
      if (p->key_tracker.value_size <= SHMShuffleGlobalConstants::NORMALIZED_KEY_SIZE) {
        VLOG (2) << "write data chunk normalized string key with key value size: " << p->key_tracker.value_size
                 << " at memory address: " << (void*) ptr;
        long normalizedKey = p->normalizedKey;
	ptr = writer.write_datachunk_normalizedstringkey (ptr, &normalizedKey, p->key_tracker.value_size);

      }
      else {
        kvBufferMgr.retrieve(p->key_tracker, (unsigned char*) ptr); 
        VLOG (2) << "write data chunk string key with key value size: " << p->key_tracker.value_size
          	 << " at memory address: " << (void*)ptr; 
        ptr += p->key_tracker.value_size;

      }
      //(3) value size 
      int value_size = p->value_tracker.value_size; 
      ptr=writer.write_datachunk_value (ptr, (unsigned char*)&value_size, sizeof(value_size));
      //(4) actual value.
      kvBufferMgr.retrieve(p->value_tracker, (unsigned char*)ptr);
      VLOG (2) << "buffer manager populated value to data chunk for string key with value size: "
               << p->value_tracker.value_size 
               << " at memory address: " << (void*) ptr;

      //for (int i=0; i<p->value_tracker.value_size; i++) {
      // unsigned char v= *(ptr+i);
      // VLOG(2) << "****NVM write at address: " << (void*) (ptr+i)<<  " with value: " << (int) v;
      //}
      ptr += p->value_tracker.value_size;
      dataChunkOffsets[current_partition_number] = ptr; //take it back for next key.
    }
  }

  //map status returns the information that later we can get back all of the written shuffle data.
  return mapStatus; 
}


void MapShuffleStoreWithStringKey::stop(){
    keys.clear();
    //NOTE: do we need to call free to buffer manager?
}

void MapShuffleStoreWithStringKey::shutdown(){
   //NOTE: need to be filled in later, for shared-memory management
   VLOG (2) << "map shuffel stoer with string keys with map id: " << mapId << " is shutdown";
   
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


