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
#include "GenericReduceChannel.h"
#include "ShuffleDataSharedMemoryReader.h"
#include "MapShuffleStoreManager.h"
#include "ShuffleStoreManager.h"
#include "ShuffleConstants.h"
#include "ShuffleDataSharedMemoryManager.h"

void GenericReduceChannel::init() {
     CHECK_NE(mapBucket.mapId, -1);

     RRegion::TPtr<void> global_null_ptr;

     uint64_t gregion_id = mapBucket.regionId; 
     uint64_t goffset = mapBucket.offset;

     unsigned char *indexchunk_offset = nullptr;
     if (SHMShuffleGlobalConstants::USING_RMB){
	  RRegion::TPtr<void> globalpointer(gregion_id, goffset);
          if (globalpointer != global_null_ptr) {
             indexchunk_offset = (unsigned char*) globalpointer.get();
             //check that global pointer mapped to valid local address of shared-memory region
	     CHECK (ShuffleStoreManager::getInstance()->check_pmap((void*)indexchunk_offset))
	       << "retrieved start index chunk address: " << (void*) indexchunk_offset << " not in range: ("
	       <<  reinterpret_cast <void* > (ShuffleStoreManager::getInstance()->getProcessMap().first)
	       << " ,"
	       <<  reinterpret_cast <void* > (ShuffleStoreManager::getInstance()->getProcessMap().second);
	  }
          //else it is still nullptr;
     }
     else{
	 //local memory allocator 
         indexchunk_offset = reinterpret_cast<unsigned char *>(goffset);
     }


     //we expect that the init will be called only when the length of the bucket is not zero.
     CHECK (indexchunk_offset != nullptr);
     unsigned char *p = indexchunk_offset;

     VLOG(2) << "retrieved index chunk offset is: " << (void*)indexchunk_offset;
     int keytypeId = ShuffleDataSharedMemoryReader::read_indexchunk_keytype_id(p);
     VLOG(2) << "retrieved index chunk key type id is: " << keytypeId;

     p += sizeof(keytypeId);

     int kclass_size = 0;
     int vclass_size = 0;
     //so far, we just skipped reading key Value and Value Value.
     //unsigned char *kvalue=nullptr;
     //unsigned char *vvalue=nullptr;

    if ((keytypeId == KValueTypeId::Int)
	   || (keytypeId == KValueTypeId::Long)
	   || (keytypeId == KValueTypeId::Float)
	   || (keytypeId == KValueTypeId::Double)
	   || (keytypeId == KValueTypeId::ByteArray)
	   || (keytypeId == KValueTypeId::String)){

	vclass_size = ShuffleDataSharedMemoryReader::read_indexchunk_size_valueclass(p);
	VLOG(2) << "retrieved index chunk value class size: " << vclass_size;

	p += sizeof(vclass_size);      //advance to value's first byte.                                                           
	//skip reading actual Key'value and Value's value. just advance the pointer                                    
	p += vclass_size;
     }
     else if (keytypeId == KValueTypeId::Object) {
	kclass_size = ShuffleDataSharedMemoryReader::read_indexchunk_size_keyclass(p);
	p += sizeof(kclass_size);
	vclass_size = ShuffleDataSharedMemoryReader::read_indexchunk_size_valueclass(p);
	p += sizeof(vclass_size);

  	//skip to read actual Key'value and Value's value. just advance the pointer                                    
	p += kclass_size;
	p += vclass_size;
     }

     int buckets_size = 0;
     unsigned char *toLogPicBucketSize = p;
     ShuffleDataSharedMemoryReader::read_indexchunk_number_total_buckets(p, &buckets_size);
 
     VLOG(2) << "retrieved index chunk bucket size: " << buckets_size
   	     << " at memory address: " << (void*)toLogPicBucketSize;

     p += sizeof(buckets_size);

     //note: we may do range checking here.
     CHECK_EQ(buckets_size, totalNumberOfPartitions);

     //then we will move to the position specified by the reduce id, which starts from 0, to 
     //the number of partitions
     uint64_t datachunk_gregion_id = -1;
     uint64_t datachunk_goffset = -1;
     int  bsize = 0;
     //NOTE: it is the reduce id that I need to retrieve from this map bucket, note map id.
     p += reducerId *(sizeof(datachunk_gregion_id)+ sizeof(datachunk_goffset) + sizeof(bsize));

     unsigned char *toLogPicOffset = p;
     ShuffleDataSharedMemoryReader::read_indexchunk_bucket(p,
                                           &datachunk_gregion_id, &datachunk_goffset, &bsize);
     
     VLOG(2) << "retrieved index chunk bucket for map id: " << mapBucket.mapId
	     << " with data chunk region id: " << (void*)datachunk_gregion_id
 	     << " with data chunk offset: " << (void*)datachunk_goffset
             << " and size: " << bsize
	     << " at memory address: " << (void*)toLogPicOffset;
     //NOTE: we also need to update the total length of the map bucket. as what gets passed 
     //from  the Spark scheduler to the reducer, is just an approximation. 
     totalLength = bsize; 
     if (SHMShuffleGlobalConstants::USING_RMB) {
         RRegion::TPtr<void> globalpointer(datachunk_gregion_id, datachunk_goffset);
         if (globalpointer != global_null_ptr) {
           currentPtr =(unsigned char*) globalpointer.get();
           //check current pointer is within local address of shared-memory region
	   CHECK (ShuffleStoreManager::getInstance()->check_pmap((void*)currentPtr))
	     << "retrieved start data chunk address: " << (void*) currentPtr << " not in range: ("
	     <<  reinterpret_cast <void* > (ShuffleStoreManager::getInstance()->getProcessMap().first)
	     << " ,"
	     <<  reinterpret_cast <void* > (ShuffleStoreManager::getInstance()->getProcessMap().second);
	 }

     }
     else {
         //local memory allocator
	 currentPtr = reinterpret_cast<unsigned char*>(datachunk_goffset);
     }

     //further check that the last address of the data chunk is still in the valid local adress range
     //for the shared-memory region 
     unsigned char *end_datachunk_addr = currentPtr + totalLength;
     CHECK (ShuffleStoreManager::getInstance()->check_pmap((void*)end_datachunk_addr))
       << "address: " << (void*) end_datachunk_addr << " not in range: ("
       <<  reinterpret_cast <void* > (ShuffleStoreManager::getInstance()->getProcessMap().first)
       << " ,"
       <<  reinterpret_cast <void* > (ShuffleStoreManager::getInstance()->getProcessMap().second);
}



void GenericReduceChannel::shutdown() {
  //do nothing in this generic channel class.
}
