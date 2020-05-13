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
#include "ShuffleConstants.h"
#include "MergeSortChannelHelper.h"
#include "ShuffleDataSharedMemoryReader.h"
#include "ShuffleDataSharedMemoryManager.h"

void MergeSortChannelHelper::obtain_kv_definition (
     MapBucket &mapBucket, int rId, int rPartitions,
     KValueTypeDefinition &kd, VValueTypeDefinition &vd){

     CHECK_NE(mapBucket.mapId, -1);

     RRegion::TPtr<void> global_null_ptr;

     uint64_t gregion_id = mapBucket.regionId;
     uint64_t goffset = mapBucket.offset;
     unsigned char *indexchunk_offset = nullptr; //local pointer

     if (SHMShuffleGlobalConstants::USING_RMB){
       RRegion::TPtr<void> globalpointer(gregion_id, goffset);
       if (globalpointer != global_null_ptr) {
         indexchunk_offset =(unsigned char*)globalpointer.get();

	 //check the null pointer is within local memory address of the mapped shared-memory region
         CHECK (ShuffleStoreManager::getInstance()->check_pmap((void*)indexchunk_offset))
	   << "retrieved start data chunk address: " << (void*) indexchunk_offset << " not in range: ("
	   <<  reinterpret_cast <void* > (ShuffleStoreManager::getInstance()->getProcessMap().first)
	   << " ,"
	   <<  reinterpret_cast <void* > (ShuffleStoreManager::getInstance()->getProcessMap().second);
       }
	     
     }
     else{
       //allocated from local memory 
       indexchunk_offset = reinterpret_cast<unsigned char *>(goffset);
     }

     unsigned char *p = indexchunk_offset;

     p += sizeof(int);

     VLOG(2) << "retrieved index chunk offset: " << (void*)indexchunk_offset;
     CHECK (indexchunk_offset != nullptr);

     int keytypeId = ShuffleDataSharedMemoryReader::read_indexchunk_keytype_id(p);
     kd.typeId = static_cast<KValueTypeId>(keytypeId); 

     VLOG(2) << "retrieved index chunk key type id is: " << kd.typeId;

     p += sizeof(keytypeId);

 
    int kclass_size = 0;
    int vclass_size = 0;
    //so far, we just skipped reading key Value and Value Value.
    if ((keytypeId == KValueTypeId::Int)
	|| (keytypeId == KValueTypeId::Long)
	|| (keytypeId == KValueTypeId::Float)
	|| (keytypeId == KValueTypeId::Double)
	|| (keytypeId == KValueTypeId::String)){

	vclass_size = ShuffleDataSharedMemoryReader::read_indexchunk_size_valueclass(p);
        vd.length = vclass_size; 
	VLOG(2) << "retrieved index chunk value class size: " << vclass_size;
        
	p += sizeof(vclass_size);      //advance to value's first byte.
	//now to read actual Key'value and Value's value. memory allocation will be freed later 
        //by reduce shuffle store.
        if (vclass_size > 0) { 
          vd.definition = (unsigned char*) malloc(vclass_size); 
          ShuffleDataSharedMemoryReader::read_indexchunk_valueclass (p, vd.definition, vclass_size);
          p += vclass_size;
	}
    }
    else if (keytypeId == KValueTypeId::Object) {
	kclass_size = ShuffleDataSharedMemoryReader::read_indexchunk_size_keyclass(p);
	p += sizeof(kclass_size);
	vclass_size = ShuffleDataSharedMemoryReader::read_indexchunk_size_valueclass(p);
	p += sizeof(vclass_size);

	//WARNING: I am now skipp to read actual Key'value and Value's value. just advance the pointer
        p += kclass_size;
	p += vclass_size;
   }

}
