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
#include "MergeSortChannelHelper.h"
#include "ReduceShuffleStoreManager.h"
#include "ReduceShuffleStoreWithIntKeys.h"
#include "ReduceShuffleStoreWithLongKeys.h"
#include "ReduceShuffleStoreWithByteArrayKeys.h"
#include "ReduceShuffleStoreWithObjKeys.h"

void ReduceShuffleStoreManager::obtain_kv_definition (MapBucket &mapBucket, int rId, int rPartitions,
	     KValueTypeDefinition &kd, VValueTypeDefinition &vd){
  MergeSortChannelHelper::obtain_kv_definition(mapBucket, rId, rPartitions, kd, vd);
}


GenericReduceShuffleStore* ReduceShuffleStoreManager::createStore(int shuffleId,
                           int reducerId, 
			   const ReduceStatus &status, int partitions, 
                           ExtensibleByteBuffers *kBMgr, ExtensibleByteBuffers *vBMgr, 
		           unsigned char *passThroughBuffer, size_t buff_capacity,
			   enum KValueTypeId tid, 
                           bool ordering, bool aggregation){

    GenericReduceShuffleStore  *store=nullptr;
    switch (tid) {
        case KValueTypeId::Int: 
          {
	    LOG(FATAL) << "int key reduce shuffle store can not be created via this method";
            break;
	  }
         case KValueTypeId::Long:
          {
	     LOG(FATAL) << "long key reduce shuffle store can not be created via this method";
	     break;
          } 

         case KValueTypeId::Float:
          {
	       LOG(FATAL) << "long key reduce shuffle store can not be created via this method"; 
	       break;
          } 

         case KValueTypeId::Double:
          {
	       LOG(FATAL) << "long key reduce shuffle store can not be created via this method" ;
	       break;
          } 

         case KValueTypeId::String:
          {
     	       //to be implemented.
	       break;
          } 

         case KValueTypeId::ByteArray:
          {
	    store = (GenericReduceShuffleStore*)
		 new ReduceShuffleStoreWithByteArrayKey(status, partitions, reducerId, 
		      kBMgr,vBMgr,
                      passThroughBuffer, buff_capacity, ordering, aggregation);
            LOG(INFO) << "create bytearray-key reduce shuffle store with shuffle id:" << shuffleId 
			 << " passthrough buffer: " << (void*)passThroughBuffer 
                         << " with buffer capacity: " << buff_capacity 
			 << " reducer id: " << reducerId << " with ordering: " << ordering
                         << " and with aggregation: " << aggregation << endl;

	    break;
          } 

         case KValueTypeId::Object:
          {
     	       //to be implemented.
	       break;
          } 

         case KValueTypeId::Unknown:
          {
	       break;
          } 
    }

    return store;
}


//the buffer manager is passed in, but we have its lifetime to be controlled by the shuffle store as well.
GenericReduceShuffleStore* ReduceShuffleStoreManager::createStore(int shuffleId, 
                  int reducerId,
	          const ReduceStatus &status, int partitions, ExtensibleByteBuffers *bMgr,
	          unsigned char *passThroughBuffer, size_t buff_capacity,
		  enum KValueTypeId tid,
    	          bool ordering, bool aggregation) {
    GenericReduceShuffleStore  *store=nullptr;
    switch (tid) {
        case KValueTypeId::Int: 
          {
	       store = (GenericReduceShuffleStore*)
		 new ReduceShuffleStoreWithIntKey(status, partitions, reducerId, bMgr, 
                      passThroughBuffer, buff_capacity, ordering, aggregation);
               LOG(INFO) << "create int-key reduce shuffle store with shuffle id:" << shuffleId 
			 << " passthrough buffer: " << (void*)passThroughBuffer 
                         << " with buffer capacity: " << buff_capacity 
			 << " reducer id: " << reducerId << " with ordering: " << ordering
                         << " and with aggregation: " << aggregation << endl;

	       break;
	  }
         case KValueTypeId::Long:
          {
	       store = (GenericReduceShuffleStore*)
		 new ReduceShuffleStoreWithLongKey(status, partitions, reducerId, bMgr, 
                      passThroughBuffer, buff_capacity, ordering, aggregation);
               LOG(INFO) << "create long-key reduce shuffle store with shuffle id:" << shuffleId 
			 << " passthrough buffer: " << (void*)passThroughBuffer 
                         << " with buffer capacity: " << buff_capacity 
			 << " reducer id: " << reducerId << " with ordering: " << ordering
                         << " and with aggregation: " << aggregation << endl;

	       break;
          } 

         case KValueTypeId::Float:
          {
     	       //to be implemented.
	       break;
          } 

         case KValueTypeId::Double:
          {
     	       //to be implemented.
	       break;
          } 

         case KValueTypeId::String:
          {
     	       //to be implemented.
	       break;
          } 

         case KValueTypeId::ByteArray:
          {
	    LOG(FATAL) << "byte-array key shuffle store can not be created in  this method";
	    break;
          } 

    case KValueTypeId::Object:
      store =
        new ReduceShuffleStoreWithObjKeys(status, reducerId, passThroughBuffer, buff_capacity, ordering, aggregation);
      break;

         case KValueTypeId::Unknown:
          {
	       break;
          } 
    }

    return store;
}


void ReduceShuffleStoreManager::stopShuffleStore(GenericReduceShuffleStore *store) {
  if (store != nullptr) {
      store->stop();
  }
}

void ReduceShuffleStoreManager::shutdownShuffleStore(GenericReduceShuffleStore *store) {
  if (store != nullptr) {
      store->shutdown();
  }
}

//finally shutdown itself 
void ReduceShuffleStoreManager::shutdown () {
  {
    //do nothing at this time.
  }
}


