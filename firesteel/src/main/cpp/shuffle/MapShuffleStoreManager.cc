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
#include <iostream>

#include "EnumKvTypes.h"
#include "MapShuffleStoreManager.h"
#include "MapShuffleStoreWithStringKeys.h"
#include "MapShuffleStoreWithIntKeys.h"
#include "MapShuffleStoreWithLongKeys.h"
#include "MapShuffleStoreWithByteArrayKeys.h"


GenericMapShuffleStore* MapShuffleStoreManager::createStore(int shuffleId, int id, 
                                           enum KValueTypeId tid, bool ordering) {
   GenericMapShuffleStore *store = nullptr;
   switch (tid) {
          case KValueTypeId::Int: 
          {
  	    store = (GenericMapShuffleStore*)(new MapShuffleStoreWithIntKey(id, ordering));
            LOG(INFO) << "create int-key map shuffle store with shuffle id:" << shuffleId 
		      << " map id: " << id << " and ordering: " << ordering << endl;
            break;
          }

          case KValueTypeId::Long: {
 	    store = (GenericMapShuffleStore*)(new MapShuffleStoreWithLongKey(id, ordering));
            LOG(INFO) << "create long-key map shuffle store with shuffle id:" << shuffleId 
		      << " map id: " << id << " and ordering: " << ordering << endl;
            break;
          }

          case KValueTypeId::Float: {
 	    LOG(ERROR)  << "Not Implemented Yet" <<endl;
            break;
          }

          case KValueTypeId::Double: {
 	    LOG(ERROR)  << "Not Implemented Yet" <<endl;
            break;
          }

          case KValueTypeId::String:
          {
            store =(GenericMapShuffleStore*)( new MapShuffleStoreWithStringKey(ordering));
            LOG(INFO) << "create string-key map shuffle store with shuffle id:" << shuffleId 
		      << " map id: " << id << " and ordering: " << ordering << endl;
             //need to be done
            break;
          }

          case KValueTypeId::ByteArray:
          {
            store =(GenericMapShuffleStore*)( new MapShuffleStoreWithByteArrayKey(id, ordering));
            LOG(INFO) << "create bytearray-key map shuffle store with shuffle id:" << shuffleId 
		      << " map id: " << id << " and ordering: " << ordering << endl;
             //need to be done
            break;
          }
          
          case KValueTypeId::Object: {
 	    LOG(ERROR)  << "Not Implemented Yet" <<endl;
            break;
          }    

          case KValueTypeId::Unknown: {
 	    LOG(ERROR)  << "Not Implemented Yet" <<endl;
            break;
          }    
     }

     return store;        
}


void MapShuffleStoreManager::stopShuffleStore (GenericMapShuffleStore *store){
  store->stop();
} 

void MapShuffleStoreManager::shutdownShuffleStore (GenericMapShuffleStore *store){
  store->shutdown();
} 

void MapShuffleStoreManager::initialize() {
   //for example, we will need to take care of the assigned share-memory region     
   LOG(INFO) << "map shuffle store manage initialize";

}

//finally shutdown itself.
//NOTE: we may need to do clean up for all shuffle store. The concern is that as the executor
//goes away, all of the DRAM related resources will automatically disappear.
void MapShuffleStoreManager::shutdown () {
   LOG(INFO) << "map shuffle store manage is shutdown";
}


