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

#ifndef REDUCESHUFFLESTOREMANAGER_H_
#define REDUCESHUFFLESTOREMANAGER_H_

#include "EnumKvTypes.h"
#include "MapStatus.h"
#include "ExtensibleByteBuffers.h"
#include "GenericReduceShuffleStore.h"

using namespace std;

//NOTE: this becomes a factory class. And all of the created reduce shuffle stores are maintained 
//at the Java side (if necessary)
class ReduceShuffleStoreManager {
 public: 
    
    ReduceShuffleStoreManager() {
       //
    }

    //Item 7: declare a virtual destructor if and only if the class contains at least one virtual function
    ~ReduceShuffleStoreManager() {
       //
    }

    //to get int a merge-sort channel from one map bucket, and then retrieve the Key type definition
    //and Value type defintion, so that we know which shuffle store to create. 
    void obtain_kv_definition (MapBucket &mapBucket, int rId, int rPartitions,
                               KValueTypeDefinition &kd, VValueTypeDefinition &vd);    
    
    //the buffer manager will have to be created before the redue shuffle store. and thus the life time of 
    //the buffer manager will need to be seperately controlled.
    //we have buffer manager, key buffer manager, and value buffer manager. some reduce shuffle store use
    //two buffer managers, and some only use the value buffer manager.
    //(1) only one buffer manager, which is the value buffer manager.
    GenericReduceShuffleStore *createStore(int shuffleId, int id, 
			   const ReduceStatus &status, int partitions, ExtensibleByteBuffers *bMgr,
		           unsigned char *passThroughBuffer, size_t buff_capacity,
		           enum KValueTypeId tid, bool ordering, bool aggregation);
    //(2) two buffer managers are used.
    GenericReduceShuffleStore *createStore(int shuffleId, int id, 
			   const ReduceStatus &status, int partitions, 
                           ExtensibleByteBuffers *kBMgr, ExtensibleByteBuffers *vBMgr, 
		           unsigned char *passThroughBuffer, size_t buff_capacity,
		           enum KValueTypeId tid, bool ordering, bool aggregation);

    //free map shuffle store's DRAM resource
    void stopShuffleStore (GenericReduceShuffleStore *store); 
    
    //free map shuffle store's  NVRAM resource, and remove the entry from the executor.
    void shutdownShuffleStore (GenericReduceShuffleStore *store); 

    //initialize this MapShufflstore 
    void initialize () {
      //for example, we will need to take care of the assigned share-memory region     
    }

    //NOTE: this will be issued from Java, from one store to the other, as the 
    //concurent hash map is maintained at Java side.
    //for each shuffle stage, cleanup the corresponding NVM related resources  
    //void cleanup (int shuffleId);

    void shutdown ();
};


#endif 
