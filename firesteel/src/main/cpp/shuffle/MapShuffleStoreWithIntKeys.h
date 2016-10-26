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

#ifndef MAPSHUFFLESTORE_WITH_INT_KEYS_H_
#define MAPSHUFFLESTORE_WITH_INT_KEYS_H_

#include <vector>
#include "ExtensibleByteBuffers.h"
#include "ShuffleConstants.h"
#include "MapStatus.h"
#include "EnumKvTypes.h"
#include "GenericMapShuffleStore.h"

using namespace std;

struct IntKeyWithValueTracker {
  int key;
  int partition;
  PositionInExtensibleByteBuffer value_tracker;

  IntKeyWithValueTracker (int k, int p,  const PositionInExtensibleByteBuffer& tracker) 
    : key (k),  partition (p), value_tracker(tracker) {
 
  };
};

class  MapShuffleStoreWithIntKey: public GenericMapShuffleStore {
 private:
     //change from vector to become dynamic array
     //vector <IntKeyWithValueTracker> keys;
     IntKeyWithValueTracker *keys;    
     //total number of keys received so far 
     size_t sizeTracker;
     //to keep track the current key buffer capacity, and decide whether we need to re-allocate the 
     //buffer or not.
     size_t currentKeyBufferCapacity; 
     //total number partitions
     int totalNumberOfPartitions; 
     //the index chunk offset, that controls the entire shared-memory region's writing
     //it is assigned when sorted data is written to shared-memory region. It has two versions.
     //version 1: the local pointer version of index chunk offset
     unsigned char *indChunkOffset = nullptr;
     //version 2: the corresponding global pointer version of index chunk offset.
     //so that we can get back to the global pointer immediately.
     uint64_t globalPointerIndChunkRegionId = -1;
     uint64_t globalPointerIndChunkOffset =-1; 

     ExtensibleByteBuffers bufferMgr;
     int mapId; //the mapper id.
     
     //for key value type definition 
     KValueTypeDefinition kvTypeDefinition;
     //value type definition
     VValueTypeDefinition vvTypeDefinition; 

     //to identify whether we need to have sorting or not for key ordering.
     bool orderingRequired;

 public: 

     //please refer to C++ 11 reference book page. 194
     //design for testing purpose
     MapShuffleStoreWithIntKey (int bufsize, int mId, bool ordering);
     //please refer to C++ 11 reference book page. 194                          
     MapShuffleStoreWithIntKey(int mId, bool ordering);

     //Item 7: declare a virtual destructor if and only if the class contains at least one virtual function
     virtual ~MapShuffleStoreWithIntKey () {
       //do nothing.keys array will be free from the call of "stop"
     }
	 
     //for testing purpose
     ExtensibleByteBuffers & getBufferMgr() {
	 return bufferMgr;
     }

     int getMapId() {
	 return mapId;
     }

     //for testing purpose.
     IntKeyWithValueTracker* getKeys() const {
       return keys;
     }
    
     //for testing purpose 
     size_t getSizeTracker() const {
        return sizeTracker;
     } 

     void storeKVPairsWithIntKeys (unsigned char *byteHolder, int voffsets[], 
	   int kvalues[], int partitions[], int numberOfPairs) ;

     void setValueType(unsigned char *vtdef, int size) {
         vvTypeDefinition.length = size;
         vvTypeDefinition.definition = (unsigned char*) malloc (size);
         memcpy(vvTypeDefinition.definition, vtdef,size);
     }

     //pass back the whole value, including pointer copy. 
     VValueTypeDefinition getVValueType() override {
       return vvTypeDefinition;
     }

     //get back the key type definition
     KValueTypeDefinition getKValueType() override {
        return kvTypeDefinition;
     }

     //to retrieve whether the map requires ordering or not
     bool needsOrdering() override {
       return orderingRequired;     
     }

     //sort for the full key. we may later use multiple runs of sort.
     //totalNumberOfPartitions will be set in this call.
     void sort(int partitions, bool ordering);

     //write the sorted shuffle data to the NVM region
     MapStatus writeShuffleData(); 

     //to clean up the resource occupied at the key related part
     void stop() override;
     //the shared memory region will need to be cleaned up by buffer manager.
     void shutdown() override;
    

};

#endif 
