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

#ifndef MAPSHUFFLESTORE_WITH_BYTEARRAYKEYS_H_
#define MAPSHUFFLESTORE_WITH_BYTEARRAYKEYS_H_

#include <vector>
#include "ExtensibleByteBuffers.h"
#include "ShuffleConstants.h"
#include "MapStatus.h"
#include "EnumKvTypes.h"
#include "GenericMapShuffleStore.h"
#include <byteswap.h> //for byte swapping on normalized key
using namespace std;


//void *memcpy(void *dest, const void *src, size_t n);
struct ByteArrayKeyWithValueTracker {
  //use 8-byte normalized key for fast comparision, before switching to regular 
  //byte-by-byte comparision. 
  unsigned long normalizedKey; 
  int partition;
  //the key size is encoded in the  key_tracker also.
  PositionInExtensibleByteBuffer key_tracker; //in the  key buffer tracker.
  PositionInExtensibleByteBuffer value_tracker; //in the value buffer tracker.
  //for future reference and data retrieval.
  //ExtensibleByteBuffers *kBufferMgr;
  //ExtensibleByteBuffers *vBufferMgr;

  ByteArrayKeyWithValueTracker(int p, unsigned char *fullKey, int keysize, 
	  const PositionInExtensibleByteBuffer &ktracker,
	  const PositionInExtensibleByteBuffer &vtracker) :
          normalizedKey(0),
	  partition(p), key_tracker(ktracker), value_tracker(vtracker) {
          //construct normalized key
	  if (keysize < SHMShuffleGlobalConstants::NORMALIZED_KEY_SIZE) { 
	      memcpy (&normalizedKey, fullKey, keysize);
	  }
          else {
	      memcpy (&normalizedKey, fullKey, SHMShuffleGlobalConstants::NORMALIZED_KEY_SIZE);
	  }

          //our machine is little-endian, but the byte[] comparator, the first byte is most 
          //signficant, so we will have to do the byte-swapping and then use unsigned long for comparision
          normalizedKey=bswap_64(normalizedKey);
  }
};


class  MapShuffleStoreWithByteArrayKey: public GenericMapShuffleStore {

 private:
     ByteArrayKeyWithValueTracker *keysandvals;
     //total number of key and values received so far 
     size_t sizeTracker;
     //to keep track the current key buffer capacity, and decide whether we need to re-allocate the
     //buffer or not.
     size_t currentKVBufferCapacity;
     
     //total number of Partitions, need to write it out to the reducer side
     int totalNumberOfPartitions; 
     //the index chunk offset, that controls the entire shared-memory region's writing 
     //it is assigned when sorted data is written to shared-memory region. It has two versions.
     //version 1: the local pointer version of index chunk offset 
     unsigned char *indChunkOffset = nullptr;
     //version 2: the corresponding global pointer version of index chunk offset.
     //so that we can get back to the global pointer immediately. 
     uint64_t globalPointerIndChunkRegionId = -1;
     uint64_t globalPointerIndChunkOffset =-1;

     //one buffer manager for key value, and one buffer manager for value value.
     ExtensibleByteBuffers kBufferMgr;
     ExtensibleByteBuffers vBufferMgr;
     int mapId ;     //mapper id

     //for key value type definition
     KValueTypeDefinition kvTypeDefinition;

     //value type defintion 
     VValueTypeDefinition vvTypeDefinition; 

     //to identify whether we need to have sorting or not for key ordering.
     bool orderingRequired;

 public: 

     //designed for testing purpose
     MapShuffleStoreWithByteArrayKey (int buffsize, int mapId, bool ordering); 

     //we need to have some upmost layer to holder a pointer, and then 
     //the buffer manager is a stack variable inside.
     MapShuffleStoreWithByteArrayKey (int mapId, bool ordering); 


     //Item 7: declare a virtual destructor if and only if the class contains at least one virtual function
     virtual ~MapShuffleStoreWithByteArrayKey () {
       //do nothing
     }

     ExtensibleByteBuffers &getKBufferMgr() {
       return kBufferMgr;
     }

     ExtensibleByteBuffers &getVBufferMgr() {
       return vBufferMgr;
     }

     int getMapId() {
       return mapId; 
     }

     //for testing purpose.
     ByteArrayKeyWithValueTracker* getKeysAndValues() {
          return keysandvals;
     }

     //introduce kvalue length array to also control the length of string-based k value, without
     //having the "\0" terminator.
     void storeKVPairsWithByteArrayKeys (unsigned char *byteHolder, int koffsets[], int voffsets[], 
	     int partitions[], int numberOfPairs);


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
     void sort(int partitions, bool ordering);

     //write the sorted shuffle data to the NVM region.
     MapStatus writeShuffleData();

     //to clean up the resource occupied at the key related part
     void stop() override;

     //the shared memory region will need to be cleaned up by buffer manager.
     void shutdown() override;
};

#endif /*MAPSHUFFLESTORE_WITH_BYTEARRAYKEYS_H_*/
