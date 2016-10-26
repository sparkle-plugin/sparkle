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
#include "PassThroughReduceChannelWithByteArrayKeys.h"
#include "ShuffleDataSharedMemoryReader.h"
#include "MapShuffleStoreManager.h"
#include "ShuffleStoreManager.h"
#include "ShuffleConstants.h"
#include "ShuffleDataSharedMemoryManager.h"

void PassThroughReduceChannelWithByteArrayKeys::getNextKeyValuePair() {
	 
	unsigned char *oldPtr = currentPtr; 

	ShuffleDataSharedMemoryReader::read_datachunk_value(currentPtr, 
			    (unsigned char*)&currentKeyValueSize, sizeof(currentKeyValueSize));

	VLOG(2) << "retrieved data chunk for map id: " << mapBucket.mapId
		<< " with byte array key value's length: " << currentKeyValueSize;

	currentPtr += sizeof(currentKeyValueSize);

	//(2) read the key value's pointer, which is already in this process's address
        currentKeyPtr = currentPtr;         
        // no caching, as it is a pass-through channel. direct advance to the value
        currentPtr += currentKeyValueSize;
   
	VLOG(2) << "retrieved data chunk key's value at memory address: " << (void*)currentKeyPtr
                << " size: " << currentKeyValueSize;

	//(3) read value's zie, then advance the pointer 
	ShuffleDataSharedMemoryReader::read_datachunk_value(currentPtr,
				    (unsigned char*)&currentValueSize, sizeof(currentValueSize));
        currentPtr += sizeof(currentValueSize);
        //no caching, as it is a pass-through channel. 
        currentValuePtr = currentPtr; 
        
	VLOG(2) << "retrieved data chunk value at memory address: " << (void*) currentValuePtr
	        << " size: " << currentValueSize;

        //direct advance to the next key/value pair
        currentPtr += currentValueSize;

	kvalueCursor++;
	totalBytesScanned += (currentPtr - oldPtr); 
}

//this is the method in which the key/value pair contributed for all of the map channnels 
//get populated into the hash table.
void PassThroughReduceEngineWithByteArrayKeys::init() {
        //to build the hash table for each channel.
	for (auto p = passThroughReduceChannels.begin(); p != passThroughReduceChannels.end(); ++p) {
		p->init();
	}

	//NOTE: should the total number of the channels to be merged is equivalent to total number of partitions?
	//or we only consider the non-zero-sized buckets/channels to be merged? 
        currentChannelIndex =0;
        sizeOfChannels = passThroughReduceChannels.size();
}

/*
 * HashMap based merging, without considering the ordering.
 */
void PassThroughReduceEngineWithByteArrayKeys::getNextKeyValuePair(
                  ByteArrayKeyWithVariableLength::PassThroughMapBuckets& passThroughResultHolder) {
     passThroughReduceChannels[currentChannelIndex].getNextKeyValuePair();
     unsigned char *currentKeyPtr =  passThroughReduceChannels[currentChannelIndex].getCurrentKeyPtr();
     int keyValueSize  =  passThroughReduceChannels[currentChannelIndex].getCurrentKeyValueSize();
     unsigned char* currentValuePtr = passThroughReduceChannels[currentChannelIndex].getCurrentValuePtr();
     int valueSize = passThroughReduceChannels[currentChannelIndex].getCurrentValueSize();
     passThroughResultHolder.addKeyValue(currentKeyPtr, keyValueSize, currentValuePtr, valueSize);
}


void PassThroughReduceEngineWithByteArrayKeys::shutdown() {
  for (auto p = passThroughReduceChannels.begin(); p != passThroughReduceChannels.end(); ++p) {
	p->shutdown();
  }

  passThroughReduceChannels.clear();

  VLOG(2) << "pass-through channels are shutdown, with current size: " 
	  << passThroughReduceChannels.size();
}

