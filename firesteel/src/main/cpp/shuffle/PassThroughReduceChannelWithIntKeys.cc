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
#include "PassThroughReduceChannelWithIntKeys.h"
#include "ShuffleDataSharedMemoryReader.h"
#include "MapShuffleStoreManager.h"
#include "ShuffleStoreManager.h"
#include "ShuffleConstants.h"
#include "ShuffleDataSharedMemoryManager.h"

void PassThroughReduceChannelWithIntKeys::getNextKeyValuePair() {
    unsigned char *oldPtr = currentPtr;

    ShuffleDataSharedMemoryReader::read_datachunk_intkey(currentPtr, &currentKeyValue, &currentValueSize);

    VLOG(2) << "retrieved data chunk for map id: " << mapBucket.mapId
            << " with key value: " << currentKeyValue << " and value size: " << currentValueSize;

    currentPtr += sizeof(currentKeyValue)+sizeof(currentValueSize);

    //WARNING: this is not an efficient way to do the work. we will have to use the big buffer to do memory copy,
    //instead of keep doing malloc. we will have to improve this.
    //currentValueValue = (unsigned char*)malloc(currentValueSize);
    //allocatedValues.push_back(currentValueValue); //to keep track of the memory allocated.
    currentValueValue =currentPtr;
    //direct copy to the pass-through buffer.
    //delegate the buffer copy until the buffer is copied to the pass-through keyvalue tracker.
    //ShuffleDataSharedMemoryReader::read_datachunk_keyassociated_value(
    //           currentPtr, currentValueSize, passThroughBuffer);

    //after that , you need to move the pointer
    currentPtr += currentValueSize;
    kvalueCursor++;
    totalBytesScanned += (currentPtr - oldPtr);
}

//this is the method in which the key/value pair contributed for all of the map channnels 
//get populated into the hash table.
void PassThroughReduceEngineWithIntKeys::init() {
    //to build the hash table for each channel.
    for (auto p = passThroughReduceChannels.begin(); p != passThroughReduceChannels.end(); ++p) {
        p->init();
    }

    //NOTE: should the total number of the channels to be merged is equivalent to total number of partitions?
    //or we only consider the non-zero-sized buckets/channels to be merged?
    currentChannelIndex =0;
    sizeOfChannels = passThroughReduceChannels.size();

    if (sizeOfChannels <= 0) {
      return;
    }

    if (passThroughReduceChannels[0].isSameNode()) {
        localBucketsRead++;
    } else {
        remoteBucketsRead++;
    }
}

/*
 * HashMap based merging, without considering the ordering.
 */
void PassThroughReduceEngineWithIntKeys::getNextKeyValuePair(
                      IntKeyWithFixedLength::PassThroughMapBuckets& passThroughResultHolder) {
    passThroughReduceChannels[currentChannelIndex].getNextKeyValuePair();
    currentKey =  passThroughReduceChannels[currentChannelIndex].getCurrentKeyValue();
    unsigned char* currentValue = passThroughReduceChannels[currentChannelIndex].getCurrentValueValue();
    int valSize = passThroughReduceChannels[currentChannelIndex].getCurrentValueSize();
    passThroughResultHolder.addKeyValue(currentKey, currentValue, valSize);

    // collect shuffle metrics.
    if (passThroughReduceChannels[currentChannelIndex].isSameNode()) {
        passThroughResultHolder.bytesLocalBucketsRead +=
            sizeof(int) * 2 + valSize; // key + valSize + val.
    } else {
        passThroughResultHolder.bytesRemoteBucketsRead +=
            sizeof(int) * 2 + valSize;
    }
}


void PassThroughReduceEngineWithIntKeys::shutdown() {
    for (auto p = passThroughReduceChannels.begin(); p != passThroughReduceChannels.end(); ++p) {
        p->shutdown();
    }

    passThroughReduceChannels.clear();
    if (VLOG_IS_ON(2)) {
        VLOG(2) << "pass-through channels are shutdown, with current size: "
                << passThroughReduceChannels.size() <<  endl;
    }
}
