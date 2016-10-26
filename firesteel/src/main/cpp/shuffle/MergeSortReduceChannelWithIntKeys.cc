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
#include "MergeSortReduceChannelWithIntKeys.h"
#include "ShuffleDataSharedMemoryReader.h"
#include "MapShuffleStoreManager.h"
#include "ShuffleStoreManager.h"
#include "ShuffleConstants.h"
#include "ShuffleDataSharedMemoryManager.h"

void MergeSortReduceChannelWithIntKeys::getNextKeyValuePair() {
	 
	unsigned char *oldPtr = currentPtr; 

	ShuffleDataSharedMemoryReader::read_datachunk_intkey(currentPtr, &currentKeyValue, &currentValueSize);

	VLOG(2) << "retrieved data chunk for map id: " << mapBucket.mapId
		<< " with key value: " << currentKeyValue << " and value size: " << currentValueSize;

	currentPtr += sizeof(currentKeyValue)+sizeof(currentValueSize);

	//WARNING: this is not an efficient way to do the work. we will have to use the big buffer to do memory copy,
	//instead of keep doing malloc. we will have to improve this. 
	//currentValueValue = (unsigned char*)malloc(currentValueSize);
	//allocatedValues.push_back(currentValueValue); //to keep track of the memory allocated.
	currentValueValue =
		ShuffleDataSharedMemoryReader::read_datachunk_keyassociated_value(
                   currentPtr, currentValueSize, bufferMgr); 

	VLOG(2) << "retrieved data chunk value: " << " start buffer: " << currentValueValue.start_buffer 
                << " position: " << currentValueValue.position_in_start_buffer
		<< " size: " << currentValueValue.value_size 
                << " with buffer manager internal position: " << bufferMgr->current_buffer().position_in_buffer();

	//after that , you need to move the pointer
	currentPtr += currentValueSize;
	kvalueCursor++;
	totalBytesScanned += (currentPtr - oldPtr); 
}

//one key with one value, to handle the situation: (1) ordering, and (2) no aggregation. 
void MergeSortReduceChannelWithIntKeys::retrieveKeyWithValue(
		     IntKeyWithFixedLength::MergeSortedMapBuckets &mergeResultHolder, 
                     size_t currentKeyTracker) {
    mergeResultHolder.addValueOnKey(currentKeyTracker, currentValueValue);
    if (hasNext()) {
      getNextKeyValuePair();
    }
    else {
      kvalueCursor = -1; //set to an invalidated state
      currentValueSize = -1;//set to an invalidated state
    }

}

//one key with multiple values, to handle the situation: (1) ordering with (2) aggregation
int MergeSortReduceChannelWithIntKeys::retrieveKeyWithMultipleValues(
		     IntKeyWithFixedLength::MergeSortedMapBuckets &mergeResultHolder, 
                     size_t currentKeyTracker) {
	int count = 0; 
	int keyToCompared = currentKeyValue; 
	//push the current value.
        //heldValues.push_back(currentValueValue);
        mergeResultHolder.addValueOnKey(currentKeyTracker, currentValueValue);
	count++; 

	while (hasNext()) {
		getNextKeyValuePair();
		int keyValue = currentKeyValue;
		if (keyValue == keyToCompared) {
			//currentValueValue and currentValueSize already get changed due to 
			//heldValues.push_back(currentValueValue);
  		        mergeResultHolder.addValueOnKey(currentKeyTracker, currentValueValue);
			count++; 
		}
		else {
			break; //done, but the current key alreayd advances to the next different key.  
		}
	}

	return count; 
}

void MergeSortReduceEngineWithIntKeys::init() {
	int channelNumber = 0; 
	for (auto p = mergeSortReduceChannels.begin(); p != mergeSortReduceChannels.end(); ++p) {
		p->init();
		//check, to make sure that we have at least one element for each channel. 
		//for testing purpose, p may have zero elements inside. 
		if (p->hasNext()) {
			//populate the first element from each channel into the priority.
			p->getNextKeyValuePair();
			int firstValue = p->getCurrentKeyValue();
			PriorityQueuedElementWithIntKey  firstElement(channelNumber, firstValue);
			mergeSortPriorityQueue.push(firstElement);
		}

		//still, we are using unique numbers. 
		channelNumber++; 
	}

	//NOTE: should the total number of the channels to be merged is equivalent to total number of partitions?
	//or we only consider the non-zero-sized buckets/channels to be merged? 
}


/*
 * to handle the situation: (1) ordering and (2) no aggregation.
 * for the top() element, find out which channel it belongs to, then after the channel to advance to fill
 * the elements that has the same value as the current top element, then fill the vacant by pushing into 
 * the next key value, if it exists. we return, until the top() return is different from the current value
 */
void MergeSortReduceEngineWithIntKeys::getNextKeyValuePair(
			      IntKeyWithFixedLength::MergeSortedMapBuckets& mergedResultHolder) {
    const PriorityQueuedElementWithIntKey& topElement = mergeSortPriorityQueue.top();
    int channelNumber = topElement.mergeChannelNumber;
    currentMergedKey = topElement.keyValue;

    //add the key first, then the key-associated values from all of the channels.
    //NOTE: after adding key, the key tracker already advance to next value, which is next key position.
    size_t currentKeyTracker=mergedResultHolder.addKey(currentMergedKey);

    mergeSortPriorityQueue.pop();

    MergeSortReduceChannelWithIntKeys &channel = mergeSortReduceChannels[channelNumber];
    channel.retrieveKeyWithValue(mergedResultHolder, currentKeyTracker);

    //if channel is empty, value size is -1.
    if (channel.getCurrentValueSize() != -1) {
        int nextKeyValue = channel.getCurrentKeyValue();
	PriorityQueuedElementWithIntKey replacementElement(channelNumber, nextKeyValue);
	mergeSortPriorityQueue.push(replacementElement);
    }
}

/*
 * to handle the situation: both (1) ordering and (2) aggregation.
 *
 * for the top() element, find out which channel it belongs to, then after the channel to advance to fill
 * the elements that has the same value as the current top element, then fill the vacant by pushing into 
 * the next key value, if it exists. we return, until the top() return is different from the current value
 */
void MergeSortReduceEngineWithIntKeys::getNextKeyValuesPair(
                              IntKeyWithFixedLength::MergeSortedMapBuckets& mergedResultHolder) {
	//clean up the value and value size holder. the values held in the vector will have the occupied memory
	//freed in some other places and other time.
	//currentMergedValues.clear();
	const PriorityQueuedElementWithIntKey& topElement = mergeSortPriorityQueue.top();
	int channelNumber = topElement.mergeChannelNumber;
	currentMergedKey = topElement.keyValue;

	//add the key first, then the key-associated values from all of the channels.
        //NOTE: after adding key, the key tracker already advance to next value, which is next key position.
        size_t currentKeyTracker=mergedResultHolder.addKey(currentMergedKey);

	mergeSortPriorityQueue.pop();

	MergeSortReduceChannelWithIntKeys &channel = mergeSortReduceChannels[channelNumber];
	
	//for this channel, to advance to the next key that is different from the current one;
	//channel.retrieveKeyWithMultipleValues(currentMergedValues);
        channel.retrieveKeyWithMultipleValues(mergedResultHolder, currentKeyTracker);
	//this is after the duplicated keys for this channel. 

	int nextKeyValue = channel.getCurrentKeyValue();
	if (nextKeyValue != currentMergedKey) {
		//because we advance from the last retrieved duplicated key
		PriorityQueuedElementWithIntKey replacementElement(channelNumber, nextKeyValue);
		mergeSortPriorityQueue.push(replacementElement);
	}
	//else: the channel is exhaused. 

	//keep moving to the other channels, until current merged key is different
 
	while (!mergeSortPriorityQueue.empty()) {
		PriorityQueuedElementWithIntKey nextTopElement = mergeSortPriorityQueue.top();
		int nextKeyValue = nextTopElement.keyValue;
		
		int other_channelnumber = nextTopElement.mergeChannelNumber;

		//this is a duplicated key, from the other channel. we will do the merge.
		if (nextKeyValue == currentMergedKey)  {
			CHECK_NE(other_channelnumber, channelNumber);

			mergeSortPriorityQueue.pop();
			 
			MergeSortReduceChannelWithIntKeys &other_channel = mergeSortReduceChannels[other_channelnumber];

			//for this channel, to retrieve the next key(s) that are identical to the current key,until exhausted.
			//the current key tracker is still the same.
			other_channel.retrieveKeyWithMultipleValues(mergedResultHolder, currentKeyTracker);
			//pick the next one from this other channel, or we already exhausted, then the key will stay the same 
			int other_nextKeyValue = other_channel.getCurrentKeyValue();
			if (other_nextKeyValue != currentMergedKey){
			        //we still have next different key to be pushed into the merge-sort network.
				PriorityQueuedElementWithIntKey other_replacementElement(other_channelnumber, other_nextKeyValue);
				//the other_channel gets popped, so we need to push the corresponding replacement element, 
                                //if it is available. 
				mergeSortPriorityQueue.push(other_replacementElement);

			}
			//else, this channel is also exhausted. 
		}
		else {
			//the top of the queue has different key value different from the current one
                        //that is no more duplicated queue. 
			break; 
		}
	}
}
