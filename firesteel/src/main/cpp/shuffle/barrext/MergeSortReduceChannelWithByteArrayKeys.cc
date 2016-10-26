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
#include "MergeSortReduceChannelWithByteArrayKeys.h"
#include "ShuffleDataSharedMemoryReader.h"
#include "MapShuffleStoreManager.h"
#include "ShuffleStoreManager.h"
#include "ShuffleConstants.h"
#include "ShuffleDataSharedMemoryManager.h"
#include "VariableLengthKeyComparator.h"

void MergeSortReduceChannelWithByteArrayKeys::getNextKeyValuePair() {
	unsigned char *oldPtr = currentPtr; 

        //(1) read byte array key's size, then advance the pointer;
	ShuffleDataSharedMemoryReader::read_datachunk_value(currentPtr, 
		    (unsigned char*) &currentKeyValueSize, sizeof(currentKeyValueSize));

	VLOG(2) << "retrieved data chunk for map id: " << mapBucket.mapId
		<< " with byte array key value's length: " << currentKeyValueSize;
	currentPtr += sizeof(currentKeyValueSize);

        //(2) read the key value's pointer, which is already in this process's address.
        currentKeyPtr =currentPtr;
        //(2) also cache in the local KEY buffer manager 
        currentKeyValue =
	       ShuffleDataSharedMemoryReader::read_datachunk_keyassociated_value(
				    currentPtr, currentKeyValueSize, kBufferMgr);

	currentPtr += currentKeyValueSize;

	VLOG(2) << "retrieved data chunk key's value at memory address: " << (void*)currentKeyPtr
		<< " size: " << currentKeyValueSize;

        //(3) read value's size, then advance the pointer
	ShuffleDataSharedMemoryReader::read_datachunk_value(currentPtr,
					    (unsigned char*)&currentValueSize, sizeof(currentValueSize));
        currentPtr += sizeof(currentValueSize); 

	//(3) assign the pointer to the value 
        currentValuePtr = currentPtr; 

        //(4) read value's value into local VALUE buffer manager
        //currentValueValue = 
	//  ShuffleDataSharedMemoryReader::read_datachunk_keyassociated_value(
	//				    currentPtr, currentValueSize, vBufferMgr);
        //NOTE: by-pass value reading from shared-memory channel, only keep the value size information
        currentValueValue =  PositionInExtensibleByteBuffer(-1, -1, currentValueSize);        

	VLOG(2) << "retrieved data chunk value at memory address: " << (void*) currentPtr
              	<< " size: " << currentValueSize;

	//after that, move the pointer right after the value.
	currentPtr += currentValueSize;

	kvalueCursor++;
	totalBytesScanned += (currentPtr - oldPtr); 
}

//one key with one value, to handle the situation: (1) ordering, and (2) no aggregation.
void MergeSortReduceChannelWithByteArrayKeys::retrieveKeyWithValue(
              ByteArrayKeyWithVariableLength::MergeSortedMapBuckets  &mergeResultHolder, 
              size_t currentKeyTracker){
    ByteArrayKeyWithVariableLength::MergeSortedValueTracker
                     pValueToAdd (currentValuePtr, currentValueSize, currentValueValue);
    mergeResultHolder.addValueOnKey(currentKeyTracker, pValueToAdd);

    if (hasNext()) {
       getNextKeyValuePair();
    }
    else {
      currentKeyPtr = nullptr;
      currentValuePtr = nullptr;
    }
}


//one key with multiple values, to handle the situation: (1) ordering with (2) aggregation.
int MergeSortReduceChannelWithByteArrayKeys::retrieveKeyWithMultipleValues(
              ByteArrayKeyWithVariableLength::MergeSortedMapBuckets  &mergeResultHolder, 
              size_t currentKeyTracker){
    int count = 0;
    unsigned char *keyPtrToCompared = currentKeyPtr;
    int keyPtrToComparedSize = currentKeyValueSize; 
    //push the current key 
    //heldValues.push_back(currentValueValue);
    ByteArrayKeyWithVariableLength::MergeSortedValueTracker
                     pValueToAdd (currentValuePtr, currentValueSize, currentValueValue);
    mergeResultHolder.addValueOnKey(currentKeyTracker, pValueToAdd);
    count++;
   
    while (hasNext()) {
       getNextKeyValuePair();
       if (VariableLengthKeyComparator::is_equal(
		 keyPtrToCompared, keyPtrToComparedSize, currentKeyPtr, currentKeyValueSize)) {
	 ByteArrayKeyWithVariableLength::MergeSortedValueTracker 
                           vMoreValueToAdd (currentValuePtr, currentValueSize, currentValueValue);
         mergeResultHolder.addValueOnKey(currentKeyTracker, vMoreValueToAdd);
         count++;
       }
       else {
	 break; //done, but the current key alreayd advances to the next different key.
       }
    }

    return count;
}


void MergeSortReduceEngineWithByteArrayKeys::init() {
	int channelNumber = 0; 
	for (auto p = mergeSortReduceChannels.begin(); p != mergeSortReduceChannels.end(); ++p) {
		p->init();
		//check, to make sure that we have at least one element for each channel. 
		//for testing purpose, p may have zero elements inside. 
		if (p->hasNext()) {
			//populate the first element from each channel into the priority.
			p->getNextKeyValuePair();
			PositionInExtensibleByteBuffer firstValue = p->getCurrentKeyValue();
                        unsigned char *firstkValuePtr= p->getKValuePtr();
                        int firstKValSize = p->getCurrentKeyValueSize();

			PriorityQueuedElementWithVariableLengthKey
			   firstElement(channelNumber, firstValue, firstkValuePtr, firstKValSize, kBufferMgr);

			mergeSortPriorityQueue.push(firstElement);
		}

		//still, we are using unique numbers. 
		channelNumber++; 
	}

	//NOTE: should the total number of the channels to be merged is equivalent to total number of partitions?
	//or we only consider the non-zero-sized buckets/channels to be merged? 
}

/*
 *to handle the situation: (1) ordering, and (2) no aggregation.
 *
 *for the top() element, find out which channel it belongs to, then after the channel to advance to 
 *fill the elements that has the same value as the current top element, then fill the vacant by pushing
 * into the next key value, if it exists. 
 * we return, until the top() return is different from the current value
 */
void MergeSortReduceEngineWithByteArrayKeys::getNextKeyValuePair(
                        ByteArrayKeyWithVariableLength::MergeSortedMapBuckets& mergedResultHolder) {
   const PriorityQueuedElementWithVariableLengthKey& topElement = mergeSortPriorityQueue.top();
   int channelNumber = topElement.mergeChannelNumber;
   currentMergedKeyValue = topElement.fullKeyValue;
   const unsigned char *currentKValuePtr = topElement.kValuePtr;
   int currentKValueSize= topElement.kValueSize;
   //add the key first, then the key-associated values from all of the channels.
   //after adding key, the key tracker already advances to next value, which is next key position.
   size_t currentKeyTracker=
              mergedResultHolder.addKey(currentKValuePtr, currentKValueSize, currentMergedKeyValue);

   mergeSortPriorityQueue.pop();

   MergeSortReduceChannelWithByteArrayKeys &channel = mergeSortReduceChannels[channelNumber];
	
   //for this channel, to advance to the next key that is different from the current one;
   //channel.retrieveKeyWithMultipleValues(currentMergedValues, currentMergeValueSizes);
   channel.retrieveKeyWithValue(mergedResultHolder, currentKeyTracker);
   //this is after the duplicated keys for this channel. 

   if (channel.getKValuePtr() != nullptr) {
     PositionInExtensibleByteBuffer  nextKeyValue = channel.getCurrentKeyValue();
     unsigned char *nextKeyValuePtr = channel.getKValuePtr();
     int nextKeyValSize = channel.getCurrentKeyValueSize();
     {
	//because we advance from the last retrieved duplicated key
       PriorityQueuedElementWithVariableLengthKey 
	 replacementElement(channelNumber, nextKeyValue, nextKeyValuePtr, nextKeyValSize, kBufferMgr);
       mergeSortPriorityQueue.push(replacementElement);
     }
   }
   //else: the channel is exhaused. 
	
}


/*
 * to handle the situation: both (1) ordering and (2) aggregation.
 *
 *for the top() element, find out which channel it belongs to, then after the channel to advance to 
 *fill the elements that has the same value as the current top element, then fill the vacant by pushing
 * into the next key value, if it exists. 
 * we return, until the top() return is different from the current value
 */
void MergeSortReduceEngineWithByteArrayKeys::getNextKeyValuesPair(
                           ByteArrayKeyWithVariableLength::MergeSortedMapBuckets& mergedResultHolder) {
   //clean up the value and value size holder. the values held in the vector will have the occupied memory
   //freed in some other places and other time.
   //currentMergedValues.clear();

   const PriorityQueuedElementWithVariableLengthKey& topElement = mergeSortPriorityQueue.top();
   int channelNumber = topElement.mergeChannelNumber;
   currentMergedKeyValue = topElement.fullKeyValue;
   const unsigned char *currentKValuePtr = topElement.kValuePtr;
   int currentKValueSize= topElement.kValueSize;
   //add the key first, then the key-associated values from all of the channels.
   //after adding key, the key tracker already advances to next value, which is next key position.
   size_t currentKeyTracker=
              mergedResultHolder.addKey(currentKValuePtr, currentKValueSize, currentMergedKeyValue);

   mergeSortPriorityQueue.pop();

   MergeSortReduceChannelWithByteArrayKeys &channel = mergeSortReduceChannels[channelNumber];
	
   //for this channel, to advance to the next key that is different from the current one;
   //channel.retrieveKeyWithMultipleValues(currentMergedValues, currentMergeValueSizes);
   channel.retrieveKeyWithMultipleValues(mergedResultHolder, currentKeyTracker);
   //this is after the duplicated keys for this channel. 

   PositionInExtensibleByteBuffer  nextKeyValue = channel.getCurrentKeyValue();
   unsigned char *nextKeyValuePtr = channel.getKValuePtr();
   int nextKeyValSize = channel.getCurrentKeyValueSize();
   if (! VariableLengthKeyComparator::is_equal(
                   nextKeyValuePtr, nextKeyValSize, currentKValuePtr, currentKValueSize) ){
	//because we advance from the last retrieved duplicated key
       PriorityQueuedElementWithVariableLengthKey 
	 replacementElement(channelNumber, nextKeyValue, nextKeyValuePtr, nextKeyValSize, kBufferMgr);
       mergeSortPriorityQueue.push(replacementElement);
   }
   //else: the channel is exhaused. 

   //keep moving to the other channels, until current merged key is different
 
   while (!mergeSortPriorityQueue.empty()) {
	PriorityQueuedElementWithVariableLengthKey nextTopElement = mergeSortPriorityQueue.top();
        const unsigned char *nextKeyValuePtr = nextTopElement.kValuePtr;
        int nextKeyValueSize = nextTopElement.kValueSize;
	
	int other_channelnumber = nextTopElement.mergeChannelNumber;

	//this is a duplicated key, from the other channel. we will do the merge.
	if (VariableLengthKeyComparator::is_equal(
				  nextKeyValuePtr, nextKeyValueSize, currentKValuePtr, currentKValueSize)) {
	  CHECK_NE(other_channelnumber, channelNumber);
   	 
          mergeSortPriorityQueue.pop();
			 
	  MergeSortReduceChannelWithByteArrayKeys &other_channel = mergeSortReduceChannels[other_channelnumber];

	  //for this channel, to advance to the next key that is different from the current one;
	  other_channel.retrieveKeyWithMultipleValues(mergedResultHolder, currentKeyTracker);
	  //pick the next one from this other channel, or we already exhausted. 
	  PositionInExtensibleByteBuffer other_nextKeyValue = other_channel.getCurrentKeyValue();
          const unsigned char *other_nextKeyValuePtr = other_channel.getKValuePtr();
          int other_nextKeyValueSize = other_channel.getCurrentKeyValueSize();
	  if (!VariableLengthKeyComparator::is_equal(
	        other_nextKeyValuePtr, other_nextKeyValueSize, currentKValuePtr, currentKValueSize)){
		PriorityQueuedElementWithVariableLengthKey 
		  other_replacementElement(other_channelnumber, other_nextKeyValue, other_nextKeyValuePtr, 
					   other_nextKeyValueSize, kBufferMgr);
		// the other_channel gets popped, so we need to push the corresponding replacement element,
                // if it is available. 
		mergeSortPriorityQueue.push(other_replacementElement);
	  }
	  //else, this channel is also exhausted. 
	}
	else {
	   //the top of the queue has different key value different from the current one, 
           //that is no more duplicated queue. 
	   break; 
	}
   }
	
}
