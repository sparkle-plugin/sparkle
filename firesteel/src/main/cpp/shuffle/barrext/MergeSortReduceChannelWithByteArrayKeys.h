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

#ifndef  _MERGE_SORT_REDUCE_CHANNEL_WITH_BYTEARRAY_KEY_H_
#define  _MERGE_SORT_REDUCE_CHANNEL_WITH_BYTEARRAY_KEY_H_

#include "ShuffleConstants.h"
#include "GenericReduceChannel.h"
#include "ExtensibleByteBuffers.h"
#include "MergeSortKeyPositionTrackerWithByteArrayKeys.h" 

#include "PriorityQueueVariableLengthKeyComparator.h"

#include <vector>
#include <queue> 

using namespace std; 

/*
 *The wrapper on each MapBucket to keep track of the current cursor  and current value 
 *
*/
class  MergeSortReduceChannelWithByteArrayKeys: public GenericReduceChannel{
private:   
        int kvalueCursor; 
        PositionInExtensibleByteBuffer currentKeyValue; 
        unsigned char *currentKeyPtr; //instead of using the key value inside currentKeyValue
        int currentKeyValueSize;  //instead of using the length inside currentKeyValue.

        //NOTE: I should be able to do the same to not to mem-copy the full value. instead, only copy the pointer 
        //which is mapped to the virtual address space of this process already.
        PositionInExtensibleByteBuffer currentValueValue; //use the value tracker in the buffer manager.
        unsigned char *currentValuePtr; //use the pointer in the memory-mapped virtual address space.
        int currentValueSize; //instead of using the length inside currentValueValue.

	//buffer managers as passed in.
	ExtensibleByteBuffers *kBufferMgr;  //key buffer manager 
	ExtensibleByteBuffers *vBufferMgr;  //value buffer manager 

public: 
	
	MergeSortReduceChannelWithByteArrayKeys (MapBucket &sourceBucket, int rId, int rPartitions,
						 ExtensibleByteBuffers* kBufMgr, ExtensibleByteBuffers *vBufMgr) :
                GenericReduceChannel(sourceBucket,rId, rPartitions),
                kvalueCursor(0),
		currentKeyValue(-1, -1, 0),
	        currentKeyPtr (nullptr),
	        currentKeyValueSize(0),
	        currentValueValue(-1, -1, 0),
	        currentValuePtr (nullptr),
 	        currentValueSize(0),
		kBufferMgr (kBufMgr), 
    	        vBufferMgr (vBufMgr){
		  //NOTE: total length passed from the reducer's mapbucket is just an approximation,
                  //as when map status passed to the scheduler, bucket size compression happens. 
                  //this total length gets recoverred precisely, at the reduce channel init () call.
	}

	~MergeSortReduceChannelWithByteArrayKeys() {
		//do nothing. 
	}

	/*
	 * to decide whether this channel is finished the scanning or not. 
	 */ 
	bool hasNext() {
		return (totalBytesScanned < totalLength); 
	}

	void getNextKeyValuePair(); 

        /*
         * return the current direct pointer of the key
         */
        unsigned char *getKValuePtr() {
	  return currentKeyPtr;
	}

        /*
         * return the size of the current key.
         */
        int  getCurrentKeyValueSize() {
	  return currentKeyValueSize;
	}

	/*
	 * return the current key's value
	 */
	PositionInExtensibleByteBuffer getCurrentKeyValue() {
		return currentKeyValue; 
	}
	
	/*
	* return the current value corresponding to the current key. 
	*/
	PositionInExtensibleByteBuffer getCurrentValueValue() {
		return currentValueValue; 
	}

	/*
	* return the pointer to the current value.
	*/
        unsigned char *getCurrentValuePtr() {
	    return (currentValuePtr); 
	}
	/*
	* return the current value's size. 
	*/
	int getCurrentValueSize() {
		return currentValueSize; 
	}

	/*
         *this is to handle the situation: (1) ordering, and (2) aggregation are both required. 
         *
	 *to popuate the passed-in holder by retrieving the Values and the corresonding sizes of
         *the Values,based on the provided key at the current position. Each time a value is retrieved, 
         *the cursor will move to the next different key value. 

	 *Note that duplicated keys can exist for a given Key value.  The occupied heap memory will
         *be released later when the full batch of the key and multiple values are done. 

	 *return the total number of the values identified that has the key  equivalent to the current key. 
	 */
        int retrieveKeyWithMultipleValues(
              ByteArrayKeyWithVariableLength::MergeSortedMapBuckets  &mergeResultHolder, 
              size_t currentKeyTracker);

	/*
         *this is to handle the situation: (1) ordering, and (2) aggregation are both required. 
         *
         */
        void retrieveKeyWithValue(
              ByteArrayKeyWithVariableLength::MergeSortedMapBuckets  &mergeResultHolder, 
              size_t currentKeyTracker);

	/*
	 * to shudown the channel and release the necessary resources, including the values created from malloc. 
	 */
	void shutdown() {
	    //WARNING: this is not an efficient way to do the work. we will have to use the big buffer to do memory copy,
	    //instead of keep doing malloc. we will have to improve this.
	    //for (auto p = allocatedValues.begin(); p != allocatedValues.end(); ++p) {
		//	free(*p);
	   //}
		
	}
	
};

class MergeSortReduceEngineWithByteArrayKeys {
private:
	vector <MergeSortReduceChannelWithByteArrayKeys>  mergeSortReduceChannels;
	int reduceId;
	int totalNumberOfPartitions;  

        //the variable-length key is stored in buffer manager managed bytebuffer.
	PositionInExtensibleByteBuffer currentMergedKeyValue; 
        unsigned char *currentMergedKeyPtr;
        int currentMergedKeyValueSize; 

	//buffer manager as passed in
	ExtensibleByteBuffers *kBufferMgr; 
	ExtensibleByteBuffers *vBufferMgr; 

	//the priority queue
	priority_queue<PriorityQueuedElementWithVariableLengthKey, 
               vector <PriorityQueuedElementWithVariableLengthKey>,
               ComparatorForPriorityQueuedElementWithVariableLengthKey> mergeSortPriorityQueue;
public: 

	//passed in: the reducer id and the total number of the partitions for the reduce side.
	MergeSortReduceEngineWithByteArrayKeys(int rId, int rPartitions, 
				       ExtensibleByteBuffers *kBufMgr, ExtensibleByteBuffers *vBufMgr):
	reduceId(rId), totalNumberOfPartitions(rPartitions), currentMergedKeyValue(-1, -1, 0),
          currentMergedKeyPtr (nullptr),
	  currentMergedKeyValueSize (0),
	  kBufferMgr(kBufMgr),
          vBufferMgr(vBufMgr),
	  mergeSortPriorityQueue (ComparatorForPriorityQueuedElementWithVariableLengthKey(kBufMgr))  {

	}

	/*
	 * to add the channel for merge sort, passing in the map bucket. 
	 */
	void addMergeSortReduceChannel(MapBucket &mapBucket) {
	   MergeSortReduceChannelWithByteArrayKeys channel(mapBucket,
			   reduceId, totalNumberOfPartitions, kBufferMgr, vBufferMgr);
	   mergeSortReduceChannels.push_back(channel);
	}

	/*
	 * to init the merge sort engine 
	 */
	void init(); 


        /*
         * to reset the buffer manager buffer to the beginning, for next key/values pair retrieval
         */
        //void reset_buffermgr() {
	//  bufferMgr->reset();
	//}
          
	/*
	* to decide whether this channel is finished the scanning or not.
	*/
	bool hasNext() {
		return (!mergeSortPriorityQueue.empty()); 
	}

        /*
         * to handle the situation that requires: (1) ordering, and (2) aggregation
	 */
	void getNextKeyValuesPair(ByteArrayKeyWithVariableLength::MergeSortedMapBuckets& mergedResultHolder);

        /*
         * to handle the situation that requires: (1) ordering, and (2) no aggregation
	 */
	void getNextKeyValuePair(ByteArrayKeyWithVariableLength::MergeSortedMapBuckets& mergedResultHolder);


	/*
	 *for all of channels to be merged, to get the next unique key.  For example, key = "abcdef". 
	 */
	PositionInExtensibleByteBuffer  getCurrentMergedKey() {
	     return currentMergedKeyValue; 
	}

	unsigned char *getCurrentMergedKeyPtr() {
          return currentMergedKeyPtr;
        }

	int  getCurrentMgergedKeyValueSize() {
          return currentMergedKeyValueSize;
        }

	void shutdown() {
	  for (auto p = mergeSortReduceChannels.begin(); p != mergeSortReduceChannels.end(); ++p) {
		p->shutdown();
	  }
	  //remove all of the channels.
	  mergeSortReduceChannels.clear();
	}

};


#endif /*_MERGE_SORT_REDUCE_CHANNEL_WITH_BYTEARRAY_KEY_H_*/
