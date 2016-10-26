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

#ifndef  _MERGE_SORT_REDUCE_CHANNEL_WITH_INT_KEY_H_
#define  _MERGE_SORT_REDUCE_CHANNEL_WITH_INT_KEY_H_

#include "GenericReduceChannel.h"
#include "ExtensibleByteBuffers.h"
#include "MergeSortKeyPositionTrackerWithIntKeys.h"
#include <vector>
#include <queue> 

using namespace std; 

/*
 *The wrapper on each MapBucket to keep track of the current cursor  and current value 
 *
*/
class  MergeSortReduceChannelWithIntKeys: public GenericReduceChannel {
private:   
	int  kvalueCursor;
	int  currentKeyValue; 
	//unsigned char *currentValueValue; //retrieved with the current key 
	PositionInExtensibleByteBuffer currentValueValue; //use the value tracker in the buffer manager. 
	int currentValueSize; //the size of the current value 
	//buffer manager, as passed in.
	ExtensibleByteBuffers  *bufferMgr;

public: 
	
	MergeSortReduceChannelWithIntKeys (MapBucket &sourceBucket, int rId, int rPartitions,
                ExtensibleByteBuffers * bufMgr) :
        	GenericReduceChannel(sourceBucket, rId, rPartitions),
		kvalueCursor(0),
		currentKeyValue(0),
		currentValueValue(-1, -1, 0),
		currentValueSize(0), 
		bufferMgr (bufMgr) {
		 //NOTE: total length passed from the reducer's mapbucket is just an approximation, 
                 //as when map status passed to the scheduler, bucket size compression happens.
                 //this total length gets recoverred precisely, at the reduce channel init () call.

	}

	~MergeSortReduceChannelWithIntKeys() {
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
	 * return the current key's value
	 */
	int getCurrentKeyValue() {
		return currentKeyValue; 
	}
	
	/*
	* return the current value corresponding to the current key. 
	*/
	PositionInExtensibleByteBuffer getCurrentValueValue() {
		return currentValueValue; 
	}

	/*
	* return the current value's size. 
	*/
	int getCurrentValueSize() {
		return currentValueSize; 
	}

	/*
         *this is to handle the situation: (1) ordering, and (2) aggregation are both required.
	 *to popuate the passed-in holder by retrieving the Values and the corresonding sizes of
         *the Values,based on the provided key at the current position. Each time a value is retrieved,
         *the cursor will move to the next different key value. 

	 *Note that duplicated keys can exist for a given Key value.  The occupied heap memory will 
         *be released later when the full batch of the key and multiple values are done. 

	 *return the total number of the values identified that has the key  equivalent to the current key. 
	 */
	int retrieveKeyWithMultipleValues(IntKeyWithFixedLength::MergeSortedMapBuckets &mergeResultHolder, 
                                          size_t currentKeyTracker);

        /*
         *this is to handle the situation: (1) ordering, and (2) aggregation are both required.
	 */
	void retrieveKeyWithValue(
	      IntKeyWithFixedLength::MergeSortedMapBuckets &mergeResultHolder,
	      size_t currentKeyTracker);

	/*
	 * to shudown the channel and release the necessary resources, including the values created 
         *from malloc. 
	 */
	void shutdown() override {
	    //WARNING: this is not an efficient way to do the work. we will have to use the big buffer
            //to do memory copy,
	    //instead of keep doing malloc. we will have to improve this.
	    //for (auto p = allocatedValues.begin(); p != allocatedValues.end(); ++p) {
		//	free(*p);
	    //}
		
	}
};

struct PriorityQueuedElementWithIntKey {
	int mergeChannelNumber; //the unique mergeChannel that the value belonging to;
	int keyValue; // the value to be compared.

	PriorityQueuedElementWithIntKey(int channelNumber, int kValue) :
		mergeChannelNumber(channelNumber),
		keyValue(kValue) {

	}
};

class ComparatorForPriorityQueuedElementWithIntKey {

public:
	inline bool operator()(const PriorityQueuedElementWithIntKey &a, 
	                              const PriorityQueuedElementWithIntKey &b) {
	   //we want to have the order with the smallest first. 
	   return (a.keyValue > b.keyValue);
	}

};


class MergeSortReduceEngineWithIntKeys {
private:
	vector <MergeSortReduceChannelWithIntKeys>  mergeSortReduceChannels;
	int reduceId;
	int totalNumberOfPartitions;  

	int currentMergedKey; 

	//buffer manager as passed in
	ExtensibleByteBuffers *bufferMgr; 

private:
	//the priority queue
	priority_queue<PriorityQueuedElementWithIntKey, vector <PriorityQueuedElementWithIntKey>,
		              ComparatorForPriorityQueuedElementWithIntKey> mergeSortPriorityQueue;
public: 

	//passed in: the reducer id and the total number of the partitions for the reduce side.
	MergeSortReduceEngineWithIntKeys(int rId, int rPartitions, ExtensibleByteBuffers *bufMgr) :
		reduceId(rId), totalNumberOfPartitions(rPartitions), currentMergedKey(-1),
		bufferMgr(bufMgr){

	}

	/*
	 * to add the channel for merge sort, passing in the map bucket. 
	 */
	void addMergeSortReduceChannel(MapBucket &mapBucket) {
	    MergeSortReduceChannelWithIntKeys  channel(mapBucket, 
                                                  reduceId, totalNumberOfPartitions, bufferMgr);
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
         *to handle the situation that requires: (1) ordering, and (2) aggregation
	 */
	void getNextKeyValuesPair(IntKeyWithFixedLength::MergeSortedMapBuckets& mergedResultHolder);
     
	/*
         *to handle the situation that requires: (1) ordering, and (2) no aggregation
	 */
        void getNextKeyValuePair(IntKeyWithFixedLength::MergeSortedMapBuckets& mergedResultHolder);

	/*
	 *for all of channels to be merged, to get the next unique key.  For example, key =  198. 
	 */
	int  getCurrentMergedKey() {
		return currentMergedKey; 
	}

	void shutdown() {
	  for (auto p = mergeSortReduceChannels.begin(); p != mergeSortReduceChannels.end(); ++p) {
		p->shutdown();
	  }
         
          //remove all of the channels. 
          mergeSortReduceChannels.clear();
	}
};


#endif 
