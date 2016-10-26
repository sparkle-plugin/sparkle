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

#ifndef  _MERGE_SORT_REDUCE_CHANNEL_WITH_STRING_KEY_H_
#define  _MERGE_SORT_REDUCE_CHANNEL_WITH_STRING_KEY_H_

#include "ShuffleConstants.h"
#include "EnumKvTypes.h"
#include "MapStatus.h" 
#include "ExtensibleByteBuffers.h"
#include  "PriorityQueueVariableLengthKeyComparator.h"
#include <vector>
#include <queue> 

using namespace std; 

/*
 *The wrapper on each MapBucket to keep track of the current cursor  and current value 
 *
*/
class  MergeSortReduceChannelWithStringKeys{
private:   
	MapBucket  &mapBucket;
	int reducerId; //which reducer this channel belongs to. 
	int totalNumberOfPartitions; //this is for value checking 
	int  kvalueCursor; //cursor on the kvalue, independent of kvalue being int
	unsigned char *currentPtr; //pointer to the underlying merge-sort channel 
        //since it is variable lenght, we use the byte buffer to store the key's value as well
	PositionInExtensibleByteBuffer  currentKeyValue; 
	//unsigned char *currentValueValue; //retrieved with the current key 
	PositionInExtensibleByteBuffer currentValueValue; //use the value tracker in the buffer manager. 


        int currentKeySize; //since it is variable length, we need to read key's size as well.
	int currentValueSize; //the size of the current value 
	int totalBytesScanned; //the length scanned so far, on the merge-sort channel
	int totalLength;  //the total length to be scanned on the merge-sort channel

	//buffer manager, as passed in.
	BufferManager  *bufferMgr;

public: 
	
	MergeSortReduceChannelWithStringKeys (MapBucket &sourceBucket, int rId, int rPartitions,
                BufferManager* bufMgr) :
		mapBucket(sourceBucket), reducerId(rId),
		totalNumberOfPartitions(rPartitions),
		kvalueCursor(0),
		currentPtr(nullptr),
		currentKeyValue(-1, -1, 0),
		currentValueValue(-1, -1, 0),
	        currentKeySize(0),
		currentValueSize(0), 
		totalBytesScanned(0),

		totalLength(sourceBucket.size),
		
		bufferMgr (bufMgr) {
		  //NOTE: total length passed from the reducer's mapbucket is just an approximation,
                  //as when map status passed to the scheduler, bucket size compression happens. 
                  //this total length gets recoverred precisely, at the reduce channel init () call.
	}

	~MergeSortReduceChannelWithStringKeys() {
		//do nothing. 
	}

	/*
	 *to identify the data chunk, and then move to the first value 
	 */
	void init(); 

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
	* return the current value's size. 
	*/
	int getCurrentValueSize() {
		return currentValueSize; 
	}

	/*
	 *to popuate the passed-in holder by retrieving the Values and the corresonding sizes of the Values, 
	 *based on the provided key at the current position. Each time a value is retrieved, the cursor will move to the 
	 *next different key value. 

	 *Note that duplicated keys can exist for a given Key value.  The occupied heap memory will be released later 
	 *when the full batch of the key and multiple values are done. 

	 *return the total number of the values identified that has the key  equivalent to the current key. 
	 */
	int retrieveKeyWithMultipleValues(vector <PositionInExtensibleByteBuffer> & heldValues, 
                                                                            vector <int> & heldValueSizes);

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


class MergeSortReduceEngineWithStringKeys {
private:
	vector <MergeSortReduceChannelWithStringKeys>  mergeSortReduceChannels;
	int reduceId;
	int totalNumberOfPartitions;  

        //the variable-length key is stored in buffer manager managed bytebuffer.
	PositionInExtensibleByteBuffer currentMergedKey; 
	vector <PositionInExtensibleByteBuffer> currentMergedValues;
	vector <int> currentMergeValueSizes; 

	//buffer manager as passed in
	ExtensibleByteBuffers  *bufferMgr; 

private:
	//the priority queue
	priority_queue<PriorityQueuedElementWithVariableLengthKey, 
               vector <PriorityQueuedElementWithVariableLengthKey>,
               ComparatorForPriorityQueuedElementWithVariableLengthKey> mergeSortPriorityQueue;
public: 

	//passed in: the reducer id and the total number of the partitions for the reduce side.
	MergeSortReduceEngineWithStringKeys(int rId, int rPartitions, ExtensibleByteBuffers *bufMgr) :
	      reduceId(rId), totalNumberOfPartitions(rPartitions), currentMergedKey(-1, -1, 0),
	      bufferMgr(bufMgr),
	      mergeSortPriorityQueue (ComparatorForPriorityQueuedElementWithVariableLengthKey(bufMgr))
              {

	}

	/*
	 * to add the channel for merge sort, passing in the map bucket. 
	 */
	void addMergeSortReduceChannel(MapBucket &mapBucket) {
		MergeSortReduceChannelWithStringKeys  channel(mapBucket, reduceId, totalNumberOfPartitions, bufferMgr);
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

	void getNextKeyValuesPair();


	/*
	 *for all of channels to be merged, to get the next unique key.  For example, key = "abcdef". 
	 */
	PositionInExtensibleByteBuffer  getCurrentMergedKey() {
		return currentMergedKey; 
	}

	/*
	 *for all of the channles to be merged, to get the next collection of the merged values. 
	 *for example, values = { pointer 1, pointer2, pointer3. pointer4, pointer5} corresponds to the key = 198. 
	 */
	vector <PositionInExtensibleByteBuffer> & getCurrentMergeValues() {
		return currentMergedValues;
	}

	/*
	 *for all of the channles to be merged, to get the next collection of the merged values' corresponding sizes. 
	 *for example, corresponding to key = 198, value-sizes = {1, 8, 2, 2, 4}.  All value-sizes should be at least 1. 
	 *
	 */
	vector <int> & getCurrentMergeValueSizes() {
		return currentMergeValueSizes;
	}

	void shutdown() {
	  for (auto p = mergeSortReduceChannels.begin(); p != mergeSortReduceChannels.end(); ++p) {
		p->shutdown();
	  }
	}

};


#endif 
