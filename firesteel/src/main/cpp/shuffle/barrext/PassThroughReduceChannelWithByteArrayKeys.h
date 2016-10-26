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

#ifndef  _PASSTHROUGH_REDUCE_CHANNEL_WITH_BYTEARRAY_KEY_H_
#define  _PASSTHROUGH_REDUCE_CHANNEL_WITH_BYTEARRAY_KEY_H_

#include "GenericReduceChannel.h"
#include "ShuffleConstants.h"
#include "PassThroughKeyValueTrackerWithByteArrayKeys.h"

#include <vector>

using namespace std; 

/*
 *for direct pass-through key/value from the buckets, without goin through sort/merge
 *or hash-aggregation.
 *
 *The wrapper on each MapBucket to keep track of the current cursor  and current value 
 *
*/
class  PassThroughReduceChannelWithByteArrayKeys: public GenericReduceChannel {
private:   
	int  kvalueCursor;
        //NOTE: it is direct pass through, and scan the whole channel before moving to the next one.
        //so it is efficient to recorded just the pointer of the key and the pointer of the value
        //this is different from sort-mergesort and hash map where data will be aggregated first.
	unsigned char *currentKeyPtr; 
        int currentKeyValueSize; 
	unsigned char *currentValuePtr; //direct value pointer recorded in channel buffer.
	int currentValueSize; //the size of the current value 

public: 
	
	PassThroughReduceChannelWithByteArrayKeys (MapBucket &sourceBucket, int rId, int rPartitions):
	        GenericReduceChannel (sourceBucket, rId, rPartitions),
		kvalueCursor(0),
		currentKeyPtr(nullptr),
		currentKeyValueSize(0),
		currentValuePtr(nullptr),
                currentValueSize(0){ 
		 //NOTE: total length passed from the reducer's mapbucket is just an approximation, 
                 //as when map status passed to the scheduler, bucket size compression happens.
                 //this total length gets recoverred precisely, at the reduce channel init () call.

	}

	~PassThroughReduceChannelWithByteArrayKeys() {
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
	 * return the current key's pointer in the mapped virtual memory address space.
	 */
	unsigned char* getCurrentKeyPtr() {
	     return currentKeyPtr;
	}
	
        int  getCurrentKeyValueSize () {
	     return currentKeyValueSize; 
	}

	/*
	* return the current value as a pointer in the mapped virtual memory address space.
	*/
	unsigned char* getCurrentValuePtr() {
	    return currentValuePtr; 
	}

	/*
	* return the current value's size. 
	*/
	int getCurrentValueSize() {
	   return currentValueSize; 
	}

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


class PassThroughReduceEngineWithByteArrayKeys {
private:
	vector <PassThroughReduceChannelWithByteArrayKeys>  passThroughReduceChannels;
	int reduceId;
	int totalNumberOfPartitions;  

        //no merging at all, with one channel at a time, so we can use direct pointer of the key
	unsigned char *currentKeyPtr;
        int currentKeySize; 
        int currentChannelIndex; //current channel under scanning
        int sizeOfChannels; //total number of the channels registered.

private:


public: 

	//passed in: the reducer id and the total number of the partitions for the reduce side.
	PassThroughReduceEngineWithByteArrayKeys(int rId, int rPartitions) :
	        reduceId(rId), totalNumberOfPartitions(rPartitions),
	        currentKeyPtr (nullptr), currentKeySize (0),
		currentChannelIndex (0),
		sizeOfChannels (0)
                {
		    //do nothing
	}

	/*
	 * to add the channel for merge sort, passing in the map bucket. 
	 */
	void addPassThroughReduceChannel(MapBucket &mapBucket) {
	    PassThroughReduceChannelWithByteArrayKeys  channel(mapBucket,reduceId, totalNumberOfPartitions);
    	    passThroughReduceChannels.push_back(channel);
	}

	/*
	 * to init the passthrough reduce engine.
	 */
	void init(); 


        /*
         * to reset the buffer manager buffer to the beginning, for next key/values pair retrieval
         */
        //void reset_buffermgr() {
	//  bufferMgr->reset();
	//}
          
	/*
	* to decide whether the engine has exhausted scanning all of the channels 
	*/
	bool hasNext() {
          if((currentChannelIndex < sizeOfChannels) && 
		passThroughReduceChannels[currentChannelIndex].hasNext()) {
	    return true;
	  } 
          else {
	    //move to next channel that has non-zero elements in the channel
 	    while( (currentChannelIndex < sizeOfChannels)
		   && (!passThroughReduceChannels[currentChannelIndex].hasNext())) {
	      currentChannelIndex++;
	    }
	    //until we run out of channel, and one channel has non-empty key/value pair
	    if((currentChannelIndex < sizeOfChannels) && 
	       passThroughReduceChannels[currentChannelIndex].hasNext()) {
   	       return true;
	    }
	    else {
              return false; 
	    }
	  }
	}

        //retrieve the key/value pair from a passed-through channel, and copy it to the byte buffer passed
        //from the Java side.
	void getNextKeyValuePair(ByteArrayKeyWithVariableLength::PassThroughMapBuckets& passThroughResultHolder);


        /**
         * return the pointer of the current key
	 */
	unsigned char*  getCurrentKeyPtr() {
	    return currentKeyPtr; 
	}

        int getCurrentKeySize() {
	    return currentKeySize; 
	}


        //to release the DRAM related resources.
        void shutdown(); 
};


#endif /* _PASSTHROUGH_REDUCE_CHANNEL_WITH_BYTEARRAY_KEY_H_ */
