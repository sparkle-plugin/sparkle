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

#ifndef  _PASSTHROUGH_REDUCE_CHANNEL_WITH_INT_KEY_H_
#define  _PASSTHROUGH_REDUCE_CHANNEL_WITH_INT_KEY_H_

#include "GenericReduceChannel.h"
#include "ShuffleConstants.h"
#include "PassThroughKeyValueTrackerWithIntKeys.h"

#include <vector>

using namespace std; 

/*
 *for direct pass-through key/value from the buckets, without goin through sort/merge
 *or hash-aggregation.
 *
 *The wrapper on each MapBucket to keep track of the current cursor  and current value 
 *
*/
class  PassThroughReduceChannelWithIntKeys: public GenericReduceChannel {
private:   
	int  kvalueCursor;
	int  currentKeyValue; 
	unsigned char *currentValueValue; //direct value pointer recorded in channel buffer.
	int currentValueSize; //the size of the current value 

public: 
	
	PassThroughReduceChannelWithIntKeys (MapBucket &sourceBucket, int rId, int rPartitions):
                GenericReduceChannel(sourceBucket, rId, rPartitions),
		kvalueCursor(0),
		currentKeyValue(0),
		currentValueValue(nullptr),
		currentValueSize(0){ 
		 //NOTE: total length passed from the reducer's mapbucket is just an approximation, 
                 //as when map status passed to the scheduler, bucket size compression happens.
                 //this total length gets recoverred precisely, at the reduce channel init () call.

	}

	~PassThroughReduceChannelWithIntKeys() {
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
	unsigned char* getCurrentValueValue() {
		return currentValueValue; 
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


class PassThroughReduceEngineWithIntKeys {
private:
    vector <PassThroughReduceChannelWithIntKeys> passThroughReduceChannels;
    int reduceId;
    int totalNumberOfPartitions;

    //no merging at all.
    int currentKey;
    int currentChannelIndex; //current channel under scanning
    int sizeOfChannels; //total number of the channels registered.

    long localBucketsRead {0};
    long remoteBucketsRead {0};

public:
    //passed in: the reducer id and the total number of the partitions for the reduce side.
    PassThroughReduceEngineWithIntKeys(int rId, int rPartitions):
    reduceId(rId), totalNumberOfPartitions(rPartitions), currentKey(-1),
    currentChannelIndex (0),
    sizeOfChannels (0) {
        //do nothing
    }

    /*
     * to add the channel for merge sort, passing in the map bucket.
     */
    void addPassThroughReduceChannel(MapBucket &mapBucket) {
        PassThroughReduceChannelWithIntKeys  channel(mapBucket,reduceId, totalNumberOfPartitions);
        passThroughReduceChannels.push_back(channel);
    }

    /*
     * to init the passthrough reduce engine.
     */
    void init();

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

                if (passThroughReduceChannels[currentChannelIndex].isSameNode()) {
                    localBucketsRead++;
                } else {
                    remoteBucketsRead++;
                }
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

    //NOTE: we still use the mergesorted map buckets as the result first. we will change the name of 
    //mergesorted map buckets later.
    void getNextKeyValuePair(IntKeyWithFixedLength::PassThroughMapBuckets& passThroughResultHolder);

    /*
     *for all of channels to be merged, to get the next unique key.  For example, key =  198. 
     */
    int getCurrentKey() {
        return currentKey;
    }

    //to release the DRAM related resources.
    void shutdown();

    long getLocalBucketsRead() {
        return localBucketsRead;
    }

    long getRemoteBucketsRead() {
        return remoteBucketsRead;
    }
};

#endif /*_PASSTHROUGH_REDUCE_CHANNEL_WITH_INT_KEY_H__*/
