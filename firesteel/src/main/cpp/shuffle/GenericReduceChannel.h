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

#ifndef  _GENERIC_REDUCE_CHANNEL_H_
#define  _GENERIC_REDUCE_CHANNEL_H_

#include "EnumKvTypes.h"
#include "MapStatus.h" 

using namespace std; 

/*
 *The generic wrapper on each MapBucket to keep track of the current cursor  and current value 
 *
*/
class  GenericReduceChannel {

protected:   
	MapBucket  &mapBucket;
	int reducerId; //which reducer this channel belongs to. 
	int totalNumberOfPartitions; //this is for value checking 
	unsigned char *currentPtr; 
	int totalBytesScanned; //the length scanned so far 
	int totalLength;  //the total length
	bool sameNode {false};

public: 
	
	GenericReduceChannel (MapBucket &sourceBucket, int rId, int rPartitions) :
		mapBucket(sourceBucket), reducerId(rId),
		totalNumberOfPartitions(rPartitions),
		currentPtr(nullptr),
		totalBytesScanned(0),
		totalLength(sourceBucket.size)
		{
		 //NOTE: total length passed from the reducer's mapbucket is just an approximation, 
                 //as when map status passed to the scheduler, bucket size compression happens.
                 //this total length gets recoverred precisely, at the reduce channel init () call.

	}

	virtual ~GenericReduceChannel() {
		//do nothing. 
	}

	/*
	 *to identify the data chunk, and then move to the first value 
	 */
	virtual void init(); 

        /*to be defined in the concrete reduce channel classes.
	 */
	virtual void shutdown();

	inline bool isSameNode() {
	  return sameNode;
	}
};

#endif /*_GENERIC_REDUCE_CHANNEL_H_*/
