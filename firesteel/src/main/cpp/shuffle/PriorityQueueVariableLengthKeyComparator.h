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
#ifndef _PRIORITY_QUEUE_VARIABLE_LENGTH_KEY_COMPARATOR_H_ 
#define _PRIORITY_QUEUE_VARIABLE_LENGTH_KEY_COMPARATOR_H_ 

#include "ExtensibleByteBuffers.h"
#include "VariableLengthKeyComparator.h" 
#include <string.h>
#include <byteswap.h>

#include <vector>
#include <queue> 

using namespace std; 

struct PriorityQueuedElementWithVariableLengthKey {
	int mergeChannelNumber; //the unique mergeChannel that the value belonging to;
	PositionInExtensibleByteBuffer fullKeyValue; // the exact value to be compared
        const unsigned char *kValuePtr; //pointer to the key value
        int kValueSize;  //the size of the key value
        unsigned long normalizedKey;  //the normalized key for comparision.
        //buffer manager as passed in
        ExtensibleByteBuffers *kBufferMgr;

        PriorityQueuedElementWithVariableLengthKey(
	   int channelNumber, 
           const PositionInExtensibleByteBuffer &kValue, 
           const unsigned char* kvPtr, int valSize,
           ExtensibleByteBuffers *bMgr):
	   mergeChannelNumber(channelNumber),
	   fullKeyValue(kValue), 
	   kValuePtr (kvPtr), kValueSize(valSize),
           normalizedKey(0),
	   kBufferMgr(bMgr){
	   //now fill the normalized keys.
	   unsigned long tvalue;
	   if (valSize < SHMShuffleGlobalConstants::NORMALIZED_KEY_SIZE) { 
	     memcpy(&tvalue, kvPtr, valSize);
	   }
           else {
             memcpy (&tvalue, kvPtr, SHMShuffleGlobalConstants::NORMALIZED_KEY_SIZE);
	   }

          //our HPE machine is little endian.
 	   normalizedKey = bswap_64(tvalue);
	}
};

class ComparatorForPriorityQueuedElementWithVariableLengthKey {

private:
     //key buffer manager as passed in
     ExtensibleByteBuffers *kBufferMgr;

public:

     ComparatorForPriorityQueuedElementWithVariableLengthKey (ExtensibleByteBuffers *bufMgr):
            kBufferMgr (bufMgr)  {
     }

     //refer to http://www.cplusplus.com/articles/2LywvCM9/ for declaring and defining
     //C++ inline function. Option1: if the next line is with "inline", then the implementation 
     //body will need to be right next to the declaration. Option 2: the inline keyword only
     //shows up at the function implementation.  
     inline bool operator()(const PriorityQueuedElementWithVariableLengthKey &a,
			    const PriorityQueuedElementWithVariableLengthKey &b) {
       if (a.normalizedKey > b.normalizedKey) {
	  return true; 
       }
       else if (a.normalizedKey < b.normalizedKey) {
          return false;
       }
       else {
         //make full comparision for: a.normalizedKey == b.normalizedKey
         return(VariableLengthKeyComparator::less_than (kBufferMgr, a.fullKeyValue, b.fullKeyValue));
       }
     }
};


#endif /*_PRIORITY_QUEUE_VARIABLE_LENGTH_KEY_COMPARATOR_H_ */
