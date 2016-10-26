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

#ifndef  _HASH_MAP_BASED_REDUCE_CHANNEL_WITH_BYTEARRAY_KEY_H_
#define  _HASH_MAP_BASED_REDUCE_CHANNEL_WITH_BYTEARRAY_KEY_H_

#include "GenericReduceChannel.h"
#include "ShuffleConstants.h"
#include "SimpleUtils.h"
#include "ExtensibleByteBuffers.h"
#include "HashMapKeyPositionTracker.h"
#include "MergeSortKeyPositionTrackerWithByteArrayKeys.h"
#include "VariableLengthKeyComparator.h"
 
#include <vector>
#include <unordered_map> 

using namespace std; 

/*
 *The wrapper on each MapBucket to keep track of the current cursor  and current value 
 *
*/
class  HashMapReduceChannelWithByteArrayKeys: public GenericReduceChannel{
private:   
	int  kvalueCursor;
        PositionInExtensibleByteBuffer currentKeyValue;  //also cached in local key buffer. 
	unsigned char *currentKeyPtr; //pointer mapped to the virtual address space. 
        int currentKeyValueSize; //the size of the key. 

	//unsigned char *currentValueValue; //retrieved with the current key 
	PositionInExtensibleByteBuffer currentValueValue; //use the value tracker in the local value buffer 
	int currentValueSize; //the size of the current value 

	//buffer manager, as passed in.
	ExtensibleByteBuffers  *kBufferMgr;
	ExtensibleByteBuffers  *vBufferMgr;

public: 
	
	HashMapReduceChannelWithByteArrayKeys (MapBucket &sourceBucket, int rId, int rPartitions,
					  ExtensibleByteBuffers *kBufMgr,
                                          ExtensibleByteBuffers *vBufMgr) :
	        GenericReduceChannel (sourceBucket, rId, rPartitions),
		kvalueCursor(0),
		currentKeyValue(-1, -1, 0),
	        currentKeyPtr (nullptr),
	        currentKeyValueSize (0),
		currentValueValue(-1, -1, 0),
		currentValueSize(0), 
	        kBufferMgr (kBufMgr),
                vBufferMgr (vBufMgr) {
		 //NOTE: total length passed from the reducer's mapbucket is just an approximation, 
                 //as when map status passed to the scheduler, bucket size compression happens.
                 //this total length gets recoverred precisely, at the reduce channel init () call.

	}

	~HashMapReduceChannelWithByteArrayKeys() {
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
         *return the current direct pointer of the key
	 */
	unsigned char *getKValuePtr() {
          return currentKeyPtr;

        }

        /*
         * return the size of the current key
	 */
	int  getCurrentKeyValueSize() {
          return currentKeyValueSize;
        }

	/*
	 * return the current key's value
	 */
	PositionInExtensibleByteBuffer  getCurrentKeyValue() {
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

//with record beging PositionInExtensbileByteBuffer
struct HashMapByteArrayRecord_hash {
   ExtensibleByteBuffers *bufferMgr; 
   
  size_t operator () (const PositionInExtensibleByteBuffer& record) const {
     //compute the hash value
    int t_buffer=record.start_buffer;
    int t_position=record.position_in_start_buffer;
    int t_scanned = 0;

    int t_buffer_capacity=bufferMgr->buffer_at(t_buffer).capacity();
    bool done = false;
    size_t result = 0;
    size_t prime = 31;

    std::hash<unsigned char> hash_fn;

    while (!done) {
      unsigned char va = bufferMgr->buffer_at(t_buffer).value_at(t_position);
      result = result *prime + hash_fn(va);

      t_scanned++;
      if (t_scanned == record.value_size) {
	  done = true;
      }
      else  {
          t_position++;
          if (t_position == t_buffer_capacity) {
	    t_buffer++;
            t_position=0;
	  }
     }
   }//done

   return result;
  }

};

//with record beging PositionInExtensbileByteBuffer
struct HashMapByteArrayRecord_equal {
  ExtensibleByteBuffers *bufferMgr; 

  bool operator () (const PositionInExtensibleByteBuffer& r1,
		    const PositionInExtensibleByteBuffer& r2)  const {
    return (VariableLengthKeyComparator::is_equal (bufferMgr, r1, r2)) ;
  }
};


class HashMapReduceEngineWithByteArrayKeys {
private:
	vector <HashMapReduceChannelWithByteArrayKeys>  hashMapReduceChannels;
	int reduceId;
	int totalNumberOfPartitions;  

	PositionInExtensibleByteBuffer currentMergedKeyValue; 
        unsigned char *currentMergedKeyPtr;
        int currentMergedKeyValueSize;
         

	//buffer manager as passed in, one for key buffer and one for value buffer.
	ExtensibleByteBuffers *kBufferMgr; 
	ExtensibleByteBuffers *vBufferMgr; 
       
	//key: the passed-in integer key.
        //value: the position in the hash-map tracker. start with zero capacity.
        //we will like to see whether it is better to start with same initial value
        
        //NOTE: consider Google HashTable whether there is API to clean up the current key/values, but
        //keep the allocated memory for future re-use?
        //key: with type of PositionInextensbileByteBuffer,
        //value: int,
        //and then hash function and equal function.
	unordered_map<PositionInExtensibleByteBuffer, int, 
                 HashMapByteArrayRecord_hash, HashMapByteArrayRecord_equal> hashMergeTable; 

        //link list auxillary to the hash table, as map can only hold one element
	KeyWithFixedLength::HashMapValueLinkingWithSameKey  valueLinkList;
    
        int totalRetrievedKeyElements;
        //this needs to be set before key/values retrieval starts.
        int hashMergeTableSize; 

        //this will be set before key/values retrieval starts.
        unordered_map<PositionInExtensibleByteBuffer, int, 
                      HashMapByteArrayRecord_hash, HashMapByteArrayRecord_equal>::iterator hashMapIterator;
public: 

	//passed in: the reducer id and the total number of the partitions for the reduce side.
	HashMapReduceEngineWithByteArrayKeys(int rId, int rPartitions, 
				ExtensibleByteBuffers *kBufMgr, ExtensibleByteBuffers *vBufMgr) :
	reduceId(rId), totalNumberOfPartitions(rPartitions), currentMergedKeyValue (-1, -1, 0),
	          currentMergedKeyPtr (nullptr),
	          currentMergedKeyValueSize (0),
		  kBufferMgr(kBufMgr),
                  vBufferMgr(vBufMgr),
                  hashMergeTable {SHMShuffleGlobalConstants::HASHMAP_INITIALIAL_CAPACITY,
	                    HashMapByteArrayRecord_hash{kBufMgr}, HashMapByteArrayRecord_equal{kBufMgr}},
                  valueLinkList(rId),
                  totalRetrievedKeyElements(0),
                  hashMergeTableSize (0)
                  {
		    //do nothing
	}

	/*
	 * to add the channel for merge sort, passing in the map bucket. 
	 */
	void addHashMapReduceChannel(MapBucket &mapBucket) {
	    HashMapReduceChannelWithByteArrayKeys  channel(mapBucket, 
				   reduceId, totalNumberOfPartitions, kBufferMgr, vBufferMgr);
    	    hashMapReduceChannels.push_back(channel);
	}

	/*
	 * to init the hash-map merge engine 
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
	  return (totalRetrievedKeyElements < (int)hashMergeTableSize);
	}

        //NOTE: we still use the mergesorted map buckets as the result first. we will change the name of 
        //mergesorted map buckets later.
	void getNextKeyValuesPair(ByteArrayKeyWithVariableLength::MergeSortedMapBuckets& mergedResultHolder);


	/*
	 *for all of channels to be merged, to get the next unique key.  For example, key =  198. 
	 */
	PositionInExtensibleByteBuffer   getCurrentMergedKey() {
		return currentMergedKeyValue; 
	}

        unsigned char *getCurrentMergedKeyPtr() {
	  return currentMergedKeyPtr;
	};

        int  getCurrentMergedKeyValueSize() {
          return currentMergedKeyValueSize;
	}

        //to release the DRAM related resources.
        void shutdown(); 
};


#endif /*_HASH_MAP_BASED_REDUCE_CHANNEL_WITH_BYTEARRAY_KEY_H_*/
