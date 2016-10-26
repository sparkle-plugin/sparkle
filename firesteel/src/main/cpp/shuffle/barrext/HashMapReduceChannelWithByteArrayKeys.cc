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
#include "HashMapReduceChannelWithByteArrayKeys.h"
#include "MergeSortKeyPositionTrackerWithByteArrayKeys.h"
#include "ShuffleDataSharedMemoryReader.h"
#include "MapShuffleStoreManager.h"
#include "ShuffleStoreManager.h"
#include "ShuffleConstants.h"
#include "ShuffleDataSharedMemoryManager.h"
#include <immintrin.h>

void HashMapReduceChannelWithByteArrayKeys::getNextKeyValuePair() {
	 
	unsigned char *oldPtr = currentPtr; 
        //prefetching the cache lines. Each cache line is 64 bytes. total 6 cache lines.
        //NOTE: the number of prefetching originally was from long/int type. We needs to do the tuning
        //to see whether it is still true.
        _mm_prefetch((char*)&currentPtr[0],   _MM_HINT_T0);
        _mm_prefetch((char*)&currentPtr[64],  _MM_HINT_T0);
        _mm_prefetch((char*)&currentPtr[128], _MM_HINT_T0);
        _mm_prefetch((char*)&currentPtr[192], _MM_HINT_T0);
        _mm_prefetch((char*)&currentPtr[256], _MM_HINT_T0);
        _mm_prefetch((char*)&currentPtr[320], _MM_HINT_T0);

        //(1) read byte array key's size, then advance the pointer;
	ShuffleDataSharedMemoryReader::read_datachunk_value(currentPtr, 
		    (unsigned char*)&currentKeyValueSize , sizeof(currentKeyValueSize));

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
        
        //(4) read value's value into local VALUE buffer manager 
        currentValueValue =
          ShuffleDataSharedMemoryReader::read_datachunk_keyassociated_value(
					    currentPtr, currentValueSize, vBufferMgr);
        VLOG(2) << "retrieved data chunk value at memory address: " << (void*) currentPtr
	        << " size: " << currentValueSize;

        //after that, move the pointer right after the value.
        currentPtr += currentValueSize;

	kvalueCursor++;
	totalBytesScanned += (currentPtr - oldPtr); 
}

//this is the method in which the key/value pair contributed for all of the map channnels 
//get populated into the hash table.
void HashMapReduceEngineWithByteArrayKeys::init() {
     //to build the hash table for each channel.
     for (auto p = hashMapReduceChannels.begin(); p != hashMapReduceChannels.end(); ++p) {
	 p->init();
         while (p->hasNext()) {
	    p->getNextKeyValuePair(); 
            PositionInExtensibleByteBuffer key = p->getCurrentKeyValue();
            PositionInExtensibleByteBuffer position=p->getCurrentValueValue();
            int position_tracker= valueLinkList.addValue(position);

            //push to the hash map:(1) to check whether the key exists
            unordered_map<PositionInExtensibleByteBuffer, int,
		  HashMapByteArrayRecord_hash, HashMapByteArrayRecord_equal>::const_iterator got
                  =hashMergeTable.find(key);

            if (got != hashMergeTable.end()) {
	       //this is not the first time the key is inserted.
               int previous_position = got->second; 
               valueLinkList.addLinkingOnValue (position_tracker, previous_position);
               //update the value corresponding to this existing key.
               hashMergeTable[key]=position_tracker;
	    }
            else{
	       //(2): this is the first time the key is inserted.
               hashMergeTable.insert (make_pair(key, position_tracker));
	   }
                 
	}
     }

     //NOTE: should the total number of the channels to be merged is equivalent to total number of partitions?
     //or we only consider the non-zero-sized buckets/channels to be merged? 
       
     //at the end, we get the iterator, ready to be retrieved for key values pair.
     totalRetrievedKeyElements=0;
     hashMergeTableSize= hashMergeTable.size();
     hashMapIterator = hashMergeTable.begin();
}

/*
 * HashMap based merging, without considering the ordering.
 */
void HashMapReduceEngineWithByteArrayKeys::getNextKeyValuesPair(
                              ByteArrayKeyWithVariableLength::MergeSortedMapBuckets&  mergedResultHolder) {

  if (hashMapIterator != hashMergeTable.end()) {
    currentMergedKeyValue = hashMapIterator->first;
    int position_tracker = hashMapIterator->second;

    //need to add to mergedresult holder. current key tracker is the index to the key
    //NOTE: ignore the actual kvalue pointer, only use the cached key in local key buffer.
    size_t currentKeyTracker=mergedResultHolder.addKey(
                               nullptr, currentMergedKeyValue.value_size, currentMergedKeyValue);
    PositionInExtensibleByteBuffer currentValueValue =
                    valueLinkList.valuesBeingTracked[position_tracker].value;
    //NOTE:similarly, ignore actual value value pointer,only use the cached val in local value buffer
    ByteArrayKeyWithVariableLength::MergeSortedValueTracker ptracker 
                                          (nullptr, currentValueValue.value_size, currentValueValue);
    mergedResultHolder.addValueOnKey(currentKeyTracker, ptracker);
  
    VLOG(2) << "hash-merge(byte array): key size=" << currentMergedKeyValue.value_size << " first element info: " 
	    <<" start buffer: " << currentValueValue.start_buffer
	    << " position: " << currentValueValue.position_in_start_buffer
	    << " size: " << currentValueValue.value_size
	    << " with value buffer manager internal position: "
            << vBufferMgr->current_buffer().position_in_buffer();

    int linkedElement = 
                    valueLinkList.valuesBeingTracked[position_tracker].previous_element;
    //-1 is the initialized value for each HashMapValueLinkingTracker element
    int linkCount=1; //first element already retrieved.
    while (linkedElement != -1)
    {
      PositionInExtensibleByteBuffer linkedValueValue = 
	      valueLinkList.valuesBeingTracked[linkedElement].value;
      ByteArrayKeyWithVariableLength::MergeSortedValueTracker vtracker 
                                           (nullptr, linkedValueValue.value_size, linkedValueValue);
      mergedResultHolder.addValueOnKey(currentKeyTracker, vtracker);

      VLOG(2) << "hash-merge(byte array): key size=" << currentMergedKeyValue.value_size 
              << " linked element info: " 
	      << " current count: " << linkCount
	      <<" start buffer: " << linkedValueValue.start_buffer
	      << " position: " << linkedValueValue.position_in_start_buffer
	      << " size: " << linkedValueValue.value_size
	      << " with value buffer manager internal position: "
              << vBufferMgr->current_buffer().position_in_buffer();

      //advance to possible next element.
      linkedElement = valueLinkList.valuesBeingTracked[linkedElement].previous_element;
      linkCount++;
    }

    VLOG(2) << "hash-merge(byte array): key size=" << currentMergedKeyValue.value_size
            << " total linked count: " << linkCount;
    //then traverse the position tracker to go back to the link list.
    totalRetrievedKeyElements ++;

    hashMapIterator++;
  }
}


void HashMapReduceEngineWithByteArrayKeys::shutdown() {
  for (auto p = hashMapReduceChannels.begin(); p != hashMapReduceChannels.end(); ++p) {
	p->shutdown();
  }
  hashMapReduceChannels.clear();
  if (VLOG_IS_ON(2)) {
    VLOG(2) << "hash-merge (key = byte array) channels are shutdown, with current size: " 
            << hashMapReduceChannels.size();
  }

  hashMergeTable.clear();
  if (VLOG_IS_ON(2)) {
        VLOG(2) << "hash-merge (key =byte arraby) table is cleared, with current size: "
                << hashMergeTable.size();
  }

  //link list auxillary to the hash table, as map can only hold one element
   valueLinkList.release();
   VLOG(2) << "hash map value linklist is released. ";
}

