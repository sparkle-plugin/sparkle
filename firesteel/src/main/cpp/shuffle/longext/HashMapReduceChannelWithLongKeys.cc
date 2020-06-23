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
#include "HashMapReduceChannelWithLongKeys.h"
#include "ShuffleDataSharedMemoryReader.h"
#include "MapShuffleStoreManager.h"
#include "ShuffleStoreManager.h"
#include "ShuffleConstants.h"
#include "ShuffleDataSharedMemoryManager.h"
#include <immintrin.h>

void HashMapReduceChannelWithLongKeys::getNextKeyValuePair() {
	 
	unsigned char *oldPtr = currentPtr; 
        //prefetching the cache lines. Each cache line is 64 bytes. total 4 cache lines.
        _mm_prefetch((char*)&currentPtr[0], _MM_HINT_T0);
        _mm_prefetch((char*)&currentPtr[64], _MM_HINT_T0);
        _mm_prefetch((char*)&currentPtr[128], _MM_HINT_T0);
        _mm_prefetch((char*)&currentPtr[192], _MM_HINT_T0);
        _mm_prefetch((char*)&currentPtr[256], _MM_HINT_T0);
        _mm_prefetch((char*)&currentPtr[320], _MM_HINT_T0);


	ShuffleDataSharedMemoryReader::read_datachunk_longkey(currentPtr, &currentKeyValue, &currentValueSize);

	VLOG(2) << "retrieved data chunk for map id: " << mapBucket.mapId
		<< " with key value: " << currentKeyValue << " and value size: " << currentValueSize;

	currentPtr += sizeof(currentKeyValue)+sizeof(currentValueSize);

	//WARNING: this is not an efficient way to do the work. we will have to use the big buffer to do memory copy,
	//instead of keep doing malloc. we will have to improve this. 
	//currentValueValue = (unsigned char*)malloc(currentValueSize);
	//allocatedValues.push_back(currentValueValue); //to keep track of the memory allocated.
	currentValueValue =
		ShuffleDataSharedMemoryReader::read_datachunk_keyassociated_value(
                   currentPtr, currentValueSize, bufferMgr); 

	VLOG(2) << "retrieved data chunk value: " << " start buffer: " << currentValueValue.start_buffer 
                << " position: " << currentValueValue.position_in_start_buffer
		<< " size: " << currentValueValue.value_size 
                << " with buffer manager internal position: " << bufferMgr->current_buffer().position_in_buffer();

	//after that , you need to move the pointer
	currentPtr += currentValueSize;
	kvalueCursor++;
	totalBytesScanned += (currentPtr - oldPtr); 
}

//this is the method in which the key/value pair contributed for all of the map channnels 
//get populated into the hash table.
void HashMapReduceEngineWithLongKeys::init() {
        //to build the hash table for each channel.
	for (auto p = hashMapReduceChannels.begin(); p != hashMapReduceChannels.end(); ++p) {
		p->init();
                while (p->hasNext()) {
		   p->getNextKeyValuePair(); 
                   long key = p->getCurrentKeyValue();
                   PositionInExtensibleByteBuffer position=p->getCurrentValueValue();
                   int position_tracker= valueLinkList.addValue(position);

                   //push to the hash map:(1) to check whether the key exists
                   unordered_map<long, int>::const_iterator got=hashMergeTable.find(key);
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
void HashMapReduceEngineWithLongKeys::getNextKeyValuesPair(
                              LongKeyWithFixedLength::MergeSortedMapBuckets& mergedResultHolder) {
  if (hashMapIterator != hashMergeTable.end()) {
    currentMergedKey = hashMapIterator->first;
    int position_tracker = hashMapIterator->second;

    //need to add to mergedresult holder. current key tracker is the index to the key
    size_t currentKeyTracker=mergedResultHolder.addKey(currentMergedKey);
    PositionInExtensibleByteBuffer currentValueValue =
                    valueLinkList.valuesBeingTracked[position_tracker].value;
    mergedResultHolder.addValueOnKey(currentKeyTracker, currentValueValue);
  
    VLOG(2) << "hash-merge(int): key=" << currentMergedKey << " first element info: " 
	    <<" start buffer: " << currentValueValue.start_buffer
	    << " position: " << currentValueValue.position_in_start_buffer
	    << " size: " << currentValueValue.value_size
	    << " with buffer manager internal position: "
            << bufferMgr->current_buffer().position_in_buffer()
            << endl;

    int linkedElement = 
                    valueLinkList.valuesBeingTracked[position_tracker].previous_element;
    //-1 is the initialized value for each HashMapValueLinkingTracker element
    int linkCount=1; //first element already retrieved.
    while (linkedElement != -1)
    {
      PositionInExtensibleByteBuffer linkedValueValue = 
	      valueLinkList.valuesBeingTracked[linkedElement].value;
      mergedResultHolder.addValueOnKey(currentKeyTracker, linkedValueValue);

      VLOG(2) << "hash-merge(int): key=" << currentMergedKey << " linked element info: " 
	      << " current count: " << linkCount
	      <<" start buffer: " << linkedValueValue.start_buffer
	      << " position: " << linkedValueValue.position_in_start_buffer
	      << " size: " << linkedValueValue.value_size
	      << " with buffer manager internal position: "
              << bufferMgr->current_buffer().position_in_buffer()
              << endl;

      //advance to possible next element.
      linkedElement = valueLinkList.valuesBeingTracked[linkedElement].previous_element;
      linkCount++;
    }

    VLOG(2) << "hash-merge(int): key=" << currentMergedKey << " total linked count: " << linkCount <<endl;
    //then traverse the position tracker to go back to the link list.
    totalRetrievedKeyElements ++;

    ++hashMapIterator;
  }
}


void HashMapReduceEngineWithLongKeys::shutdown() {
  for (auto p = hashMapReduceChannels.begin(); p != hashMapReduceChannels.end(); ++p) {
	p->shutdown();
  }
  hashMapReduceChannels.clear();
  if (VLOG_IS_ON(2)) {
    VLOG(2) << "hash-merge channels are shutdown, with current size: " 
            << hashMapReduceChannels.size() <<  endl;
  }

  hashMergeTable.clear();
  if (VLOG_IS_ON(2)) {
        VLOG(2) << "hash-merge table is cleared, with current size: " << hashMergeTable.size()<<endl;
  }

  //link list auxillary to the hash table, as map can only hold one element
   valueLinkList.release();
   VLOG(2) << "hash map value linklist is released. " << endl;
}

