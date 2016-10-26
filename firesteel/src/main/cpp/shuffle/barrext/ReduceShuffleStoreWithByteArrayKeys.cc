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
#include <cassert>
#include <iostream>
#include <vector>
#include "MapShuffleStoreManager.h"
#include "ReduceShuffleStoreWithByteArrayKeys.h"
#include "ShuffleDataSharedMemoryReader.h"
#include "MergeSortReduceChannelWithByteArrayKeys.h"
#include "HashMapReduceChannelWithByteArrayKeys.h"
#include "MergeSortKeyPositionTrackerWithByteArrayKeys.h"
#include "PassThroughKeyValueTrackerWithByteArrayKeys.h"
#include "PassThroughReduceChannelWithByteArrayKeys.h"


//note: we will need to have range checking later. 
ByteArrayKeyWithVariableLength::RetrievedMapBucket ReduceShuffleStoreWithByteArrayKey::retrieve_mapbucket(int mapId){
  //CHECK(mapId <totalNumberOfPartitions);
  CHECK(mapId < reduceStatus.getSizeOfMapBuckets());

  //to invoke this method, we will need to have the flag of ordering or aggregation to be turned on
  CHECK(orderingRequired || aggregationRequired);

  ByteArrayKeyWithVariableLength::RetrievedMapBucket result(reducerId, mapId);

  MapBucket mapBucket= reduceStatus.bucket_for_mapid(mapId); 
  MergeSortReduceChannelWithByteArrayKeys mergeSortReduceChannel (
			  mapBucket, reducerId, totalNumberOfPartitions, kBufferMgr, vBufferMgr);
  mergeSortReduceChannel.init(); 
 
  while (mergeSortReduceChannel.hasNext()) {
	 mergeSortReduceChannel.getNextKeyValuePair();
	 PositionInExtensibleByteBuffer keyValue = mergeSortReduceChannel.getCurrentKeyValue();
	 PositionInExtensibleByteBuffer valueValue = mergeSortReduceChannel.getCurrentValueValue();
	 int valueSize = mergeSortReduceChannel.getCurrentValueSize();

         //populate key's value
         unsigned char *keyValueInBuffer = (unsigned char*) malloc (keyValue.value_size);
         kBufferMgr->retrieve(keyValue, keyValueInBuffer);
         
         result.keys.push_back(keyValueInBuffer);
         result.keySizes.push_back(keyValue.value_size);

	 //populate value's value
	 //void retrieve (const PositionInExtensibleByteBuffer &posBuffer, unsigned char *buffer);
	 unsigned char *valueValueInBuffer = (unsigned char*)malloc(valueValue.value_size);
	 vBufferMgr->retrieve(valueValue, valueValueInBuffer);
	 result.values.push_back(valueValueInBuffer);
	 result.valueSizes.push_back(valueSize);
  }

  return result;
}

void ReduceShuffleStoreWithByteArrayKey::free_retrieved_mapbucket(
                       ByteArrayKeyWithVariableLength::RetrievedMapBucket &mapBucket){
    //vector <PositionInExtensibleByteBuffer> values = mapBucket.get_values();
    //for (auto p =values.begin(); p!=values.end(); ++p) {
    //  free (*p);
    //}
    //defer to shutdown for the buffer manager. 
}

//for real key/value pair retrieval                                                                                                                                 
void ReduceShuffleStoreWithByteArrayKey::init_mergesort_engine() {

  for (auto p = reduceStatus.mapBuckets.begin(); p != reduceStatus.mapBuckets.end(); ++p) {
    if (p->size > 0 ) {
      theMergeSortEngine.addMergeSortReduceChannel(*p);
    }
  }

  theMergeSortEngine.init();

  engineInitialized=true;
}


//for real key/value pair retrieval, based on hash map.
void ReduceShuffleStoreWithByteArrayKey::init_hashmap_engine() {

  for (auto p = reduceStatus.mapBuckets.begin(); p != reduceStatus.mapBuckets.end(); ++p) {
    if (p->size > 0) {
      theHashMapEngine.addHashMapReduceChannel(*p);
    }
  }

  theHashMapEngine.init();

  engineInitialized=true;
}

//for real key/value pair retrieval, based on direct pass-through
void ReduceShuffleStoreWithByteArrayKey::init_passthrough_engine() {
  for (auto p = reduceStatus.mapBuckets.begin(); p != reduceStatus.mapBuckets.end(); ++p) {
    if (p->size > 0) {
      thePassThroughEngine.addPassThroughReduceChannel(*p);
    }
  }

  thePassThroughEngine.init();

  engineInitialized=true;
}


//this is for testing purpose. 
ByteArrayKeyWithVariableLength::MergeSortedMapBucketsForTesting
           ReduceShuffleStoreWithByteArrayKey::retrieve_mergesortedmapbuckets () {
  ByteArrayKeyWithVariableLength::MergeSortedMapBucketsForTesting result (reducerId);

  if (!engineInitialized) {
    if (orderingRequired) {
      init_mergesort_engine();
    }
    else if (aggregationRequired){
      init_hashmap_engine();
    }
    else {
      //for runtime assertion
      CHECK(orderingRequired || aggregationRequired)
        << "retrieval of mergesortmapbucket requires flag of ordering/aggregation to be on";
    }
  }

  //make sure that the holder is really activated.
  CHECK(mergedResultHolder.isActivated());
  //to reset the position buffers for merge-sorted result.
  mergedResultHolder.reset();

  if (orderingRequired) {
    //to handle two situations
    if (aggregationRequired){ 
      //situation 1: both ordering and aggregation are requird.
      while (theMergeSortEngine.hasNext()) {
        theMergeSortEngine.getNextKeyValuesPair(mergedResultHolder);
      }
    }
    else{
      //situation 2: ordering, but no aggregation, is required, an example is sort operator: sortBy,
      //which does not require aggregation.
      while (theMergeSortEngine.hasNext()) {
        theMergeSortEngine.getNextKeyValuePair(mergedResultHolder);
      }
    }
  }
  else if (aggregationRequired){
    while (theHashMapEngine.hasNext()) {
      theHashMapEngine.getNextKeyValuesPair(mergedResultHolder);
    }
  }
  else {
    //runtime assertion
    CHECK(orderingRequired || aggregationRequired)
      << "retrieval of mergesortmapbucket requires flag of ordering/aggregation to be on";
  }

  //then formulate the returned results. 
  //the keys and for each key the position pointers.  
  //NOTE: for merge-sort, this will work for sort + no aggregation and sort + aggregation.
  //as for no aggregation, start_position and end_position will be identical.
  for (size_t kt =0; kt< mergedResultHolder.keyTracker; kt++) {
    ByteArrayKeyWithVariableLength::MergeSortedKeyTracker currentKeyTracker = mergedResultHolder.keys[kt];
    PositionInExtensibleByteBuffer key = currentKeyTracker.cachedKeyValue;
    result.keys.push_back(key);
    size_t start_position = currentKeyTracker.start_position;
    size_t end_position  = currentKeyTracker.end_position;

    vector <PositionInExtensibleByteBuffer> valuesForKey;
    vector<int> valueSizesForKey;

    for (size_t pt =start_position; pt < end_position; pt++) {
      PositionInExtensibleByteBuffer pBuffer = mergedResultHolder.kvaluesGroups[pt].cachedValueValue;
      valuesForKey.push_back(pBuffer);
      valueSizesForKey.push_back(pBuffer.value_size);
    }

    result.kvaluesGroups.push_back(valuesForKey);
    result.kvaluesGroupSizes.push_back(valueSizesForKey);
  }


  //NOTE: we will need to move shutdown of merge-sort engine into somewhere else. 
  //at this time, merge-sort engine uses malloc to hold the returned Value's value.
  //mergeSortEngine.shutdown(); 

  return result; 
}

//this is for the real key/value retrieval, with the maximum number of keys retrieved to be specified
//an empty holder is created first, and then gets filled in.
int ReduceShuffleStoreWithByteArrayKey::retrieve_mergesortedmapbuckets (int max_number) {

  //We should not reset the buffer manager any time to get the next batch of the key-values, as the merge-sort network
  //is not empty yet, and the pending elements in the merge-sort network rely on the buffer manager to store the values
  //resetting it invalidates the pending elements in the network.
  //this buffer reset has to go before init_mergesort_engine, as it uses buffer manager to retrieve the first element.
 
  //theMergeSortEngine.reset_buffermgr();

  if (!engineInitialized) {
    if (orderingRequired) {
       init_mergesort_engine();
    }
    else if (aggregationRequired) {
       init_hashmap_engine();
    }
    else {
      //runtime assertion                                                                                                             
      CHECK(orderingRequired || aggregationRequired)
	<< "retrieval of mergesortmapbucket requires flag of ordering/aggregation to be on";
    }
  }

  int numberOfRetrievedKeys=0; 

  if (orderingRequired) {
    VLOG(2) << "**ordering required. choose merge-sort engine"<<endl;
    //make sure that the holder is really activated. 
    CHECK(mergedResultHolder.isActivated());

    if (aggregationRequired) {
      LOG(INFO) << "merge-sort engine choose ordering-and-aggregation path";

      while (theMergeSortEngine.hasNext()) {
           //note that, each time, the merged result holder will be reset.
	  theMergeSortEngine.getNextKeyValuesPair(mergedResultHolder);

          if (VLOG_IS_ON(2)) {
            vector<PositionInExtensibleByteBuffer> retrieved_values;
            //NOTE: once the key is added, the key tracker moves to the next position.
            size_t keyValueTracker = mergedResultHolder.keyTracker -1; 
            size_t start_position =  mergedResultHolder.keys[keyValueTracker].start_position;
            size_t end_position =    mergedResultHolder.keys[keyValueTracker].end_position;
            //size_t positionBufferTracker = mergedResultHolder.positionBufferTracker;

	    PositionInExtensibleByteBuffer keyValue = theMergeSortEngine.getCurrentMergedKey();
            VLOG(2) << "**retrieve mergesort mapbuckets**" << " byte-array key size is: "
                    << keyValue.value_size;
            VLOG(2) << "**retrieve mergesort mapbuckets**" << " start position is: " << start_position
                    << " end position is" << end_position;

            //if the key does not have any values to be associated with, start_position is the same as end_position.
            for (size_t p = start_position; p <end_position; p++) {
	      retrieved_values.push_back(mergedResultHolder.kvaluesGroups[p].cachedValueValue);
	    }

            VLOG(2) << "**retrieve mergesort mapbuckets**" << " value size is: " << retrieved_values.size();
	  }

	  //retrieved values are already reflected in the merged result holder already.
	  //resultHolder.kvaluesGroups.push_back(retrieved_values);

          numberOfRetrievedKeys++;
          if (numberOfRetrievedKeys == max_number) {
	     break;
	  }
      }
    }
    else {
      LOG(INFO) << "merge-sort engine choose ordering-no-aggregation path";

      while (theMergeSortEngine.hasNext()) {
           //note that, each time, the merged result holder will be reset.
	  theMergeSortEngine.getNextKeyValuePair(mergedResultHolder);

          if (VLOG_IS_ON(2)) {
            vector<PositionInExtensibleByteBuffer> retrieved_values;
            //NOTE: once the key is added, the key tracker moves to the next position.
            size_t keyValueTracker = mergedResultHolder.keyTracker -1; 
            size_t start_position =  mergedResultHolder.keys[keyValueTracker].start_position;
            size_t end_position =    mergedResultHolder.keys[keyValueTracker].end_position;
            //size_t positionBufferTracker = mergedResultHolder.positionBufferTracker;

	    PositionInExtensibleByteBuffer keyValue = theMergeSortEngine.getCurrentMergedKey();
            VLOG(2) << "**retrieve mergesort mapbuckets**" << " byte-array key size is: "
                    << keyValue.value_size;
            VLOG(2) << "**retrieve mergesort mapbuckets**" << " start position is: " << start_position
                    << " end position is" << end_position;

            //Note, in this situation, start_position and end_position should be identical, as no
            //aggregation for the same key is involved.
            for (size_t p = start_position; p <end_position; p++) {
	      retrieved_values.push_back(mergedResultHolder.kvaluesGroups[p].cachedValueValue);
	    }

            VLOG(2) << "**retrieve mergesort mapbuckets**" << " value size is: " << retrieved_values.size();
	  }

	  //retrieved values are already reflected in the merged result holder already.
	  //resultHolder.kvaluesGroups.push_back(retrieved_values);

          numberOfRetrievedKeys++;
          if (numberOfRetrievedKeys == max_number) {
	     break;
	  }
      }//end while
    }//end ordering-no-aggregation
  }
  else if (aggregationRequired){
     VLOG(2) << "**aggregation required. choose hash-map engine"<<endl; 
     //make sure that the holder is really activated. 
     CHECK(mergedResultHolder.isActivated());

     while (theHashMapEngine.hasNext()) {
           //note that, each time, the merged result holder will be reset.
	  theHashMapEngine.getNextKeyValuesPair(mergedResultHolder);

          if (VLOG_IS_ON(2)) {
            vector<PositionInExtensibleByteBuffer> retrieved_values;
            //NOTE: once the key is added, the key tracker moves to the next position.
            size_t keyValueTracker = mergedResultHolder.keyTracker -1; 
            size_t start_position =  mergedResultHolder.keys[keyValueTracker].start_position;
            size_t end_position =    mergedResultHolder.keys[keyValueTracker].end_position;
            //size_t positionBufferTracker = mergedResultHolder.positionBufferTracker;

	    PositionInExtensibleByteBuffer keyValue = theHashMapEngine.getCurrentMergedKey();
            VLOG(2) << "**retrieve hash-map mapbuckets**" << " byte-array key size is: "
                    << keyValue.value_size;
            VLOG(2) << "**retrieve hash-map mapbuckets**" << " start position is: " << start_position
                    << " end position is" << end_position;

            //if the key does not have any values to be associated with, start_position is the same as end_position.
            for (size_t p = start_position; p <end_position; p++) {
	      retrieved_values.push_back(mergedResultHolder.kvaluesGroups[p].cachedValueValue);
	    }

            VLOG(2) << "**retrieve hahs-map mapbuckets**" << " value size is: " << retrieved_values.size();
	  }

	  //retrieved values are already reflected in the merged result holder already.
	  //resultHolder.kvaluesGroups.push_back(retrieved_values);

          numberOfRetrievedKeys++;
          if (numberOfRetrievedKeys == max_number) {
	     break;
	  }
    }
  }
  else {
       //runtime assertion 
       CHECK(orderingRequired || aggregationRequired) 
             << "retrieval of mergesortmapbucket requires flag of ordering/aggregation to be on";
  }

  return numberOfRetrievedKeys;

  //NOTE: we will need to move shutdown of merge-sort engine into somewhere else. 
  //at this time, merge-sort engine uses malloc to hold the returned Value's value.
  //mergeSortEngine.shutdown(); 
}


int ReduceShuffleStoreWithByteArrayKey::retrieve_passthroughmapbuckets (int max_number){
  if (!engineInitialized) {
    if ( !(orderingRequired || aggregationRequired)) {
       init_passthrough_engine();
    }
  }
     
  int numberOfRetrievedKeys=0; 
  if (!(orderingRequired || aggregationRequired)) {
    VLOG(2) << "**no ordering/aggregation required. choose pass-through engine"<<endl; 
     
    CHECK(passThroughResultHolder.isActivated());

    while (thePassThroughEngine.hasNext()) {
           //note that, each time, the merged result holder will be reset.
	  thePassThroughEngine.getNextKeyValuePair(passThroughResultHolder);

          if (VLOG_IS_ON(2)) {
            //NOTE: once the key is added, the key tracker moves to the next position.
            size_t keyValueOffsetTracker = passThroughResultHolder.keyValueOffsetTracker -1; 
            int start_keyoffset  =  passThroughResultHolder.keyAndValueOffsets[keyValueOffsetTracker].start_keyoffset;
            int end_keyoffset  =  passThroughResultHolder.keyAndValueOffsets[keyValueOffsetTracker].end_keyoffset;

	    unsigned char *keyPtr = thePassThroughEngine.getCurrentKeyPtr();
	    int keySize = thePassThroughEngine.getCurrentKeySize();

            VLOG(2) << "**retrieve pass-through mapbuckets**" << " start key offset is: " << start_keyoffset;
            VLOG(2) << "**retrieve pass-through mapbuckets**" << " end key offset is: " << end_keyoffset;
            VLOG(2) << "**retrieve pass-through mapbuckets**" << " key pointer is: " << (void*)keyPtr;
            VLOG(2) << "**retrieve pass-through mapbuckets**" << " key size is: " << keySize <<endl;

	  }

	  //retrieved values are already reflected in the merged result holder already.
	  //resultHolder.kvaluesGroups.push_back(retrieved_values);
          numberOfRetrievedKeys++;
          if (numberOfRetrievedKeys == max_number) {
	     break;
	  }
     }
  }
  else {
     //runtime assertion 
     CHECK(!(orderingRequired || aggregationRequired)) 
             << "retrieval of pass-through mapbucket requires no flag of ordering/aggregation to be on";
  }

  return numberOfRetrievedKeys;
}

void ReduceShuffleStoreWithByteArrayKey::stop(){

  if (!isStopped) {
    //buffer manager is about DRAM. it will be cleaned up when the shuffle store is stoped.
    if (kBufferMgr != nullptr) {
       kBufferMgr->free_buffers();
       delete kBufferMgr; 
       kBufferMgr = nullptr;
    }

    if (vBufferMgr != nullptr) {
       vBufferMgr->free_buffers();
       delete vBufferMgr; 
       vBufferMgr = nullptr;
    }

    VLOG(2) << "**reduce shuffle store buffer manager deleted and assigned to nullptr "; 

    //The merge result holder needs to be free. 
    //NOTE: this will be put into the buffer pool later.
    mergedResultHolder.release();
    VLOG(2) << "**mergedResultHolder released " <<endl; 
    passThroughResultHolder.release();
    VLOG(2) << "**passThroughResultHolder released " <<endl; 

    theMergeSortEngine.shutdown();
    VLOG(2) << "**reduce shuffle store associated merge-sort engine shutdown " <<endl; 
    theHashMapEngine.shutdown();
    VLOG(2) << "**reduce shuffle store associated hash map engine shutdown " <<endl; 
    thePassThroughEngine.shutdown();
    VLOG(2) << "**reduce shuffle store associated pass-through engine shutdown " <<endl; 

    isStopped = true;

    LOG(INFO) << "**reduce shuffle store with byte-array keys " 
            << " with id: " << reducerId << " stopped"
            << " buffer pool size: " 
            << ShuffleStoreManager::getInstance()->getByteBufferPool()->currentSize() <<endl;
  }
  else {
    LOG(INFO) << "**reduce shuffle store with byte-array keys "
              << " with id: " << reducerId << " already stopped"  <<endl;
    
  }

}

void ReduceShuffleStoreWithByteArrayKey::shutdown(){
  LOG(INFO) << "**reduce shuffle store with byte-array keys " 
            << " with reduce id: " << reducerId << " is shutting down" <<endl; 
   //NOTE: we can not shutdown after retrieve_mergesortmapbuckets. as otherwise, 
   //it will release all of the allocated memory for the retrieved values. 

}
