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

#ifndef REDUCESHUFFLESTORE_WITH_LONG_KEYS_H_
#define REDUCESHUFFLESTORE_WITH_LONG_KEYS_H_

#include <vector>
#include "ShuffleConstants.h"
#include "ExtensibleByteBuffers.h"
#include "MergeSortReduceChannelWithLongKeys.h"
#include "HashMapReduceChannelWithLongKeys.h"
#include "MapStatus.h"
#include "GenericReduceShuffleStore.h"
#include "MergeSortKeyPositionTrackerWithLongKeys.h"
#include "PassThroughKeyValueTrackerWithLongKeys.h"
#include "PassThroughReduceChannelWithLongKeys.h"

using namespace std;

class  ReduceShuffleStoreWithLongKey: public GenericReduceShuffleStore {
 private:
     //Note: this becomes a local copy.
     ReduceStatus reduceStatus; 
     int totalNumberOfPartitions; 
     int reducerId; 
 
     //The buffer manager, but with type of ExtensibleByteBuffers. Just like map-side shuffle store.
     ExtensibleByteBuffers *bufferMgr; 
     //engine 1: merge-sort basd;
     MergeSortReduceEngineWithLongKeys theMergeSortEngine;
     //engine 2: hash-map based.
     HashMapReduceEngineWithLongKeys theHashMapEngine;
     //engine 3: pass-through based
     PassThroughReduceEngineWithLongKeys thePassThroughEngine;

     bool engineInitialized;
     
     //for key value type definition 
     KValueTypeDefinition kvTypeDefinition;

     //value type definition
     VValueTypeDefinition vvTypeDefinition;

     //merge-sorted result holder.
     //NOTE: please consider this to be re-usable buffer (Jun Li, 9/7/2015)
     //NOTE: this mergeResultHolder is for sort/merge or hash-aggregation. Not for pass-through
     LongKeyWithFixedLength::MergeSortedMapBuckets mergedResultHolder;
    
     //NOTE: this passThroughResultHolder is for pass-through operator(s) only
     LongKeyWithFixedLength::PassThroughMapBuckets passThroughResultHolder;
    
     //to specify whether the reduce-side needs key ordering or not:
     //if key ordering is required, we will use merge-sort to merge sorted data from the map side.
     //otherwise, we will use hash map based merge without taking into account ordering
     bool orderingRequired;
     //to specify whether the reduce-side needs aggregation or not:
     //if aggregation is required, we will use hash map based merge. otherwise, if no ordering is required,
     //and no aggregation is required, we will use direct pass-through without any merging.
     bool aggregationRequired;

     //to track whether the "stop" command has been arealdy fullfilled.
     bool isStopped; 

 public: 

	 ReduceShuffleStoreWithLongKey(const ReduceStatus &status,
				      int partitions, int redId, ExtensibleByteBuffers *bMgr,
                                      unsigned char *passThroughBuffer, size_t buf_capacity,
                                      bool ordering, bool aggregation) :
		 reduceStatus(status), totalNumberOfPartitions(partitions), reducerId(redId),
		 bufferMgr(bMgr),
		 theMergeSortEngine(redId, partitions, bMgr),
		 theHashMapEngine(redId, partitions, bMgr),
		 thePassThroughEngine(redId, partitions),
		 engineInitialized (false), kvTypeDefinition(KValueTypeId::Long),
		 mergedResultHolder(redId, ordering, aggregation),
		 passThroughResultHolder (redId, ordering, aggregation, passThroughBuffer, buf_capacity),  
		 orderingRequired (ordering),
		 aggregationRequired(aggregation),
		 isStopped(false) {

	 }

	 //Item 7: declare a virtual destructor if and only if the class contains at least one virtual function
         virtual ~ReduceShuffleStoreWithLongKey () {
           //do nothing
         }
    
         //retrieve reducer id
	 int getReducerId() {
	    return reducerId;
	 }

	 //for testing purpose 
	 ReduceStatus& getReduceStatus() {
		return reduceStatus; 
	 }

	 //to expose the buffer manager. 
	 ExtensibleByteBuffers * getBufferMgr() {
		return bufferMgr; 
	 }
         
         //to expose the final merged result for sort/merge and hash-aggregation
         LongKeyWithFixedLength::MergeSortedMapBuckets& getMergedResultHolder () {
	      return mergedResultHolder;
	 }

         //to expose the final merge result for pass-throgh opeartor(s)
         LongKeyWithFixedLength::PassThroughMapBuckets& getPassThroughResultHolder () {
	     return passThroughResultHolder;
	 }


         //to reset the merge result's position pointer positions.
         void reset_mergedresult(){
     	     mergedResultHolder.reset();
	 }

	 //to reset the pass-throgh result's position pointer
         void reset_passthroughresult(){
     	     passThroughResultHolder.reset();
	 }

         MergeSortReduceEngineWithLongKeys& getMergeSortEngine() {
	    return theMergeSortEngine;  
	 }
      
         void setVVTypeDefinition(const VValueTypeDefinition &def){
 	       //value copy
	     vvTypeDefinition = def;
	 }

         VValueTypeDefinition getVValueType() override {
	     return vvTypeDefinition;
	 }

         KValueTypeDefinition getKValueType() override {
	    return kvTypeDefinition;
	 }

         bool needsOrdering() override {
	    return orderingRequired;
	 }

         bool needsAggregation() override {
	   return aggregationRequired;
	 }

        //for testing purpose.to retrieve only the bucket contributed from the specified mapId; 
	LongKeyWithFixedLength::RetrievedMapBucket retrieve_mapbucket(int mapId); 
        void free_retrieved_mapbucket(LongKeyWithFixedLength::MergeSortedMapBucketsForTesting &mapBucket);

        //for testing purpose. to retrieve all of the map buckets and aggregate the key with 
        //the values, for all of the buckets that belong to the same reducer id. 
	LongKeyWithFixedLength::MergeSortedMapBucketsForTesting retrieve_mergesortedmapbuckets () ;


        //for real key/value pair retrieval based on merge-sort 
        void init_mergesort_engine();
        //for real key/value pair retrieval based on hash-map
        void init_hashmap_engine();
        //for real key/value pair retrieval via direct pass-through
        void init_passthrough_engine();

	//for real key/value pair retrieval, either via sort/merge or via hash-aggregation,
        // with the specified number of keys to be retrieved at a given time.
        //return the actual number of k-vs obtained.
        int retrieve_mergesortedmapbuckets (int max_number);
        //for real key/value pair retrieval, via direct pass-through
        int retrieve_passthroughmapbuckets (int max_number);

        //NOTE: who will issue the clean up of the intermediate results. It seems 
        //that is when the last kv pair gets pull out. 
        void stop() override;

        //NOTE: do we have shutdown?
        void shutdown() override; 
};

#endif /*REDUCESHUFFLESTORE_WITH_LONG_KEYS_H_*/
