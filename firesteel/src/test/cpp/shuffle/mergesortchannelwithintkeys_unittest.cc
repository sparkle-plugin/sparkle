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

#include "SimpleUtils.h"
#include "MapShuffleStoreWithIntKeys.h"
#include "ReduceShuffleStoreWithIntKeys.h"
#include "MergeSortReduceChannelWithIntKeys.h"

#include <iostream> 

#include "gtest/gtest.h"
#include "glog/logging.h"
#include <unordered_map>

using namespace std;


namespace MERGESORT_CHANNEL_WITH_INTKEYS_TEST {
	struct KeyValuePartitionTuple3 {
		int keyValue;
		unsigned char *valueValue;
		int valueSize;
		int partition;

		KeyValuePartitionTuple3(int kValue, unsigned char* vV, int vSize, int part) :
			keyValue(kValue), valueValue(vV), valueSize(vSize), partition(part) {

		}
	};

	struct OneMapStoreBatch {
		vector <KeyValuePartitionTuple3> keyValuePairs;

		OneMapStoreBatch() {

		}
	};

	struct OneMapStore {
		vector <OneMapStoreBatch> batches;
		int mapId;
		OneMapStore(int mId) : mapId(mId) {

		}
	};
};

using namespace MERGESORT_CHANNEL_WITH_INTKEYS_TEST;


//to test one map, two keys, to see whether 
//void Test_MergeSortChannel_SingleMapWithTwoKeys_InOnePartition() {

TEST(MergeSortChannel,SingleMapWithTwoKeys_InOnePartition) {

	// Seed with a real random value, if available
	Rand_int randomGenerator{ 0, 120 };

	int buffer_size = 1 * 1024;

	//int payload_size1 = (int)buffer_size * 0.8;
	int payload_size1 = 10; 
	//int payload_size2 = (int)buffer_size *2.8;
	int payload_size2 = 12; 

	unsigned char *buffer1 = (unsigned char*)malloc(sizeof (unsigned char)*payload_size1);
	//populate randome number to it 
	for (int i = 0; i<payload_size1; i++) {
		buffer1[i] = (unsigned char)randomGenerator();
		cout << " test case: populated for buffer 1: " << (int)buffer1[i] << endl;
	}

	unsigned char *buffer2 = (unsigned char*)malloc(sizeof (unsigned char)*payload_size2);
	//populate randome number to it 
	for (int i = 0; i<payload_size2; i++) {
		buffer2[i] = (unsigned char)randomGenerator();
		cout << " test case: populated for buffer 2: " << (int)buffer2[i] << endl;
	}

	unsigned char *holder = (unsigned char*)malloc(sizeof(unsigned char)* (payload_size1 + payload_size2));

	int voffsets[2] = { payload_size1, payload_size1 + payload_size2 };
	memcpy(holder, buffer1, payload_size1);

	//NOTE: I can not update the holder, as I need this for the following computation that holder has to be resetted. 
	unsigned char *pholder = holder + payload_size1;
	memcpy(pholder, buffer2, payload_size2);

	int kvalues[2] = { 145, 37 };
	int partitions[2] = { 0, 0 }; //only gets into one single partition.  

	int numberOfPairs = 2;
	int totalPartitions = 2;

	//map id =0 only produces reduce id = 1's key value pairs. 
	int MAP_ID = 0;
	MapShuffleStoreWithIntKey shuffleStore(buffer_size, MAP_ID);

	for (int i = 0; i < payload_size1 + payload_size2; i++) {
		unsigned char element = holder[i];
		cout << "after initial memcopy, total buffer at position: " << i << " with value: " << (int)element << endl;
	}

        //introduced value type definition, thus, before store key/value pairs, store the value type definition
	int valueTypeDefinitionLength = 99;
	unsigned char valueTypeDefinition[1024];
	for (int kp=0; kp<valueTypeDefinitionLength; kp++) {
	  valueTypeDefinition[kp] = (unsigned char) randomGenerator();
	}
	shuffleStore.setValueType(valueTypeDefinition, valueTypeDefinitionLength);

	//NOTE: the passed-in buffer has to be the one that is resetted to be the beginning!!
	shuffleStore.storeKVPairsWithIntKeys(holder, voffsets, kvalues, partitions, numberOfPairs);

	//need to inspect the buffer manager at this time. 
	ExtensibleByteBuffers &bufferMgr = shuffleStore.getBufferMgr();
	int current_buffer_position = bufferMgr.current_buffer_position();

	//values go beyond two buffers; 
	//assert(current_buffer_position == 0);

	//display the result of current buffer
	ByteBufferHolder& firstBuffer = bufferMgr.buffer_at(current_buffer_position);
	int position_in_current_buffer = firstBuffer.position_in_buffer();
	for (int i = 0; i < position_in_current_buffer; i++) {
		unsigned char element = firstBuffer.value_at(i);
		cout << "bytes stored in buffer manager for buffer: " << current_buffer_position << " at position: " << i
			<< " is: " << (int)element << endl;
	}


	shuffleStore.sort(totalPartitions);

	//check the buffer manager's populated data 

	//I only have one mapper for contribution. 
	MapStatus mapStatus = shuffleStore.writeShuffleData();
	EXPECT_NE (mapStatus.offsetToIndexBucket, 0);
	EXPECT_EQ (mapStatus.totalNumberOfPartitions, totalPartitions);
	EXPECT_EQ (mapStatus.bucketSizes.size(), 2);

	//need to create the reduce side buffer manager
	ExtensibleByteBuffers reduceSideBufferManager(buffer_size);

	{
		int reduceId = 0;
		//I only have one map bucket.
		MapBucket mapBucket(reduceId, mapStatus.bucketSizes[reduceId],
			mapStatus.shmRegionName, mapStatus.offsetToIndexBucket, mapStatus.mapId);

		MergeSortReduceChannelWithIntKeys mergeSortReduceChannelWithIntKeys(mapBucket, 
                     reduceId, totalPartitions, &reduceSideBufferManager);

		mergeSortReduceChannelWithIntKeys.init();

		bool hasNext = mergeSortReduceChannelWithIntKeys.hasNext();

		EXPECT_EQ (hasNext,  true);

		mergeSortReduceChannelWithIntKeys.getNextKeyValuePair(); 
		int key1 = mergeSortReduceChannelWithIntKeys.getCurrentKeyValue();
		PositionInExtensibleByteBuffer retrieved_value1 = mergeSortReduceChannelWithIntKeys.getCurrentValueValue();
		int retrieved_value1_size = mergeSortReduceChannelWithIntKeys.getCurrentValueSize();

		EXPECT_EQ (key1, 37);
		EXPECT_EQ (retrieved_value1_size, 12);

		//do hash comparision
		size_t hash_result_original_1 = HashUtils::hash_compute(buffer2, payload_size2);
		unsigned char *retrieved_value1_buffer = (unsigned char*)malloc(retrieved_value1.value_size);
		reduceSideBufferManager.retrieve(retrieved_value1, retrieved_value1_buffer);
		size_t hash_result_retrieved_1 = HashUtils::hash_compute(retrieved_value1_buffer, retrieved_value1_size);


		cout << "for reduce id " << reduceId << " for key: " << key1 
                        << "  hash result original is: " << hash_result_original_1
			<< " and hash result retrieved is: " << hash_result_retrieved_1 << endl;

		EXPECT_EQ (hash_result_original_1, hash_result_retrieved_1);
		hasNext = mergeSortReduceChannelWithIntKeys.hasNext();

                EXPECT_EQ (hasNext, true);

		mergeSortReduceChannelWithIntKeys.getNextKeyValuePair();
		int key2 = mergeSortReduceChannelWithIntKeys.getCurrentKeyValue();
		PositionInExtensibleByteBuffer retrieved_value2 = mergeSortReduceChannelWithIntKeys.getCurrentValueValue();
		int retrieved_value2_size = mergeSortReduceChannelWithIntKeys.getCurrentValueSize();

		EXPECT_EQ (key2, 145);
		EXPECT_EQ (retrieved_value2_size, 10);

		//do hash comparision
		size_t hash_result_original_2 = HashUtils::hash_compute(buffer1, payload_size1);
		unsigned char *retrieved_value2_buffer = (unsigned char*)malloc(retrieved_value2.value_size);
		reduceSideBufferManager.retrieve(retrieved_value2, retrieved_value2_buffer);
		size_t hash_result_retrieved_2 = HashUtils::hash_compute(retrieved_value2_buffer, retrieved_value2_size);


		cout << "for reduce id " << reduceId << " for key: " << key2 
                     << "  hash result original is: " << hash_result_original_2
		     << " and hash result retrieved is: " << hash_result_retrieved_2 << endl;

		EXPECT_EQ (hash_result_original_2, hash_result_retrieved_2);
	}

}

//void Test_MergeSortEngine_SingleMapWithTwoKeys_InOnePartition() {
TEST(MergeSortEngine, SingleMapWithTwoKeys_InOnePartition) {

	// Seed with a real random value, if available
	Rand_int randomGenerator{ 0, 120 };

	int buffer_size = 1 * 1024;

	//int payload_size1 = (int)buffer_size * 0.8;
	int payload_size1 = 10;
	//int payload_size2 = (int)buffer_size *2.8;
	int payload_size2 = 12;

	unsigned char *buffer1 = (unsigned char*)malloc(sizeof (unsigned char)*payload_size1);
	//populate randome number to it 
	for (int i = 0; i<payload_size1; i++) {
		buffer1[i] = (unsigned char)randomGenerator();
		cout << " test case: populated for buffer 1: " << (int)buffer1[i] << endl;
	}

	unsigned char *buffer2 = (unsigned char*)malloc(sizeof (unsigned char)*payload_size2);
	//populate randome number to it 
	for (int i = 0; i<payload_size2; i++) {
		buffer2[i] = (unsigned char)randomGenerator();
		cout << " test case: populated for buffer 2: " << (int)buffer2[i] << endl;
	}

	unsigned char *holder = (unsigned char*)malloc(sizeof(unsigned char)* (payload_size1 + payload_size2));

	int voffsets[2] = { payload_size1, payload_size1 + payload_size2 };
	memcpy(holder, buffer1, payload_size1);

	//NOTE: I can not update the holder, as I need this for the following computation that holder has to be resetted. 
	unsigned char *pholder = holder + payload_size1;
	memcpy(pholder, buffer2, payload_size2);

	int kvalues[2] = { 145, 37 };
	int partitions[2] = { 0, 0 }; //only gets into one single partition.  

	int numberOfPairs = 2;
	int totalPartitions = 2;

	//map id =0 only produces reduce id = 1's key value pairs. 
	int MAP_ID = 0;
	MapShuffleStoreWithIntKey shuffleStore(buffer_size, MAP_ID);

	for (int i = 0; i < payload_size1 + payload_size2; i++) {
		unsigned char element = holder[i];
		cout << "after initial memcopy, total buffer at position: " << i << " with value: " << (int)element << endl;
	}


        //introduced value type definition, thus, before store key/value pairs, store the value type definition
	int valueTypeDefinitionLength = 99;
	unsigned char valueTypeDefinition[1024];
	for (int kp=0; kp<valueTypeDefinitionLength; kp++) {
	  valueTypeDefinition[kp] = (unsigned char) randomGenerator();
	}
	shuffleStore.setValueType(valueTypeDefinition, valueTypeDefinitionLength);

	//NOTE: the passed-in buffer has to be the one that is resetted to be the beginning!!
	shuffleStore.storeKVPairsWithIntKeys(holder, voffsets, kvalues, partitions, numberOfPairs);

	//need to inspect the buffer manager at this time. 
	ExtensibleByteBuffers &bufferMgr = shuffleStore.getBufferMgr();
	int current_buffer_position = bufferMgr.current_buffer_position();

	//values go beyond two buffers; 
	//assert(current_buffer_position == 0);

	//display the result of current buffer
	ByteBufferHolder& firstBuffer = bufferMgr.buffer_at(current_buffer_position);
	int position_in_current_buffer = firstBuffer.position_in_buffer();
	for (int i = 0; i < position_in_current_buffer; i++) {
		unsigned char element = firstBuffer.value_at(i);
		cout << "bytes stored in buffer manager for buffer: " << current_buffer_position << " at position: " << i
			<< " is: " << (int)element << endl;
	}


	shuffleStore.sort(totalPartitions);

	//check the buffer manager's populated data 

	//I only have one mapper for contribution. 
	MapStatus mapStatus = shuffleStore.writeShuffleData();

	EXPECT_NE (mapStatus.offsetToIndexBucket, 0);
	EXPECT_EQ (mapStatus.totalNumberOfPartitions, totalPartitions);
	EXPECT_EQ (mapStatus.bucketSizes.size(), 2);

	ExtensibleByteBuffers reduceSideBufferManager(buffer_size);

	{
		int reduceId = 0;
		//I only have one map bucket.
		MapBucket mapBucket(reduceId, mapStatus.bucketSizes[reduceId],
			mapStatus.shmRegionName, mapStatus.offsetToIndexBucket, mapStatus.mapId);

		MergeSortReduceEngineWithIntKeys mergeSortReduceEngineWithIntKeys(reduceId, 
                       totalPartitions, &reduceSideBufferManager);
		mergeSortReduceEngineWithIntKeys.addMergeSortReduceChannel(mapBucket);
		mergeSortReduceEngineWithIntKeys.init();

		//first key after the merge sort. 
		bool hasNext = mergeSortReduceEngineWithIntKeys.hasNext();

		EXPECT_EQ (hasNext, true);

		mergeSortReduceEngineWithIntKeys.getNextKeyValuesPair();
		int key1 = mergeSortReduceEngineWithIntKeys.getCurrentMergedKey();

	        EXPECT_EQ (key1, 37);

		vector <PositionInExtensibleByteBuffer> & firstValues = mergeSortReduceEngineWithIntKeys.getCurrentMergeValues();
		vector <int> & firstValuesSizes = mergeSortReduceEngineWithIntKeys.getCurrentMergeValueSizes();

		PositionInExtensibleByteBuffer firstValue = firstValues[0];
		int firstValueSize = firstValuesSizes[0];

		EXPECT_EQ (firstValues.size(), 1);
		EXPECT_EQ (firstValuesSizes.size(), 1);
		EXPECT_EQ (firstValueSize, 12);

		size_t hash_result_original_1 = HashUtils::hash_compute(buffer2, payload_size2);
		unsigned char *firstValue_buffer = (unsigned char*)malloc(firstValue.value_size);
		reduceSideBufferManager.retrieve(firstValue, firstValue_buffer);

		size_t hash_result_retrieved_1 = HashUtils::hash_compute(firstValue_buffer, firstValueSize);


		cout << "for reduce id " << reduceId << " for key: " << key1 
                     << "  hash result original is: " << hash_result_original_1
		     << " and hash result retrieved is: " << hash_result_retrieved_1 << endl;

	        EXPECT_EQ (hash_result_original_1,  hash_result_retrieved_1);

		//second key after the merge sort. 
		hasNext = mergeSortReduceEngineWithIntKeys.hasNext();

                EXPECT_EQ (hasNext, true);

		mergeSortReduceEngineWithIntKeys.getNextKeyValuesPair();
		int key2 = mergeSortReduceEngineWithIntKeys.getCurrentMergedKey();

		vector <PositionInExtensibleByteBuffer> & secondValues = mergeSortReduceEngineWithIntKeys.getCurrentMergeValues();
		vector <int> & secondValuesSizes = mergeSortReduceEngineWithIntKeys.getCurrentMergeValueSizes();

		PositionInExtensibleByteBuffer secondValue = secondValues[0];
		int secondValueSize = secondValuesSizes[0];

		EXPECT_EQ (key2, 145);
		EXPECT_EQ (secondValueSize,  10);
		EXPECT_EQ (secondValues.size(), 1);
		EXPECT_EQ (secondValuesSizes.size(), 1);

		size_t hash_result_original_2 = HashUtils::hash_compute(buffer1, payload_size1);
		unsigned char *secondValue_buffer = (unsigned char*)malloc(secondValue.value_size);
		reduceSideBufferManager.retrieve(secondValue, secondValue_buffer);
		size_t hash_result_retrieved_2 = HashUtils::hash_compute(secondValue_buffer, secondValueSize);

		cout << "for reduce id " << reduceId << " for key: " << key2 
                     << "  hash result original is: " << hash_result_original_2
		     << " and hash result retrieved is: " << hash_result_retrieved_2 << endl;

		EXPECT_EQ (hash_result_original_2, hash_result_retrieved_2);

		hasNext = mergeSortReduceEngineWithIntKeys.hasNext();

		EXPECT_EQ (hasNext, false);
	}

}

//a hash table represented by the key and the hash of value. so that collision is almost impossible.
struct MergeSortTestStoreIdentity {
	int kvalue;
	size_t compuatedHashForValue;

	MergeSortTestStoreIdentity(int k, int h) : kvalue(k), compuatedHashForValue (h) {
	};
};

struct MergeSortTestStoreIdentity_hash {
	size_t operator()  (const MergeSortTestStoreIdentity & identity) const
	{
		const int prime = 31;
		int hash = 1;
		hash = prime *hash + identity.kvalue;
		hash = prime * hash + identity.compuatedHashForValue;
		return hash;
	}
};

struct MergeSortTestStoreIdentity_equal {
	bool operator() (const MergeSortTestStoreIdentity  &identity1, const MergeSortTestStoreIdentity  &identity2) const {
		if ((identity1.kvalue == identity2.kvalue)
			&& (identity1.compuatedHashForValue == identity2.compuatedHashForValue)) {
			return true;
		}
		else {
			return false;
		}

	}
};


//multiple keys, values, and multiple maps, but with the parition going through hash key. therefore, some of the reducer id may have empty collection. increased partition number.
//void Test_MergeSortEngine_MultipleKeysValuesPartitionsWriteAndRead_With_MultipleBuffers_WITH_HASHPARTITIONS() {
TEST(MergeSortEngine, MultipleKeysValuesPartitionsWriteAndRead_With_MultipleBuffers_WITH_HASHPARTITIONS) {

	unordered_map< MergeSortTestStoreIdentity, unsigned char *,
		MergeSortTestStoreIdentity_hash, MergeSortTestStoreIdentity_equal>  populatedKeyValueStore;


	int buffer_size = 1 * 1024;
	// Seed with a real random value, if available
	Rand_int charValueRandomGenerator{ 0, 120 };
        //WARNING: I have to change the size from 10 to 20 or 11, in order to not to get to the collsion.
        //fixed!!. This is the reference problem in ExtensibleByteBuffers.cc
	Rand_int payloadSizeRandomGenerator{ 1, 10 * buffer_size };
	Rand_int  kvalueRandomGenerator{ 0, 100000 };


	int EACH_BATCH_SIZE = 20;
	int TOTAL_NUMBER_OF_BATCH_IN_EACH_MAP = 20;
	int TOTAL_NUMBER_OF_MAPSTORES = 20;
	//do we need to have the number of map partitions to be the same as the number of the reducer partitions
	//or the reducer partition can be different from the map partition (I think so!) 
	int TOTAL_NUMBER_PARTITIONS = 7; //so that some of the reducer has only empty partitions. 
	int KEY_TO_BE_MODULATED = 17; //so that the key will good chance to collide. 

	vector <MERGESORT_CHANNEL_WITH_INTKEYS_TEST::OneMapStore> ALL_MAP_STORES;
	vector <MapShuffleStoreWithIntKey> ALL_CREATED_MAPSHUFFLE_STORES;
	vector <MapStatus>  ALL_MAPSHUFFLE_STORES_MAPSTATUSES;
	//in each map store, we have many batches;



	for (int mapStoreIndex = 0; mapStoreIndex < TOTAL_NUMBER_OF_MAPSTORES; mapStoreIndex++) {
		int MAP_ID = mapStoreIndex;

		MapShuffleStoreWithIntKey shuffleStore(buffer_size, MAP_ID);


		MERGESORT_CHANNEL_WITH_INTKEYS_TEST::OneMapStore oneMapStore(MAP_ID);
		//can not store like this one, as this is value copied, not reference pushing. 
		//ALL_MAP_STORES.push_back(oneMapStore);

		for (int b = 0; b < TOTAL_NUMBER_OF_BATCH_IN_EACH_MAP; b++) {
			MERGESORT_CHANNEL_WITH_INTKEYS_TEST::OneMapStoreBatch oneBatch;
			//you can not do this way. It is value copied. not reference pushing
			//oneMapStore.batches.push_back(oneBatch);

			for (int i = 0; i < EACH_BATCH_SIZE; i++) {
				int kvalue = kvalueRandomGenerator() % KEY_TO_BE_MODULATED; 
				 
				int payload_size = payloadSizeRandomGenerator();
				//int payload_size = 4;
				unsigned char *buffer = (unsigned char*)malloc(sizeof (unsigned char)*payload_size);
				//populate randome number to it 
				int partition_number = HashUtils::hash_key(kvalue, TOTAL_NUMBER_PARTITIONS);
				cout << "generated key is: " << kvalue << " goes to partition number: " << partition_number << endl;

				for (int bindex = 0; bindex < payload_size; bindex++) {
					buffer[bindex] = (unsigned char)charValueRandomGenerator();
					//cout << " map id: " << MAP_ID << " populated for buffer: " << (int)buffer[bindex] << " for key: " << kvalue 
					//   << " for partition: " << partition_number << endl;
				}

				//let's use partition as i; 
				//KeyValuePartitionTuple3(int kValue, unsigned char* vV, int vSize, int part)

				MERGESORT_CHANNEL_WITH_INTKEYS_TEST::KeyValuePartitionTuple3 tuple(kvalue, buffer, payload_size, partition_number);
				oneBatch.keyValuePairs.push_back(tuple);

				//also, store the generated key and value into the populatedKeyValueStore
                                size_t computed_hash = HashUtils::hash_compute(buffer, payload_size);
                                bool found = false; 
                                MergeSortTestStoreIdentity tIdentity(kvalue, computed_hash);
				unordered_map< MergeSortTestStoreIdentity, unsigned char*,
						MergeSortTestStoreIdentity_hash, MergeSortTestStoreIdentity_equal>::const_iterator p =
						populatedKeyValueStore.find(tIdentity);
				if (p != populatedKeyValueStore.end()) {
						found = true; 

				}
                                assert (found == false); //I should not expect to see collision. otherwise, it is wrong!
				populatedKeyValueStore[tIdentity] = buffer;
			}

			//push to map shuffle store 
			int total_holder_size = 0;
			int *valueSizesInEachBatch = new int[EACH_BATCH_SIZE];
			int cursor = 0;
			for (auto p = oneBatch.keyValuePairs.begin(); p != oneBatch.keyValuePairs.end(); ++p) {
				total_holder_size += p->valueSize;
				valueSizesInEachBatch[cursor] = p->valueSize;
				cursor++;
			}

			unsigned char *holder = (unsigned char*)malloc(total_holder_size);
			int *kvalues = new int[EACH_BATCH_SIZE]();
			int *voffsets = new int[EACH_BATCH_SIZE]();
			int *partitions = new int[EACH_BATCH_SIZE]();

			cursor = 0;
			unsigned char *ptr = holder;
			for (auto p = oneBatch.keyValuePairs.begin(); p != oneBatch.keyValuePairs.end(); ++p) {
				kvalues[cursor] = p->keyValue;
				//please check this is correct or not. 
				if (cursor == 0) {
					voffsets[cursor] = valueSizesInEachBatch[cursor];
				}
				else {
					voffsets[cursor] = voffsets[cursor - 1] + valueSizesInEachBatch[cursor];
				}
				partitions[cursor] = p->partition;

				//memcpy to holder's pointer
				memcpy((void*)ptr, (void*)p->valueValue, p->valueSize);
				ptr += p->valueSize;

				cursor++;

			}

			//still keep the holder as initial value. 
			for (int x = 0; x < EACH_BATCH_SIZE; x++) {
				//cout << "at batch " << "partition" << x << "with value of: " << partitions[x] << endl; 
			}


                        //introduced value type definition, thus, before store key/value pairs, store the value type definition
                        int valueTypeDefinitionLength = 99;
                        unsigned char valueTypeDefinition[1024];
                        for (int kp=0; kp<valueTypeDefinitionLength; kp++) {
                      	  valueTypeDefinition[kp] = (unsigned char) charValueRandomGenerator();
                    	}
                	shuffleStore.setValueType(valueTypeDefinition, valueTypeDefinitionLength);

			shuffleStore.storeKVPairsWithIntKeys(holder, voffsets, kvalues, partitions, EACH_BATCH_SIZE);

			//we push to one map store, only when the match is fully populated. NOTE: it is value copied.
			oneMapStore.batches.push_back(oneBatch);

		}//end of all batches for a map store 

		/*
		//this is for map store: need to inspect the buffer manager at this time.
		ExtensibleByteBuffers &bufferMgr = shuffleStore.getBufferMgr();
		int current_buffer_position = bufferMgr.current_buffer_position();

		assert(current_buffer_position == 0);

		//display the result of current buffer
		ByteBufferHolder& firstBuffer = bufferMgr.buffer_at(current_buffer_position);
		int position_in_current_buffer = firstBuffer.position_in_buffer();
		for (int i = 0; i < position_in_current_buffer; i++) {
		unsigned char element = firstBuffer.value_at(i);
		cout << "map id: " << mapStoreIndex << "bytes stored in buffer manager for buffer: " << current_buffer_position << " at position: " << i
		<< " is: " << (int)element << endl;
		}

		*/

		shuffleStore.sort(TOTAL_NUMBER_PARTITIONS);
		MapStatus mapStatus = shuffleStore.writeShuffleData();
		ALL_MAPSHUFFLE_STORES_MAPSTATUSES.push_back(mapStatus);

		ALL_MAP_STORES.push_back(oneMapStore);

		EXPECT_NE (mapStatus.offsetToIndexBucket,0);
		EXPECT_EQ (mapStatus.totalNumberOfPartitions,TOTAL_NUMBER_PARTITIONS);
		//YES. need to make sure that bucket size is the same as the total number of partitions. 
		EXPECT_EQ (mapStatus.bucketSizes.size(), TOTAL_NUMBER_PARTITIONS);


		ALL_CREATED_MAPSHUFFLE_STORES.push_back(shuffleStore);

	}//total number of map stores 


	int total_found = 0;
	//int reduceId = 5; 
	for (int reduceId = 0; reduceId < TOTAL_NUMBER_PARTITIONS; reduceId++) 
	{
	 
		    cout << "************reduce id " << reduceId << "*************************" << endl;

			//I only have one map bucket.
			ReduceStatus reduceStatus(reduceId);

			for (auto p = ALL_MAPSHUFFLE_STORES_MAPSTATUSES.begin(); p != ALL_MAPSHUFFLE_STORES_MAPSTATUSES.end(); ++p) {
				MapBucket mapBucket(reduceId, p->bucketSizes[reduceId],
					p->shmRegionName, p->offsetToIndexBucket, p->mapId);
				reduceStatus.addMapBucket(mapBucket);
			}

		 
                       	ExtensibleByteBuffers reduceSideBufferManager(buffer_size);
			ReduceShuffleStoreWithIntKey reduceShuffleStore(reduceStatus, 
							TOTAL_NUMBER_PARTITIONS, reduceId, &reduceSideBufferManager);

			KeyWithFixedLength::MergeSortedMapBuckets mergeSortedMapBuckets =
                                               reduceShuffleStore.retrieve_mergesortedmapbuckets(); 

			EXPECT_EQ (mergeSortedMapBuckets.reducerId,  reduceId);

			vector <int> retrieved_keys = mergeSortedMapBuckets.keys; 

			vector <vector <PositionInExtensibleByteBuffer>> retrieve_kvaluesGroups = mergeSortedMapBuckets.kvaluesGroups;
			//we will need the size to de-serialize the byte array. 
			vector <vector <int>> kvaluesGroupSizes = mergeSortedMapBuckets.kvaluesGroupSizes;

			//first check: the keys are in asscending order 
			int cursor = 0; 
			int kvalue_1, kvalue_2 = 0; 
			for (auto p = retrieved_keys.begin();  p != retrieved_keys.end(); ++p) {
				if (cursor == 0) {
					kvalue_1 = *p;
				}
				else {
					kvalue_2 = *p;
					assert(kvalue_2 > kvalue_1);
					kvalue_1 = kvalue_2; 
				}
				cursor++; 

				cout << "after merge-sorting, retrieved unique key is: " << *p << endl;
			}

			//second check: all of the (k, v) can be found from the original store 
	  
			cursor = 0; 
			for (auto p = retrieved_keys.begin(); p != retrieved_keys.end(); ++p) {
				int kvalue = *p; 
				vector <PositionInExtensibleByteBuffer> values = retrieve_kvaluesGroups[cursor];
				vector <int> sizes = kvaluesGroupSizes[cursor];

				cursor++; 
				int kvSize = values.size(); 

				for (int kvIndex = 0; kvIndex < kvSize; kvIndex++) {
					PositionInExtensibleByteBuffer value = values[kvIndex];
					int  size = sizes[kvIndex];

					cout << "scanning merge-sort result, retrieved unique key is: " << kvalue << " with value size: " << size << endl;
					bool found = false; 
					unsigned char *value_buffer = (unsigned char*)malloc(value.value_size);
					reduceShuffleStore.getBufferMgr()->retrieve(value, value_buffer);
					MergeSortTestStoreIdentity sortTestStoreIdentity(kvalue,
						                              HashUtils::hash_compute(value_buffer, size));

					unordered_map< MergeSortTestStoreIdentity, unsigned char*,
						MergeSortTestStoreIdentity_hash, MergeSortTestStoreIdentity_equal>::const_iterator p =
						populatedKeyValueStore.find(sortTestStoreIdentity);
					if (p != populatedKeyValueStore.end()) {
						found = true; 
						total_found++; 
					}

                                        EXPECT_EQ(found, true);

				}
			}

			 
	} //finish the scan of reduce id 

	//third check: all of the (k, v) found should be the same as the origial generalted (k,v) pair. 
	//NOTE: this check will have to go through all of the reducer id
	cout << "total matched k-v pair is: " << total_found << endl; 

	EXPECT_EQ (total_found, EACH_BATCH_SIZE * TOTAL_NUMBER_OF_BATCH_IN_EACH_MAP *TOTAL_NUMBER_OF_MAPSTORES);

}


// STEP 3. Call RUN_ALL_TESTS() in main().
int main(int argc, char **argv) {
  //Test_MergeSortEngine_MultipleKeysValuesPartitionsWriteAndRead_With_MultipleBuffers_WITH_HASHPARTITIONS(); 
  //Test_MergeSortEngine_SingleMapWithTwoKeys_InOnePartition();
  //Test_MergeSortChannel_SingleMapWithTwoKeys_InOnePartition();
  //with main, we can attach some google test related hooks.                                                                     

  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler() ;

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

//
// We do this by linking in src/gtest_main.cc file, which consists of
// a main() function which calls RUN_ALL_TESTS() for us.
//
// This runs all the tests you've defined, prints the result, and
// returns 0 if successful, or 1 otherwise.
//
// Did you notice that we didn't register the tests?  The
// RUN_ALL_TESTS() macro magically knows about all the tests we
// defined.  Isn't this convenient?
