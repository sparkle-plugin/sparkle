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

#include "MapShuffleStoreWithIntKeys.h"
#include "ReduceShuffleStoreWithIntKeys.h"
#include <string>
#include <vector>
#include <iostream>
#include <math.h>

#include "SimpleUtils.h"
#include "gtest/gtest.h"
#include "glog/logging.h"

#include <cassert> 
using namespace std;


 
//void TEST_MapShuffleStoreWithIntKeysTest_TwoSimplePartitionsWithIntKeySort() {
TEST(MapShuffleStoreWithIntKeysTest,TwoSimplePartitionsWithIntKeySort) {


  // Seed with a real random value, if available
  Rand_int randomGenerator {0, 120};
  
  int buffer_size=1*1024;
  int payload_size1 = (int)floor(buffer_size / 4);
  int payload_size2 = (int)floor(buffer_size *1.25);
  
  unsigned char *buffer1 = (unsigned char*)malloc (sizeof (unsigned char)*payload_size1);
  //populate randome number to it 
  for (int i=0; i<payload_size1; i++) {
      buffer1[i] = (unsigned char) randomGenerator(); 
  }

  unsigned char *buffer2 = (unsigned char*)malloc (sizeof (unsigned char)*payload_size2);
  //populate randome number to it 
  for (int i=0; i<payload_size2; i++) {
      buffer2[i] = (unsigned char) randomGenerator(); 
  }

  unsigned char *holder = (unsigned char*) malloc ( sizeof(unsigned char) * (payload_size1+payload_size2));
  unsigned char *holder_ptr = holder;
 
  int voffsets[2] = {payload_size1, payload_size1+payload_size2};
  memcpy(holder_ptr, buffer1, payload_size1);
  holder_ptr+=payload_size1;
  memcpy(holder_ptr, buffer2, payload_size2);

  int kvalues[2] = {145, 37};
  int partitions[2] = {34, 21};
  
  int numberOfPairs = 2; 
  int totalPartitions = 128; 

  int mapId = 9900;
  MapShuffleStoreWithIntKey shuffleStore(buffer_size, mapId);

  //introduced value type definition, thus, before store key/value pairs, store the value type definition
  int valueTypeDefinitionLength = 99;
  unsigned char valueTypeDefinition[1024];
  for (int kp=0; kp<valueTypeDefinitionLength; kp++) {
      valueTypeDefinition[kp] = (unsigned char) randomGenerator(); 
  }
  shuffleStore.setValueType(valueTypeDefinition, valueTypeDefinitionLength);

  shuffleStore.storeKVPairsWithIntKeys (holder, voffsets, kvalues, partitions, numberOfPairs);
  shuffleStore.sort (totalPartitions);
 
  vector <IntKeyWithValueTracker> & keys = shuffleStore.getKeys();

  //it is ascending order, with partition first, then key. 
  EXPECT_EQ (keys.size() , 2);

  EXPECT_EQ (keys[0].partition, partitions[1]);
  EXPECT_EQ (keys[1].partition, partitions[0]);

  EXPECT_EQ (keys[0].key, kvalues[1]);
  EXPECT_EQ (keys[1].key, kvalues[0]);

  free(holder);
}
 


//simple test with two partitions and one buffer is enough to hold the values.
//void Test_MapShuffleStoreWithIntKeysTest_TwoSimplePartitionsWriteAndRead_WithOne_Buffer() {
TEST(MapShuffleStoreWithIntKeysTest,TwoSimplePartitionsWriteAndRead_WithOne_Buffer) {
  int buffer_size = 1 * 1024;
  // Seed with a real random value, if available
  Rand_int randomGenerator{0, 120 };
 
  int payload_size1 = 12;
  int payload_size2 = 10;
  
  unsigned char *buffer1 = (unsigned char*)malloc (sizeof (unsigned char)*payload_size1);
  //populate randome number to it 
  for (int i=0; i<payload_size1; i++) {
      buffer1[i] = (unsigned char) randomGenerator(); 
      cout << " test case: populated for buffer 1: " << (int)buffer1[i] <<endl;
  }

  unsigned char *buffer2 = (unsigned char*)malloc (sizeof (unsigned char)*payload_size2);
  //populate randome number to it 
  for (int i=0; i<payload_size2; i++) {
      buffer2[i] = (unsigned char) randomGenerator(); 
      cout << " test case: populated for buffer 2: " << (int)buffer2[i] <<endl;
  }

  unsigned char *holder = (unsigned char*) malloc ( sizeof(unsigned char) * (payload_size1+payload_size2));
 
  int voffsets[2] = {payload_size1, payload_size1+payload_size2};
  memcpy(holder, buffer1, payload_size1);

  //NOTE: I can not update the holder, as I need this for the following computation that holder has to be resetted. 
  unsigned char *pholder = holder + payload_size1;
  memcpy(pholder, buffer2, payload_size2);

  int kvalues[2] = {145, 37};
  int partitions[2] = {1, 0}; //two partitions: but opposite, 
  
  int numberOfPairs = 2; 
  int totalPartitions = 2; 

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
  shuffleStore.storeKVPairsWithIntKeys (holder, voffsets, kvalues, partitions, numberOfPairs);

  //need to inspect the buffer manager at this time. 
  ExtensibleByteBuffers &bufferMgr = shuffleStore.getBufferMgr();
  int current_buffer_position = bufferMgr.current_buffer_position();
  EXPECT_EQ(current_buffer_position, 0);

  //display the result of current buffer
  ByteBufferHolder& firstBuffer = bufferMgr.buffer_at(current_buffer_position);
  int position_in_current_buffer = firstBuffer.position_in_buffer();
  for (int i = 0; i < position_in_current_buffer; i++) {
	  unsigned char element = firstBuffer.value_at(i);
	  cout << "bytes stored in buffer manager for buffer: " << current_buffer_position << " at position: " << i
		  << " is: " << (int)element << endl;
  }


  shuffleStore.sort (totalPartitions);
  
  //check the buffer manager's populated data 
  
  //I only have one mapper for contribution. 
  MapStatus mapStatus = shuffleStore.writeShuffleData();
 

  EXPECT_NE (mapStatus.offsetToIndexBucket, 0);
  EXPECT_EQ (mapStatus.totalNumberOfPartitions, totalPartitions);
  EXPECT_EQ (mapStatus.bucketSizes.size(), 2);

  {
	  int reduceId = 0;
	  //I only have one map bucket.
	  MapBucket mapBucket(reduceId, mapStatus.bucketSizes[reduceId],
		  mapStatus.shmRegionName, mapStatus.offsetToIndexBucket, mapStatus.mapId);
	  ReduceStatus reduceStatus(reduceId);
	  reduceStatus.addMapBucket(mapBucket);

          //introduce a seperate buffer manager 
          ExtensibleByteBuffers bufferMgr(buffer_size);
	  ReduceShuffleStoreWithIntKey reduceShuffleStore(reduceStatus,
                                               totalPartitions, reduceId, &bufferMgr);

	  //only one map so far
	  {
		  int mapId = MAP_ID; //so reduce Id =0 pick up bucket corresponding to map id =0; 
		  
		  KeyWithFixedLength::RetrievedMapBucket retrievedMapBucket = reduceShuffleStore.retrieve_mapbucket(mapId);

		  vector<int>& retrieved_keys = retrievedMapBucket.get_keys();
		  vector <unsigned char*> retrieved_values = retrievedMapBucket.get_values();
		  EXPECT_EQ(kvalues[1], retrieved_keys[0]);
		  unsigned char *retrieved_value = retrieved_values[0];
		  cout << "at test code: retrieved value with offset at memory address: " << (void*)retrieved_value << endl;

		  //do hash comparision
		  size_t hash_result_original = HashUtils::hash_compute(buffer2, payload_size2);
		  size_t hash_result_retrieved = HashUtils:: hash_compute(retrieved_value, payload_size2);


		  cout << "for reduce id " << reduceId << "and for map id: " << mapId << " at test code: hash result original is: " << hash_result_original
			  << " and hash result retrieved is: " << hash_result_retrieved << endl;
		  EXPECT_EQ(hash_result_original, hash_result_retrieved);
	  }

	  //since only one map, so if I put down mapId =1, I should get the assertion error 
	  {
		  int mapId = MAP_ID+1; 

		  if (mapId < reduceShuffleStore.getReduceStatus().getSizeOfMapBuckets()) {
		            KeyWithFixedLength::RetrievedMapBucket retrievedMapBucket =
                                        reduceShuffleStore.retrieve_mapbucket(mapId);
			  //what will happend? I only have one map status for this reduce id, so 
			  //we will encounter assert error. 
		  }
		  else{
			  cout << "for reduce id: " << reduceId 
                               << " (expected) WARNING: the selected map id:" 
                               << mapId << " is bigger than reduce shuffle store's size of map buckets: "
			       << reduceShuffleStore.getReduceStatus().getSizeOfMapBuckets() << endl;
		  }

	  }
  }


  {
	  int reduceId = 1;
	  MapBucket mapBucket(reduceId, mapStatus.bucketSizes[reduceId],
		  mapStatus.shmRegionName, mapStatus.offsetToIndexBucket, MAP_ID); //I only have one map to play around
	  ReduceStatus reduceStatus(reduceId);
	  reduceStatus.addMapBucket(mapBucket);

          ExtensibleByteBuffers bufferMgr(buffer_size);
	  ReduceShuffleStoreWithIntKey reduceShuffleStore(reduceStatus, totalPartitions, reduceId, &bufferMgr);

	  //only one map so far
	  {
		  //int mapId = 0; //so reduce Id =0 pick up bucket corresponding to map id =0; 
	          KeyWithFixedLength::RetrievedMapBucket retrievedMapBucket =
                                                   reduceShuffleStore.retrieve_mapbucket(MAP_ID);

		  vector<int>& retrieved_keys = retrievedMapBucket.get_keys();
		  vector <unsigned char*> retrieved_values = retrievedMapBucket.get_values();
	          EXPECT_EQ(kvalues[0], retrieved_keys[0]);

		  unsigned char *retrieved_value = retrieved_values[0];
		  cout << "at test code: retrieved value with offset at memory address: " << (void*)retrieved_value << endl;

		  //do hash comparision
		  size_t hash_result_original = HashUtils::hash_compute(buffer1, payload_size1);
		  size_t hash_result_retrieved = HashUtils::hash_compute(retrieved_value, payload_size1);


		  cout << "for reduce Id " << reduceId << " and for map id: " 
                       << MAP_ID << " at test code: hash result original is: " << hash_result_original
		       << " and hash result retrieved is: " << hash_result_retrieved << endl;

		  EXPECT_EQ (hash_result_original, hash_result_retrieved);
	  }

	  //since only one map, so if I put down mapId =1, I should get the assertion error 
	  {
		  int mapId = 1;

		  if (mapId < reduceShuffleStore.getReduceStatus().getSizeOfMapBuckets()) {
		          KeyWithFixedLength::RetrievedMapBucket retrievedMapBucket =
                                                        reduceShuffleStore.retrieve_mapbucket(mapId);
			  //what will happend? I only have one map status for this reduce id, so 
			  //we will encounter assert error. 
		  }
		  else{
			  cout << "for reduce id: " << reduceId 
                               << " (expected) WARNING: the selected map id:" << mapId 
                               << " is bigger than reduce shuffle store's size of map buckets: "
			       << reduceShuffleStore.getReduceStatus().getSizeOfMapBuckets() << endl;
		  }

	  }
  }

  free(holder);
}


//two key-value pairs, but with values filled in multiple buffers. 
//void Test_MapShuffleStoreWithIntKeysTest_TwoSimplePartitionsWriteAndRead_With_MultipleBuffers() {
TEST(MapShuffleStoreWithIntKeysTest,TwoSimplePartitionsWriteAndRead_With_MultipleBuffers) {


	// Seed with a real random value, if available
	Rand_int randomGenerator{ 0, 120 };

	int buffer_size = 1 * 1024;
	int payload_size1 = (int) floor(buffer_size * 0.89);
	int payload_size2 = (int) floor(buffer_size *2.38);

	unsigned char *buffer1 = (unsigned char*)malloc(sizeof (unsigned char)*payload_size1);
	//populate randome number to it 
	for (int i = 0; i<payload_size1; i++) {
		buffer1[i] = (unsigned char)randomGenerator();
		//cout << " test case: populated for buffer 1: " << (int)buffer1[i] << endl;
	}

	unsigned char *buffer2 = (unsigned char*)malloc(sizeof (unsigned char)*payload_size2);
	//populate randome number to it 
	for (int i = 0; i<payload_size2; i++) {
		buffer2[i] = (unsigned char)randomGenerator();
		//cout << " test case: populated for buffer 2: " << (int)buffer2[i] << endl;
	}

	unsigned char *holder = (unsigned char*)malloc(sizeof(unsigned char)* (payload_size1 + payload_size2));

	int voffsets[2] = { payload_size1, payload_size1 + payload_size2 };
	memcpy(holder, buffer1, payload_size1);

	//NOTE: I can not update the holder, as I need this for the following computation that holder has to be resetted. 
        unsigned char *pholder = holder + payload_size1;
	memcpy(pholder, buffer2, payload_size2);

	int kvalues[2] = { 145, 37 };
	int partitions[2] = { 1, 0 }; //two partitions: but opposite, 

	int numberOfPairs = 2;
	int totalPartitions = 2;

	int MAP_ID = 0;
	MapShuffleStoreWithIntKey shuffleStore(buffer_size, MAP_ID);

	//for (int i = 0; i < payload_size1 + payload_size2; i++) {
	//		unsigned char element = holder[i];
		//cout << "after initial memcopy, total buffer at position: " << i << " with value: " << (int)element << endl;
	//}


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
	//CHECK_EQ(current_buffer_position, 0);

	//display the result of current buffer
	//ByteBufferHolder& firstBuffer = bufferMgr.buffer_at(current_buffer_position);
	//int position_in_current_buffer = firstBuffer.position_in_buffer();
	//for (int i = 0; i < position_in_current_buffer; i++) {
	//	unsigned char element = firstBuffer.value_at(i);
	//	//cout << "bytes stored in buffer manager for buffer: " << current_buffer_position << " at position: " << i
		//	<< " is: " << (int)element << endl;
	//}


	shuffleStore.sort(totalPartitions);

	//check the buffer manager's populated data 

	//I only have one mapper for contribution. 
	MapStatus mapStatus = shuffleStore.writeShuffleData();
	EXPECT_NE(mapStatus.offsetToIndexBucket, 0);
	EXPECT_EQ(mapStatus.totalNumberOfPartitions, totalPartitions);
	EXPECT_EQ(mapStatus.bucketSizes.size(), 2);

	{
		int reduceId = 0;
		//I only have one map bucket.
		MapBucket mapBucket(reduceId, mapStatus.bucketSizes[reduceId],
			mapStatus.shmRegionName, mapStatus.offsetToIndexBucket, mapStatus.mapId);
		ReduceStatus reduceStatus(reduceId);
		reduceStatus.addMapBucket(mapBucket);

                ExtensibleByteBuffers bufferMgr(buffer_size);
		ReduceShuffleStoreWithIntKey reduceShuffleStore(reduceStatus, totalPartitions, reduceId, &bufferMgr);

		//only one map so far
		{
			int mapId = MAP_ID; //so reduce Id =0 pick up bucket corresponding to map id =0; 

			KeyWithFixedLength::RetrievedMapBucket retrievedMapBucket =
                                                  reduceShuffleStore.retrieve_mapbucket(mapId);

			vector<int>& retrieved_keys = retrievedMapBucket.get_keys();
			vector <unsigned char*> retrieved_values = retrievedMapBucket.get_values();
			EXPECT_EQ(kvalues[1],retrieved_keys[0]);

			unsigned char *retrieved_value = retrieved_values[0];
			cout << "at test code: retrieved value with offset at memory address: "
                             << (void*)retrieved_value << endl;

			//do hash comparision
			size_t hash_result_original = HashUtils::hash_compute(buffer2, payload_size2);
			size_t hash_result_retrieved = HashUtils::hash_compute(retrieved_value, payload_size2);


			cout << "for reduce id " << reduceId << "and for map id: " << mapId 
                             << " at test code: hash result original is: " << hash_result_original
			     << " and hash result retrieved is: " << hash_result_retrieved << endl;

			EXPECT_EQ(hash_result_original, hash_result_retrieved);
		}

		//since only one map, so if I put down mapId =1, I should get the assertion error 
		{
			int mapId = MAP_ID + 1;

			if (mapId < reduceShuffleStore.getReduceStatus().getSizeOfMapBuckets()) {
			         KeyWithFixedLength::RetrievedMapBucket retrievedMapBucket = 
                                                             reduceShuffleStore.retrieve_mapbucket(mapId);
				//what will happend? I only have one map status for this reduce id, so 
				//we will encounter assert error. 
			}
			else{
				cout << "for reduce id: " << reduceId
                                     << " (expected) WARNING: the selected map id:" << mapId 
                                     << " is bigger than reduce shuffle store's size of map buckets: "
				     << reduceShuffleStore.getReduceStatus().getSizeOfMapBuckets() << endl;
			}

		}
	}


	{
		int reduceId = 1;
		MapBucket mapBucket(reduceId, mapStatus.bucketSizes[reduceId],
			mapStatus.shmRegionName, mapStatus.offsetToIndexBucket, MAP_ID); //I only have one map to play around
		ReduceStatus reduceStatus(reduceId);
		reduceStatus.addMapBucket(mapBucket);

                ExtensibleByteBuffers bufferMgr(buffer_size);
		ReduceShuffleStoreWithIntKey reduceShuffleStore(reduceStatus, totalPartitions, reduceId, &bufferMgr);

		//only one map so far
		{
			//int mapId = 0; //so reduce Id =0 pick up bucket corresponding to map id =0; 
		         KeyWithFixedLength::RetrievedMapBucket retrievedMapBucket =
                                                        reduceShuffleStore.retrieve_mapbucket(MAP_ID);

			vector<int>& retrieved_keys = retrievedMapBucket.get_keys();
			vector <unsigned char*> retrieved_values = retrievedMapBucket.get_values();
			EXPECT_EQ(kvalues[0], retrieved_keys[0]);

			unsigned char *retrieved_value = retrieved_values[0];
			cout << "at test code: retrieved value with offset at memory address: "
                             << (void*)retrieved_value << endl;

			//do hash comparision
			size_t hash_result_original = HashUtils::hash_compute(buffer1, payload_size1);
			size_t hash_result_retrieved = HashUtils::hash_compute(retrieved_value, payload_size1);


			cout << "for reduce Id " << reduceId << " and for map id: "
                             << MAP_ID << " at test code: hash result original is: " << hash_result_original
			     << " and hash result retrieved is: " << hash_result_retrieved << endl;

			EXPECT_EQ(hash_result_original, hash_result_retrieved);
		}

		//since only one map, so if I put down mapId =1, I should get the assertion error 
		{
			int mapId = 1;

			if (mapId < reduceShuffleStore.getReduceStatus().getSizeOfMapBuckets()) {
			       KeyWithFixedLength::RetrievedMapBucket retrievedMapBucket = 
                                          reduceShuffleStore.retrieve_mapbucket(mapId);
				//what will happend? I only have one map status for this reduce id, so 
				//we will encounter assert error. 
			}
			else{
				cout << "for reduce id: " << reduceId
                                     << " (expected) WARNING: the selected map id:" << mapId 
                                     << " is bigger than reduce shuffle store's size of map buckets: "
				     << reduceShuffleStore.getReduceStatus().getSizeOfMapBuckets() << endl;
			}

		}
	}

	free(holder);
}


namespace SHUFFLE_STORE_WITH_INT_KEYS_TEST {
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

using namespace SHUFFLE_STORE_WITH_INT_KEYS_TEST;

//multiple keys, values, and multiple maps, but with the continuously increased partition number.
//void Test_MapShuffleStoreWithIntKeysTest_MultipleKeysValuesPartitionsWriteAndRead_With_MultipleBuffers_WITHFIXED_PARTITIONS() {
TEST(MapShuffleStoreWithIntKeysTest,MultipleKeysValuesPartitionsWriteAndRead_With_MultipleBuffers_WITHFIXED_PARTITIONS) {

	int buffer_size = 1 * 1024;
	// Seed with a real random value, if available
	Rand_int charValueRandomGenerator{ 0, 120 };
	Rand_int payloadSizeRandomGenerator{ 1, 10 * buffer_size };
	Rand_int  kvalueRandomGenerator{ 0, 100000 };

	 
	int EACH_BATCH_SIZE =2;
	int TOTAL_NUMBER_OF_BATCH_IN_EACH_MAP = 3; 
	int TOTAL_NUMBER_OF_MAPSTORES = 4;
	int TOTAL_NUMBER_PARTITIONS = EACH_BATCH_SIZE*TOTAL_NUMBER_OF_BATCH_IN_EACH_MAP;

	vector <SHUFFLE_STORE_WITH_INT_KEYS_TEST::OneMapStore> ALL_MAP_STORES;
	vector <MapShuffleStoreWithIntKey> ALL_CREATED_MAPSHUFFLE_STORES;
	vector <MapStatus>  ALL_MAPSHUFFLE_STORES_MAPSTATUSES;
	//in each map store, we have many batches;
 
 

	for (int mapStoreIndex = 0; mapStoreIndex < TOTAL_NUMBER_OF_MAPSTORES; mapStoreIndex++) {
		int MAP_ID = mapStoreIndex; 
		int partition_number = 0; //always start with 0; 

		MapShuffleStoreWithIntKey shuffleStore(buffer_size, MAP_ID);
		

		SHUFFLE_STORE_WITH_INT_KEYS_TEST::OneMapStore oneMapStore(MAP_ID);
		//can not store like this one, as this is value copied, not reference pushing. 
		//ALL_MAP_STORES.push_back(oneMapStore);

		for (int b = 0; b < TOTAL_NUMBER_OF_BATCH_IN_EACH_MAP; b++) {
			SHUFFLE_STORE_WITH_INT_KEYS_TEST::OneMapStoreBatch oneBatch;
			//you can not do this way. It is value copied. not reference pushing
			//oneMapStore.batches.push_back(oneBatch);

			for (int i = 0; i < EACH_BATCH_SIZE; i++) {
				int kvalue = kvalueRandomGenerator();
				int payload_size = payloadSizeRandomGenerator();
				//int payload_size = 4;
				unsigned char *buffer = (unsigned char*)malloc(sizeof (unsigned char)*payload_size);
				//populate randome number to it 
				for (int bindex = 0; bindex < payload_size; bindex++) {
					buffer[bindex] = (unsigned char)charValueRandomGenerator();
					//cout << " test case: populated for buffer: " << (int)buffer[bindex] << endl;
				}

				//let's use partition as i; 
				//KeyValuePartitionTuple3(int kValue, unsigned char* vV, int vSize, int part)
				KeyValuePartitionTuple3 tuple(kvalue, buffer, payload_size, partition_number);
				partition_number++;
				oneBatch.keyValuePairs.push_back(tuple);
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
				memcpy((void*)ptr, (void*) p->valueValue, p->valueSize);
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
			
			/*
			//need to inspect the buffer manager at this time. 
			ExtensibleByteBuffers &bufferMgr = shuffleStore.getBufferMgr();
			int current_buffer_position = bufferMgr.current_buffer_position();

			assert(current_buffer_position == 0);

			//display the result of current buffer
			ByteBufferHolder& firstBuffer = bufferMgr.buffer_at(current_buffer_position);
			int position_in_current_buffer = firstBuffer.position_in_buffer();
			for (int i = 0; i < position_in_current_buffer; i++) {
				unsigned char element = firstBuffer.value_at(i);
				//cout << "bytes stored in buffer manager for buffer: " << current_buffer_position << " at position: " << i
				//	<< " is: " << (int)element << endl;
			}

            */

			//we push to one map store, only when the match is fully populated. NOTE: it is value copied.
			oneMapStore.batches.push_back(oneBatch);

		}//end of all batches for a map store 

		shuffleStore.sort(TOTAL_NUMBER_PARTITIONS);
		MapStatus mapStatus = shuffleStore.writeShuffleData();
		ALL_MAPSHUFFLE_STORES_MAPSTATUSES.push_back(mapStatus);

		ALL_MAP_STORES.push_back(oneMapStore);

		EXPECT_NE (mapStatus.offsetToIndexBucket, 0);
		EXPECT_EQ (mapStatus.totalNumberOfPartitions, TOTAL_NUMBER_PARTITIONS);
		//YES. need to make sure that bucket size is the same as the total number of partitions. 
		EXPECT_EQ (mapStatus.bucketSizes.size(), TOTAL_NUMBER_PARTITIONS);
		ALL_CREATED_MAPSHUFFLE_STORES.push_back(shuffleStore);

	}//total number of map stores 

	 
   
	for (int reduceId = 0; reduceId < TOTAL_NUMBER_PARTITIONS; reduceId++) {
		for (int mapId = 0; mapId < TOTAL_NUMBER_OF_MAPSTORES; mapId ++ ) 
		{
			//I only have one map bucket.
			ReduceStatus reduceStatus(reduceId);

			for (auto p = ALL_MAPSHUFFLE_STORES_MAPSTATUSES.begin(); p != ALL_MAPSHUFFLE_STORES_MAPSTATUSES.end(); ++p) {
				MapBucket mapBucket(reduceId, p->bucketSizes[reduceId],
					p->shmRegionName, p->offsetToIndexBucket, p->mapId);
				reduceStatus.addMapBucket(mapBucket);
			}

                        ExtensibleByteBuffers bufferMgr(buffer_size);
			ReduceShuffleStoreWithIntKey reduceShuffleStore(reduceStatus, 
									TOTAL_NUMBER_PARTITIONS, reduceId, &bufferMgr);

			//only one map so far
			{
				//we will simulate later how map bucket can be empty. 
			        KeyWithFixedLength::RetrievedMapBucket retrievedMapBucket = 
                                                            reduceShuffleStore.retrieve_mapbucket(mapId);

				vector<int>& retrieved_keys = retrievedMapBucket.get_keys();
				vector <unsigned char*> retrieved_values = retrievedMapBucket.get_values();

				//because we only rerieve from the specified map id, the bucket (that is, partition number) = reduce id.
				//since our partition number keeps increasing, so we only have one. 
				EXPECT_EQ (retrieved_keys.size(), 1);
				EXPECT_EQ (retrieved_values.size(),1);
				//gather from the particular map store, then gather for all of the key-value's that have the partition number matches 
				//to the reducer id.
				OneMapStore& theParticularMapStore = ALL_MAP_STORES[mapId];
				//scan through this map store to find the keys and values 
				vector <int> original_keys;
				vector <unsigned char *> original_values;
				vector <int> original_valueSizes;

				for (auto p = theParticularMapStore.batches.begin(); p != theParticularMapStore.batches.end(); ++p) {
					for (auto q = p->keyValuePairs.begin(); q != p->keyValuePairs.end(); ++q) {
						//*q is a tuple 
						if (q->partition == reduceId) {
							original_keys.push_back(q->keyValue);
							original_values.push_back(q->valueValue);
							original_valueSizes.push_back(q->valueSize);
						}
					}
				}

				EXPECT_EQ (original_keys.size(),1);
				EXPECT_EQ (original_values.size(),1);

				int total_matches = 0;
				int number_of_keys_to_scan = original_keys.size();

				for (int i = 0; i < number_of_keys_to_scan; i++) {
					int theKey = original_keys[i];

					for (int counter = 0; counter < (int)retrieved_keys.size(); counter++) {
						int scanned_key = retrieved_keys[counter];
						if (scanned_key == theKey) {
							unsigned char *buffer_retrieved = retrieved_values[counter];
							unsigned char *buffer_original = original_values[i];
							int payload_size = original_valueSizes[i];

							//for (int t = 0; t < payload_size; t++) {
							//	cout << "retrieved: " << (int)buffer_retrieved[t] << " v.s. " << " original: " <<  (int) buffer_original[t] << endl;
							//}
							size_t hash_result_original = HashUtils::hash_compute(buffer_original, payload_size);
							size_t hash_result_retrieved = HashUtils::hash_compute(buffer_retrieved, payload_size);

							cout << "for reduce id " << reduceId << "and for map id: " 
                                                             << mapId << " at test code: hash result original is: " << hash_result_original
							     << " and hash result retrieved is: " << hash_result_retrieved << endl;

							EXPECT_EQ (hash_result_original,hash_result_retrieved);
							total_matches++;

						}
					}
				}

			        EXPECT_EQ (total_matches,number_of_keys_to_scan);
			} //finish map bucket scanning

		} //finish one pair of <reduce id, map id> 
	} //finish the scan of reduce id 

     
}


//multiple keys, values, and multiple maps, but with the parition going through hash key. therefore, some of the reducer id may have empty collection. increased partition number.
//void Test_MapShuffleStoreWithIntKeysTest_MultipleKeysValuesPartitionsWriteAndRead_With_MultipleBuffers_WITH_HASHPARTITIONS() {
TEST(MapShuffleStoreWithIntKeysTest, MultipleKeysValuesPartitionsWriteAndRead_With_MultipleBuffers_WITH_HASHPARTITIONS) {

	int buffer_size = 1 * 1024;
	// Seed with a real random value, if available
	Rand_int charValueRandomGenerator{ 0, 120 };
        //WARNING: in LInux, if I choose 10 below, then I will get the hash keys that have values not the same from the retrieved.
	//Rand_int payloadSizeRandomGenerator{ 1, 10 * buffer_size };
        //problem fixed!!! it is the reference issue.
        Rand_int payloadSizeRandomGenerator{ 1, 10 * buffer_size };

	Rand_int  kvalueRandomGenerator{ 0, 100000 };


	int EACH_BATCH_SIZE = 10;
	int TOTAL_NUMBER_OF_BATCH_IN_EACH_MAP = 20;
	int TOTAL_NUMBER_OF_MAPSTORES =10;
	//do we need to have the number of map partitions to be the same as the number of the reducer partitions
	//or the reducer partition can be different from the map partition (I think so!) 
	int TOTAL_NUMBER_PARTITIONS = 7; //so that some of the reducer has only empty partitions. 

	vector <OneMapStore> ALL_MAP_STORES;
	vector <MapShuffleStoreWithIntKey> ALL_CREATED_MAPSHUFFLE_STORES;
	vector <MapStatus>  ALL_MAPSHUFFLE_STORES_MAPSTATUSES;
	//in each map store, we have many batches;



	for (int mapStoreIndex = 0; mapStoreIndex < TOTAL_NUMBER_OF_MAPSTORES; mapStoreIndex++) {
		int MAP_ID = mapStoreIndex;

		MapShuffleStoreWithIntKey shuffleStore(buffer_size, MAP_ID);


	    OneMapStore oneMapStore(MAP_ID);
		//can not store like this one, as this is value copied, not reference pushing. 
		//ALL_MAP_STORES.push_back(oneMapStore);

		for (int b = 0; b < TOTAL_NUMBER_OF_BATCH_IN_EACH_MAP; b++) {
			OneMapStoreBatch oneBatch;
			//you can not do this way. It is value copied. not reference pushing
			//oneMapStore.batches.push_back(oneBatch);

			for (int i = 0; i < EACH_BATCH_SIZE; i++) {
				int kvalue = kvalueRandomGenerator();
				int payload_size = payloadSizeRandomGenerator();
				//int payload_size = 2;
				unsigned char *buffer = (unsigned char*)malloc(sizeof (unsigned char)*payload_size);
				//populate randome number to it 
				int partition_number = HashUtils::hash_key(kvalue, TOTAL_NUMBER_PARTITIONS);

				for (int bindex = 0; bindex < payload_size; bindex++) {
					buffer[bindex] = (unsigned char)charValueRandomGenerator();
					//cout << " map id: " << MAP_ID << " populated for buffer: " << (int)buffer[bindex] << " for key: " << kvalue 
					 //   << " for partition: " << partition_number << endl;
				}

				//let's use partition as i; 
				//KeyValuePartitionTuple3(int kValue, unsigned char* vV, int vSize, int part)
				 
				KeyValuePartitionTuple3 tuple(kvalue, buffer, payload_size, partition_number);
				oneBatch.keyValuePairs.push_back(tuple);
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

		EXPECT_NE (mapStatus.offsetToIndexBucket, 0);
		EXPECT_EQ (mapStatus.totalNumberOfPartitions, TOTAL_NUMBER_PARTITIONS);
		//YES. need to make sure that bucket size is the same as the total number of partitions. 
		EXPECT_EQ (mapStatus.bucketSizes.size(), TOTAL_NUMBER_PARTITIONS);

		ALL_CREATED_MAPSHUFFLE_STORES.push_back(shuffleStore);

	}//total number of map stores 



	for (int reduceId = 0; reduceId < TOTAL_NUMBER_PARTITIONS; reduceId++) {
		for (int mapId = 0; mapId < TOTAL_NUMBER_OF_MAPSTORES; mapId++)
		{
			//I only have one map bucket.
			ReduceStatus reduceStatus(reduceId);

			for (auto p = ALL_MAPSHUFFLE_STORES_MAPSTATUSES.begin(); p != ALL_MAPSHUFFLE_STORES_MAPSTATUSES.end(); ++p) {
				MapBucket mapBucket(reduceId, p->bucketSizes[reduceId],
					p->shmRegionName, p->offsetToIndexBucket, p->mapId);
				reduceStatus.addMapBucket(mapBucket);
			}

    	                ExtensibleByteBuffers bufferMgr(buffer_size);
     		        ReduceShuffleStoreWithIntKey reduceShuffleStore(reduceStatus,
									TOTAL_NUMBER_PARTITIONS, reduceId, &bufferMgr);

			//this is for one map at a time. 
			{
				//we will simulate later how map bucket can be empty. 
			        KeyWithFixedLength::RetrievedMapBucket retrievedMapBucket = 
                                                                reduceShuffleStore.retrieve_mapbucket(mapId);

				vector<int>& retrieved_keys = retrievedMapBucket.get_keys();
				vector <unsigned char*> & retrieved_values = retrievedMapBucket.get_values();
				vector <int> & retrieved_valuesizes = retrievedMapBucket.get_valueSizes();

				if (retrieved_keys.size() == 0) {
					cout << "reduce id: " << reduceId << "map id:" << mapId << " 0 collection" << endl;
				}
				 

				//gather from the particular map store, then gather for all of the key-value's that 
                                //have the partition number matches to the reducer id.
				OneMapStore& theParticularMapStore = ALL_MAP_STORES[mapId];
				//scan through this map store to find the keys and values 
				vector <int> original_keys;
				vector <unsigned char *> original_values;
				vector <int> original_valueSizes;

				for (auto p = theParticularMapStore.batches.begin(); p != theParticularMapStore.batches.end(); ++p) {
					for (auto q = p->keyValuePairs.begin(); q != p->keyValuePairs.end(); ++q) {
						//*q is a tuple 
						if (q->partition == reduceId) {
							original_keys.push_back(q->keyValue);
							original_values.push_back(q->valueValue);
							original_valueSizes.push_back(q->valueSize);
						}
					}
				}

				//NOTE: they can be 0 size. 
				EXPECT_EQ (original_keys.size(), retrieved_keys.size());
				EXPECT_EQ (original_values.size(),retrieved_values.size());
				
				int total_matches = 0;
				int number_of_keys_to_scan = retrieved_keys.size();

				//Also, the retrieved keys are in ascending order 
				for (int t = 0; t<number_of_keys_to_scan; t++) {
					if (t < number_of_keys_to_scan - 1) {
						assert(retrieved_keys[t] <= retrieved_keys[t + 1]);
					}
				}

				//NOTE: by using the scanned key for display, we should see the keys are in ascending order, if the reducer id is the same!!!
				if (number_of_keys_to_scan > 0) {
					for (int i = 0; i < number_of_keys_to_scan; i++) {
						//int theKey = original_keys[i];
						int theKey = retrieved_keys[i];
						int payload_retrieved = retrieved_valuesizes[i];
						bool matched = false; 

						//NOTE: this takes into account duplicated key!! 
						for (int counter = 0; counter < number_of_keys_to_scan; counter++) {
							int scanned_key = original_keys[counter];
							if (scanned_key == theKey) {
								//the matching target is: retrieved_values, not the original values. 
								unsigned char *buffer_retrieved = retrieved_values[i];
								unsigned char *buffer_original = original_values[counter];
								int payload_size_original = original_valueSizes[counter];

								
								size_t hash_result_original = HashUtils::hash_compute(buffer_original, payload_size_original);
								size_t hash_result_retrieved = HashUtils::hash_compute(buffer_retrieved, payload_retrieved);

								cout << "reduce id: " << reduceId << "map id:" << mapId 
                                                                     << " hash result original is: " << hash_result_original
								     << " and hash result retrieved is: " << hash_result_retrieved
                                                                     << " for key: " << scanned_key << endl;

								if (hash_result_original != hash_result_retrieved) {
									/*
									for (int t = 0; t < payload_size; t++) {
									  cout << "reduce id: " << reduceId << "map id:" << mapId
									    << "retrieved: " << (int)buffer_retrieved[t] << " v.s. " << " original: " <<  (int) buffer_original[t] 
										<< " *******duplicated key*******"
										<< endl;
									}
									*/
									cout << "***********************************duplicated key****************************" << endl; 
								}
								else {
									//the key may be duplicated, so that the values are not equivalent. so we need to advanced to next key.
									matched = true; 
									total_matches++;
									break; 
								}
							 
							}
						}

					        EXPECT_EQ (matched, true); 

					}
				}

				EXPECT_EQ (total_matches, number_of_keys_to_scan);

				//then free the map bucket's acquired memory.
				reduceShuffleStore.free_retrieved_mapbucket(retrievedMapBucket); 

			} //finish map bucket scanning

		} //finish one pair of <reduce id, map id> 
	} //finish the scan of reduce id 


}


// Step 3. Call RUN_ALL_TESTS() in main().
int main(int argc, char **argv) {
   //Test_MapShuffleStoreWithIntKeysTest_MultipleKeysValuesPartitionsWriteAndRead_With_MultipleBuffers_WITH_HASHPARTITIONS();
   //Test_MapShuffleStoreWithIntKeysTest_MultipleKeysValuesPartitionsWriteAndRead_With_MultipleBuffers_WITHFIXED_PARTITIONS();
   //Test_MapShuffleStoreWithIntKeysTest_TwoSimplePartitionsWriteAndRead_With_MultipleBuffers();
   //Test_MapShuffleStoreWithIntKeysTest_TwoSimplePartitionsWriteAndRead_WithOne_Buffer();
   //TEST_MapShuffleStoreWithIntKeysTest_TwoSimplePartitionsWithIntKeySort();
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
