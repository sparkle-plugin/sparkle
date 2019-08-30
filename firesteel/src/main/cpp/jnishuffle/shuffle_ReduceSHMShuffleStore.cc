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
#include <stdexcept>
#include <vector>
#include <chrono>
#include "com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore.h"
#include "ShuffleStoreManager.h"
#include "ReduceShuffleStoreManager.h"
#include "ReduceShuffleStoreWithIntKeys.h"
#include "ReduceShuffleStoreWithLongKeys.h"
#include "ReduceShuffleStoreWithByteArrayKeys.h"
#include "ReduceShuffleStoreWithObjKeys.h"
#include "ExtensibleByteBuffers.h"
#include "GenericReduceShuffleStore.h"
#include "JniUtils.h"

using namespace std;

/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    ninitialize
 * Signature: (JIII)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_ninitialize
  (JNIEnv *env, jobject obj, jlong ptrToShuffleManager, jint shuffleId, jint reduceId, jint numberOfPartitions) {
  //todo: we need to understand how reduce shuffle store will need to be brought together by Spark. 
  //at this time, defer until merge sort is called.

}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    nstop
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_nstop
(JNIEnv *env, jobject obj, jlong shuffleStorePtr) {
  GenericReduceShuffleStore *shuffleStore = reinterpret_cast <GenericReduceShuffleStore *> (shuffleStorePtr);
  shuffleStore->stop();
  VLOG(2) << "****in JNI nstop, to stop reduce shuffle store***"<<endl;
}


/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    nshutdown
 * Signature: (JII)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_nshutdown
(JNIEnv *env, jobject obj, jlong shuffleStorePtr) {
  GenericReduceShuffleStore *shuffleStore = reinterpret_cast <GenericReduceShuffleStore *> (shuffleStorePtr);
  shuffleStore->shutdown();
}


/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    nmergeSort
 * Signature: (JIILcom/hp/hpl/firesteel/shuffle/ShuffleDataModel/ReduceStatus;IILjava/nio/ByteBuffer;IZZ)J
 */
JNIEXPORT jlong JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_nmergeSort
(JNIEnv *env, jobject obj, jlong ptrToShuffleManager, jint shuffleId, jint reducerId,
 jobject reduceStatus, jint totalBuckets, jint numberOfPartitions, jobject byteBuffer, jint bufferCapacity,
 jboolean ordering, jboolean aggregation) {
  //NOTE: do we need this method? as merge sort is internal to shuffle engine, what you do is to

  //(1): get the shuffle store manager pointer, and then the reduce shuffle store manager 
  ShuffleStoreManager *shuffleStoreManager = reinterpret_cast <ShuffleStoreManager *> (ptrToShuffleManager);
  //at this time, reduce shuffle store manager is already initialized when shuffle store manager is initialied.                                              
  ReduceShuffleStoreManager *reduceShuffleStoreManager =  shuffleStoreManager->getReduceShuffleStoreManager();

  //(2) we are missing the step to examine from one of the ReduceStatus, the key value specification. 
  // in order to determine the type (int, float, string, object) for reduce shuffle store.   

  //(3)
  //(3.1) prepare the ReduceStatus from the reduceStatus java object.
  ReduceStatus status (reducerId); //then keep addMapBucket, after creating MapBucket
  
  jclass cls = env->GetObjectClass(reduceStatus);

  jfieldID mapIdsFieldId = env->GetFieldID(cls, "mapIds", "[I");
  if (mapIdsFieldId == NULL) {
    LOG(FATAL) << "can not find the field of mapIds with type of int array" <<endl;
    return 0;
  }  

  static jfieldID regionIdsOfIndexChunksFieldId = NULL; 
  if (regionIdsOfIndexChunksFieldId == NULL) {
    regionIdsOfIndexChunksFieldId = env->GetFieldID(cls, "regionIdsOfIndexChunks", "[J");
    if (regionIdsOfIndexChunksFieldId == NULL) {
      LOG(FATAL) << "can not find the field of regionIdsOfIndexChunks with type of long array" <<endl;
      return 0;
   }
  }

  static jfieldID offsetsOfIndexChunksFieldId = NULL;
  if (offsetsOfIndexChunksFieldId == NULL) {
    offsetsOfIndexChunksFieldId = env->GetFieldID(cls, "offsetsOfIndexChunks", "[J");
    if (offsetsOfIndexChunksFieldId == NULL) {
      LOG(FATAL) << "can not find the field of offsetsOfIndexChunks with type of long array" <<endl;
      return 0;
    }
  }
  
  static jfieldID sizesFieldId = NULL;
  if (sizesFieldId == NULL) {
    sizesFieldId = env->GetFieldID(cls, "sizes", "[J");
    if (sizesFieldId == NULL ) {
      LOG(FATAL) << "can not find the field of sizes with type of long array" <<endl;
      return 0;
    }
  }
  
  VLOG(2) << "****in JNI mergeSort keys, finished object field ID retrieval***";

  //retrieve the mapIds 
  jobject mapIdsObject = env->GetObjectField(reduceStatus, mapIdsFieldId);
  jobject offsetsObject = env->GetObjectField(reduceStatus, offsetsOfIndexChunksFieldId);
  jobject sizesObject = env->GetObjectField(reduceStatus, sizesFieldId);
  jobject regionIdsObject = env->GetObjectField(reduceStatus, regionIdsOfIndexChunksFieldId);

  jintArray *mapIdsVal = reinterpret_cast<jintArray *>(&mapIdsObject);
  jlongArray *offsetsVal = reinterpret_cast<jlongArray *>(&offsetsObject);
  jlongArray *sizesVal = reinterpret_cast<jlongArray *> (&sizesObject);
  jlongArray *regionIdsVal = reinterpret_cast<jlongArray *>(&regionIdsObject);

  int *mapIdsIntVal = env->GetIntArrayElements(*mapIdsVal, NULL); //could be copied, no guarantee.
  if (mapIdsIntVal == NULL) {
    LOG(FATAL) << "retrieved mapIds int array is null" <<endl;
    return 0;
  }

  VLOG(2) << "****in JNI mergeSort, finished map ids (int array) retrieval***";

  long *regionIdsLongVal = env->GetLongArrayElements(*regionIdsVal, NULL); //could be copied, no guarantee.
  if (regionIdsLongVal == NULL) {
    LOG(FATAL) << "retrieved regionIds long array is null" <<endl;
    return 0;
  }

  VLOG(2) << "****in JNI mergeSort, finished region ids (long array) retrieval***";


  long *offsetsLongVal=env->GetLongArrayElements(*offsetsVal, NULL); //could be copied, no guarantee.
  if (offsetsLongVal == NULL) {
    LOG(FATAL) << "retrieved offsets long array is null" <<endl;
    return 0;
  }

  VLOG(2) << "****in JNI mergeSort, finished offsets (long array) retrieval***";

  long *sizesLongVal = env->GetLongArrayElements(*sizesVal, NULL); //could be copied, no guarantee
  if (sizesLongVal == NULL) {
    LOG(FATAL) << "retrieved sizes long array is null" <<endl;
    return 0;
  }

  VLOG(2) << "****in JNI mergeSort, finished sizes (long array) retrieval***";

  for (int i=0; i<totalBuckets; i++) {
    MapBucket bucket (reducerId, sizesLongVal[i], regionIdsLongVal[i], offsetsLongVal[i], mapIdsIntVal[i]);
    status.addMapBucket(bucket);

  }  
 
  VLOG(2) << "****in JNI mergeSort keys, finished adding all map buckets***";

  //direct buffer is used for pass-through based operators.
  unsigned char *buffer = (unsigned char*)env->GetDirectBufferAddress(byteBuffer);
  //Herein ExtensibleByteBuffer is used, this is different to hold the Java side's direct bytebuffer
  //for de-serialization.
  ExtensibleByteBuffers *kBufferManager = nullptr;
  ExtensibleByteBuffers *vBufferManager = nullptr;
  if (ordering || aggregation) {
    kBufferManager = 
                new ExtensibleByteBuffers (SHMShuffleGlobalConstants::BYTEBUFFER_HOLDER_SIZE);
  }

  //before we do this step, we will need to retrieve the key type information and value type information from
  //a channel.

  void *reduceShuffleStore = nullptr;

  //map buckets will have size always > 0, even with partition sizes are zero.
  if (status.mapBuckets.size() > 0) { 
     //we will need to pick up the map bucket that is with non-zero length
     //MapBucket firstBucket = status.mapBuckets[0]; 
     KValueTypeDefinition kd;
     VValueTypeDefinition vd;
     bool  nonzerosize_found = false; 
     for (auto p = status.mapBuckets.begin(); p != status.mapBuckets.end(); ++p) {
       if (p->size > 0) {
  	  nonzerosize_found = true; 
          reduceShuffleStoreManager->obtain_kv_definition (*p,
                               reducerId, numberOfPartitions, kd, vd);
          break;
       }
     }

     if (!nonzerosize_found) {
       //all buckets are with zero size, which is different from all partitions are with zero size.
       kd.typeId = KValueTypeId::Int;
     }

     VLOG(2) << "****in JNI mergeSort, kvalue type id identified to be: " << kd.typeId<<endl;
     VLOG(2) << "****in JNI mergeSort, ordering identified to be: " << (bool)ordering
	     << " and aggregation identified to be: " << (bool)aggregation <<endl;

     switch (kd.typeId) {

        case KValueTypeId::Int: 
	  {
             VLOG(2) << "****in JNI mergeSort, kvalue type id is identified with Int***";

             GenericReduceShuffleStore *gResultStore=
                 reduceShuffleStoreManager->createStore(shuffleId, reducerId,
					 status, numberOfPartitions, 
                                         kBufferManager, 
					 buffer, bufferCapacity,
                                         kd.typeId, ordering, aggregation);
	     ReduceShuffleStoreWithIntKey *resultStore = dynamic_cast<ReduceShuffleStoreWithIntKey*>(gResultStore);
             //pointer will be free when stop the shuffle store
             resultStore->setVVTypeDefinition(vd); 
             reduceShuffleStore=(void*)resultStore;
             
             VLOG(2) << "****in JNI mergeSort(identified with int keys), finished creating reduce shuffle store***";
             break;
	  }

        case KValueTypeId::Long: 
        {
             VLOG(2) << "****in JNI mergeSort, kvalue type id is identified with Long***";

             GenericReduceShuffleStore *gResultStore=
                 reduceShuffleStoreManager->createStore(shuffleId, reducerId,
					 status, numberOfPartitions, 
                                         kBufferManager, 
					 buffer, bufferCapacity,
                                         kd.typeId, ordering, aggregation);
	     ReduceShuffleStoreWithLongKey *resultStore = dynamic_cast<ReduceShuffleStoreWithLongKey*>(gResultStore);
             //pointer will be free when stop the shuffle store
             resultStore->setVVTypeDefinition(vd); 
             reduceShuffleStore=(void*)resultStore;
             
             VLOG(2) << "****in JNI mergeSort(identified with long keys), finished creating reduce shuffle store***";
             break;

        }
        case KValueTypeId::Float: 
        {
          VLOG(2) << "****in JNI mergeSort(identified with float keys), kvalue type id is identified with Float***";
          break; 
        }

        case KValueTypeId::Double: 
        {
          VLOG(2) << "****in JNI mergeSort(identified with double keys), kvalue type id is identified with Double***";
          break; 
        }
        case KValueTypeId::String: 
        {
          VLOG(2) << "****in JNI mergeSort(identified with string keys), kvalue type id is identified with String***";
          break; 
        }

        case KValueTypeId::ByteArray:
        {
          VLOG(2) << "****in JNI mergeSort(identified with byte-array keys), kvalue type id is identified with byte-array***";

          //kBufferManager is created early in this method.

          vBufferManager = 
                new ExtensibleByteBuffers (SHMShuffleGlobalConstants::BYTEBUFFER_HOLDER_SIZE);


          GenericReduceShuffleStore *gResultStore=
                 reduceShuffleStoreManager->createStore(shuffleId, reducerId,
					 status, numberOfPartitions, 
					 kBufferManager, vBufferManager,
					 buffer, bufferCapacity,
                                         kd.typeId, ordering, aggregation);
	     ReduceShuffleStoreWithByteArrayKey *resultStore = dynamic_cast<ReduceShuffleStoreWithByteArrayKey*>(gResultStore);
             //pointer will be free when stop the shuffle store
             resultStore->setVVTypeDefinition(vd); 
             reduceShuffleStore=(void*)resultStore;
             
             VLOG(2) << "****in JNI mergeSort(identified with byte-array keys), finished creating reduce shuffle store***";


	  break;
        }
        case KValueTypeId::Object:
        {
          VLOG(2) << "****in JNI mergeSort(identified with object keys), kvalue type id is identified with Object***";
          break; 
        }
        case KValueTypeId::Unknown:
        {
          VLOG(2) << "****in JNI mergeSort(identified with unknown keys), kvalue type id is identified with Unknown***";
          break; 
        }


	  //to be continued later for other specific types of stores.
     }
  }
  else {
     LOG(ERROR) << "in JNI mergeSort, map buckets for the reducer is 0";
  }
   
  //free local objects 
  env->ReleaseIntArrayElements(*mapIdsVal, mapIdsIntVal, 0);
  env->ReleaseLongArrayElements(*regionIdsVal, regionIdsLongVal, 0);
  env->ReleaseLongArrayElements(*offsetsVal, offsetsLongVal, 0);
  env->ReleaseLongArrayElements(*sizesVal, sizesLongVal, 0);

  long ptr = reinterpret_cast<long>(reduceShuffleStore);

  //NOTE: actually there is no specific merge-sort call, intead, the pull-based merge-sort happens when we 
  //invoke the getKvalues method.

  VLOG(2) << "****in JNI mergeSort, finished all methods called with returned ptr=" 
          << (void*)reduceShuffleStore;

  return ptr;
}

JNIEXPORT jlong JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_ncreateShuffleStore
(JNIEnv* env, jobject obj, jlong ptrShuffleStoreManager, jint shuffleId, jint reducerId,
 jobject reduceStatus, jint totalBuckets, jobject byteBuffer, jint bufferCapacity,
 jboolean needOrdering, jboolean needAggregation) {

  ReduceStatus status(reducerId);
  {
    jclass clazz = env->GetObjectClass(reduceStatus);

    jfieldID fidSizes = env->GetFieldID(clazz, "sizes", "[J");
    long* bucketSizes = env->GetLongArrayElements((jlongArray)env->GetObjectField(reduceStatus, fidSizes), NULL);

    jfieldID fidRegionIds =
      env->GetFieldID(clazz, "regionIdsOfIndexChunks", "[J");
    long* regionIds =
      env->GetLongArrayElements((jlongArray)env->GetObjectField(reduceStatus, fidRegionIds), NULL);

    jfieldID fidOffsets = env->GetFieldID(clazz, "offsetsOfIndexChunks", "[J");
    long* offsets =
      env->GetLongArrayElements((jlongArray)env->GetObjectField(reduceStatus, fidOffsets), NULL);

    jfieldID fidMapIds = env->GetFieldID(clazz, "mapIds", "[I");
    int* mapIds =
      env->GetIntArrayElements((jintArray)env->GetObjectField(reduceStatus, fidMapIds), NULL);

    for (int i=0; i<totalBuckets; ++i) {
      MapBucket bucket(reducerId, bucketSizes[i], regionIds[i], offsets[i], mapIds[i]);
      status.addMapBucket(bucket);
    }
  }

  unsigned char *buffer =
    (unsigned char*) env->GetDirectBufferAddress(byteBuffer);
  ReduceShuffleStoreManager* reduceShuffleStoreManager =
    reinterpret_cast<ShuffleStoreManager*>(ptrShuffleStoreManager)->getReduceShuffleStoreManager();

  ReduceShuffleStoreWithObjKeys* resultStore = dynamic_cast<ReduceShuffleStoreWithObjKeys*>(
    reduceShuffleStoreManager->createStore(shuffleId, reducerId,
                                           status, -1, // Do we really need numPartitions?
                                           nullptr, buffer, bufferCapacity,
                                           KValueTypeId::Object, needOrdering, needAggregation));

  resultStore->prepare(env);

  return reinterpret_cast<long>(resultStore);
}


/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    ngetKValueTypeId
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_ngetKValueTypeId
(JNIEnv *env, jobject obj, jlong ptrToStore) {

  //(1): get the shuffle store manager pointer, and then the reduce shuffle store manager 
  GenericReduceShuffleStore *shuffleStore = reinterpret_cast <GenericReduceShuffleStore* > (ptrToStore);
  KValueTypeId kvTypeId =shuffleStore->getKValueType().typeId;
  return (jint)kvTypeId; 
}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    ngetKValueType
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_ngetKValueType
(JNIEnv *env, jobject obj, jlong ptrToStore){

 {

    const char *exClassName = "java/lang/UnsupportedOperationException";
    jclass ecls = env->FindClass (exClassName);
    if (ecls != NULL){
      env->ThrowNew(ecls, "ngetKValueType only for arbitrary <k,v>, which is not supported currently");
    }

  }
  return 0;
}


/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    ngetVValueType
 * Signature: (JII)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_ngetVValueType
(JNIEnv *env, jobject obj, jlong ptrToStore){

  //(1): get the shuffle store manager pointer, and then the reduce shuffle store manager 
  GenericReduceShuffleStore *gStore = reinterpret_cast <GenericReduceShuffleStore *> (ptrToStore);
  jbyteArray vtypeDefArrayVal = NULL;

  VValueTypeDefinition vvalueTypeDef = gStore->getVValueType();

  unsigned char *vtypeDef = vvalueTypeDef.definition; 
  int size = vvalueTypeDef.length; 
       
  //now create a jbyteArray from the passed in pointer.
  vtypeDefArrayVal = env->NewByteArray(size);
  if (vtypeDefArrayVal == NULL) {
    LOG(FATAL) <<"cannot create vvalue type definition byte arrary" <<endl;
       return NULL;
  }

  env->SetByteArrayRegion(vtypeDefArrayVal, 0, size, (const signed char*)vtypeDef);
      
  if (VLOG_IS_ON(3)) {
       VLOG(3) << "***in JNI ngetVValueType, value type size is: " << size << endl;
       for (int i=0; i<size; i++) {
	    VLOG(3) << "***in JNI ngetVValueType with int keys, value type at position: " << i 
                    << " value: " << (int)vtypeDef[i] << endl;
       }
  }

  VLOG(2) << "***in JNI ngetVValueType, finished assigning the vvalue type definition array***"<<endl;   

  return vtypeDefArrayVal;
}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    nGetKVPairs
 * Signature: (Ljava/nio/ByteBuffer;I[II)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_nGetKVPairs
(JNIEnv *env, jobject obj, jlong ptrToReduceStore, jobjectArray okvalues, jobject byteBuffer,
 jint buffer_capacity, jintArray voffsetsArray, jint knumbers){
  ReduceShuffleStoreWithObjKeys* reduceShuffleStore =
    reinterpret_cast<ReduceShuffleStoreWithObjKeys*> (ptrToReduceStore);

  // retrieve knumbers kv pairs from buckets via the store.
  vector<vector<ReduceKVPair>> pairs {reduceShuffleStore->fetchAggregatedPairs(knumbers)};
  int actualNumKVPairs = static_cast<int>(pairs.size());

  byte* buffer = (byte*)env->GetDirectBufferAddress(byteBuffer);
  int currentBufferSize {0};

  // copy key-values pair back to Java.
  vector<jint> valueOffsets;
  int actualOffset {0};
  for (int i=0; i<actualNumKVPairs; ++i) {
    env->SetObjectArrayElement(okvalues, i, pairs[i][0].getKey());

    for (auto& pair : pairs[i]) {
      int serValueSize = pair.getSerValueSize();
      if (currentBufferSize + serValueSize > buffer_capacity) {
        throw length_error("direct buffer is almost full.");
      }

      memcpy(buffer, pair.getSerValue(), serValueSize);
      buffer += serValueSize;

      actualOffset += serValueSize;
    }

    valueOffsets.push_back(actualOffset);

    reduceShuffleStore->deleteJobjectKeys(env, pairs[i]);
  }

  env->SetIntArrayRegion(voffsetsArray, 0, actualNumKVPairs, valueOffsets.data());

  return static_cast<int>(actualNumKVPairs);
}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    nGetKVPairsWithIntKeys
 * Signature: (JLjava/nio/ByteBuffer;IILcom/hp/hpl/firesteel/shuffle/ShuffleDataModel/MergeSortedResult;)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_nGetKVPairsWithIntKeys
(JNIEnv *env, jobject obj, jlong ptrToReduceStore, jobject byteBuffer, jint buffer_capacity,
  jint kmaxNumbers, jobject mergeResult) {
  //NOTE: byteBuffer is not used, this is assumed to be the same as the one passed to sort-merge method call earlier.
  VLOG(2) << "***in JNI getKVPairs with int keys, specified max number of KVs: " << kmaxNumbers;
   
  //for sure, this is an int-key based reduce shuffle store, return the actual number of the k-vs.
  ReduceShuffleStoreWithIntKey *shuffleStoreWithIntKeys = 
                       reinterpret_cast<ReduceShuffleStoreWithIntKey *> (ptrToReduceStore);
  // we can not do reset for each batch of get k-values, as the pending elements in merge-sort network 
  // requires the buffer to not be reset (otherwise, data gets lost).
  // BufferManager *bufferManager = shuffleStoreWithIntKeys->getBufferMgr();
  // bufferManager->reset(); //to get to the ByteBuffer's initial position.

  //int reducerId = shuffleStoreWithIntKeys->getReducerId();
  IntKeyWithFixedLength::MergeSortedMapBuckets& resultHolder=shuffleStoreWithIntKeys->getMergedResultHolder();
  shuffleStoreWithIntKeys->reset_mergedresult();
  int actualNumberOfKVs = shuffleStoreWithIntKeys->retrieve_mergesortedmapbuckets(kmaxNumbers);

  VLOG(2) << "***in JNI getKVPairs with int keys, finished retrieve_mergesort with number of KVs: " << actualNumberOfKVs;   

  //(1)now populate the merge result object. it is with type of ShuffleDataModel.MergeSortedResult in Java.
  jclass cls = env->GetObjectClass(mergeResult);
  //(2)I only care about int kValues  and Voffsets and bufferExceeded
  static jfieldID kvaluesArrayFieldId = NULL;
  if (kvaluesArrayFieldId == NULL) {
     kvaluesArrayFieldId = env->GetFieldID(cls, "intKvalues", "[I");
     if (kvaluesArrayFieldId == NULL) {
       LOG(FATAL) << "can not find field intKvalues" <<endl;
       return -1;
     }
  }

  static jfieldID voffsetsArrayFieldId = NULL; 
  if (voffsetsArrayFieldId == NULL) {
     voffsetsArrayFieldId = env->GetFieldID(cls, "voffsets", "[I");
     if (voffsetsArrayFieldId  == NULL) {
      LOG(FATAL) << "can not find field voffsets" <<endl;
      return -1;
     }
  }

  //"Z" is for boolean
  static jfieldID bufferExceededFieldId = NULL; 
  if (bufferExceededFieldId == NULL) {
    bufferExceededFieldId  = env->GetFieldID(cls, "bufferExceeded", "Z");
    if (bufferExceededFieldId == NULL) {
      LOG(FATAL) << "can not find field bufferExceeded" << endl;
      return -1;
    }
  }
  
  //(2.1) kvalues
  jint *kvaluesArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));
  for (int i=0; i<actualNumberOfKVs; i++) {
     kvaluesArray[i]= resultHolder.keys[i].key;
     VLOG(3) << "***in JNI getKVPairs with int keys, retrieved i=" << i<< " key value: " << kvaluesArray[i];
  }

  jintArray kvaluesArrayVal = env->NewIntArray(actualNumberOfKVs);
  if (kvaluesArrayVal == NULL) {
    LOG(FATAL) <<"cannot create kvalues int arrary" <<endl;
    return -1;
  }

  env->SetIntArrayRegion(kvaluesArrayVal, 0, actualNumberOfKVs, kvaluesArray);

  VLOG(2) << "***in JNI getKVPairs with int keys, finished assigning the kvalues array***";   

  //(2.2)voffsets. Note that we only need the boundary of the two value groups, as the de-serialization
  // knows how to do de-serialization by itself.
  unsigned char *buffer = (unsigned char*)env->GetDirectBufferAddress(byteBuffer);
  jint *voffsetsArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));

  //(3) convert the values stored in extensible server to the linear single bytebuffer.
  jboolean bufferExceeded = false;
  int accumulated_size=0;
  ExtensibleByteBuffers *bufferManager = shuffleStoreWithIntKeys->getBufferMgr();

  for (int i=0; i<actualNumberOfKVs; i++) {
     size_t start_position = resultHolder.keys[i].start_position;
     size_t end_position = resultHolder.keys[i].end_position;
     //check buffer will not exceed 
     for (size_t p=start_position; p<end_position; p++) {
       accumulated_size += resultHolder.kvaluesGroups[p].value_size;
     }

     if (accumulated_size > buffer_capacity) {
        bufferExceeded = true;
        break; //abort the update of the bytebuffer array.
     }

     VLOG(3) << "***in JNI getKVPairs with int keys, retrieved i=" << i<< " key value with corresponding value vector: ";
     VLOG(3) <<" ***in JNI getKVPairs with int keys, start-position= " << start_position << " end-position = " << end_position;
     for (size_t p=start_position; p<end_position; p++) {
	   VLOG(3) << "*****in JNI getKVPairs with int keys, retrieved position: "
                   << resultHolder.kvaluesGroups[p].position_in_start_buffer 
                  << " value size: " << resultHolder.kvaluesGroups[p].value_size; 
           bufferManager->retrieve(resultHolder.kvaluesGroups[p], buffer);
           buffer += resultHolder.kvaluesGroups[p].value_size; 
    }

    voffsetsArray[i] = accumulated_size; 
    //the problem that I have at this time: 
    VLOG(3) << "*****in JNI getKVPairs with int keys, final offset position: " << voffsetsArray[i];
  }

  VLOG(2) << "***in JNI getKVPairs with int keys, finished constructing the voffset array***";   

  jintArray voffsetsArrayVal = env->NewIntArray(actualNumberOfKVs);
  if (voffsetsArrayVal == NULL) {
    LOG(FATAL) << "can not create voffsets int array" << endl;
    return -1;
  }
  env->SetIntArrayRegion(voffsetsArrayVal, 0, actualNumberOfKVs, voffsetsArray); 

  VLOG(2) << "***in JNI getKVPairs with int keys, finished assigning voffset array***";   

  //(3) set all of the fields
  env->SetObjectField (mergeResult, kvaluesArrayFieldId, kvaluesArrayVal);
  env->SetObjectField (mergeResult, voffsetsArrayFieldId, voffsetsArrayVal);
  //with extensible bytebuffer manager, we will never get the buffer exceeded. what will be extended
  //might be the non-extensible byte buffer that works a a carrier for this batch of the data.
  env->SetBooleanField(mergeResult, bufferExceededFieldId, bufferExceeded);

  VLOG(2) << "***in JNI getKVPairs with int keys, retrieved buffer exceeded flag is: " << (bool)bufferExceeded;
 
  //(4) finally free some intermediate pointers
  free(kvaluesArray);
  free(voffsetsArray);
  
  VLOG(2) << "***in JNI getKVPairs with int keys, free allocated kvalues and voffsets array: "; 

  return actualNumberOfKVs;
}


/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    nGetSimpleKVPairsWithIntKeys
 * Signature: (JLjava/nio/ByteBuffer;IILcom/hp/hpl/firesteel/shuffle/ShuffleDataModel/MergeSortedResult;)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_nGetSimpleKVPairsWithIntKeys
(JNIEnv *env, jobject obj, jlong ptrToReduceStore, jobject byteBuffer, jint buffer_capacity,
   jint kmaxNumbers, jobject mergeResult){
   int actualNumberOfKVs = 0;

   //NOTE: byteBuffer is not used, this is assumed to be the same as the one passed to sort-merge method call earlier.
   VLOG(2) << "***in JNI getSimpleKVPairs with int keys, specified max number of KVs: " << kmaxNumbers;

   //(1)now populate the merge result object. it is with type of ShuffleDataModel.MergeSortedResult in Java.
   jclass cls = env->GetObjectClass(mergeResult);
   //(2)I only care about int kValues  and Voffsets and bufferExceeded
   static jfieldID kvaluesArrayFieldId = NULL;
   if (kvaluesArrayFieldId == NULL) {
     kvaluesArrayFieldId = env->GetFieldID(cls, "intKvalues", "[I");
     if (kvaluesArrayFieldId == NULL) {
       LOG(FATAL) << "can not find field intKvalues" <<endl;
       return -1;
     }
   }

   static jfieldID voffsetsArrayFieldId = NULL; 
   if (voffsetsArrayFieldId == NULL) {
     voffsetsArrayFieldId = env->GetFieldID(cls, "voffsets", "[I");
     if (voffsetsArrayFieldId  == NULL) {
      LOG(FATAL) << "can not find field voffsets" <<endl;
      return -1;
     }
   }

   //"Z" is for boolean
   static jfieldID bufferExceededFieldId = NULL; 
   if (bufferExceededFieldId == NULL) {
     bufferExceededFieldId  = env->GetFieldID(cls, "bufferExceeded", "Z");
     if (bufferExceededFieldId == NULL) {
        LOG(FATAL) << "can not find field bufferExceeded" << endl;
        return -1;
     }
   }
   
   //for sure, this is an int-key based reduce shuffle store, return the actual number of the k-vs.
   ReduceShuffleStoreWithIntKey *shuffleStoreWithIntKeys = 
                       reinterpret_cast<ReduceShuffleStoreWithIntKey *> (ptrToReduceStore);

   jint *kvaluesArray = nullptr;
   jint *voffsetsArray = nullptr;

   // we can not do reset for each batch of get k-values, as the pending elements in merge-sort network 
   // requires the buffer to not be reset (otherwise, data gets lost).
   // BufferManager *bufferManager = shuffleStoreWithIntKeys->getBufferMgr();
   // bufferManager->reset(); //to get to the ByteBuffer's initial position.
 
   //before doing the retrieval, make sure that the buffer capacity is what has been allocated and expected with size
   //CHECK_EQ (buffer_capacity, SHMShuffleGlobalConstants::SERIALIZATION_BUFFER_SIZE)
   //         << " buffer capacity: " << buffer_capacity << " does not match specifized size: "
   //         << SHMShuffleGlobalConstants::SERIALIZATION_BUFFER_SIZE;

   //int reducerId = shuffleStoreWithIntKeys->getReducerId();
   jboolean bufferExceeded = false; 

   bool ordering = shuffleStoreWithIntKeys->needsOrdering(); 
   if (!ordering) {
     IntKeyWithFixedLength::PassThroughMapBuckets& resultHolder = shuffleStoreWithIntKeys->getPassThroughResultHolder();
     shuffleStoreWithIntKeys->reset_passthroughresult();
     actualNumberOfKVs = shuffleStoreWithIntKeys->retrieve_passthroughmapbuckets(kmaxNumbers);

     VLOG(2) << "***in JNI getSimpleKVPairs with int keys, finished retrieve_passthroughmapbuckets with number of KVs: " << actualNumberOfKVs;   

     //(2.1) kvalues
     kvaluesArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));
     //(2.2)voffsets. Note that we only need the boundary of the values, as the de-serialization
     // knows how to do de-serialization by itself.
     voffsetsArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));

     for (int i=0; i<actualNumberOfKVs; i++) {
       kvaluesArray[i]= resultHolder.keyAndValueOffsets[i].key;
       voffsetsArray[i] = resultHolder.keyAndValueOffsets[i].offset;
       VLOG(3) << "***in JNI getSimpleKVPairs with int keys, retrieved i=" << i<< " key value: " << kvaluesArray[i]
             << " with value at offset: " << voffsetsArray[i] <<endl;
     }
   }//end of direct pass-through
   else {
     IntKeyWithFixedLength::MergeSortedMapBuckets& resultHolder = shuffleStoreWithIntKeys->getMergedResultHolder();
     shuffleStoreWithIntKeys->reset_mergedresult();
     actualNumberOfKVs = shuffleStoreWithIntKeys->retrieve_mergesortedmapbuckets(kmaxNumbers);
     VLOG(2) << "***in JNI getSimpleKVPairs with byte-array keys, ordering required, finished retrieving number of KVs: "
	     << actualNumberOfKVs;

     //(2.1) kvalues
     kvaluesArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));
     //(2.2)voffsets. Note that we only need the boundary of the values, as the de-serialization
     // knows how to do de-serialization by itself.
     voffsetsArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));

     unsigned char *buffer = (unsigned char*)env->GetDirectBufferAddress(byteBuffer);
     int accumulated_size=0;

     ExtensibleByteBuffers *vBufferManager = shuffleStoreWithIntKeys->getBufferMgr();

     for (int i=0; i<actualNumberOfKVs; i++) {
       kvaluesArray[i] = resultHolder.keys[i].key;
       {
         size_t start_position = resultHolder.keys[i].start_position;
         size_t end_position = resultHolder.keys[i].end_position;
	 //check buffer will not exceed
         int vaccumulated_size = accumulated_size;
	 //I only have one position in this situation
         for (size_t p=start_position; p<end_position;p++) {
	   vaccumulated_size  += resultHolder.kvaluesGroups[p].value_size;
	 }

         if (vaccumulated_size > buffer_capacity) {
	   bufferExceeded = true;
           break; 
	 }

	 VLOG(3) << "***in JNI getSimpleKVPairs with int keys, retrieved i=" << i
		 << " key value with corresponding value vector: ";
         //I only have one position in  this situation, thus start-position equal to end_position
	 VLOG(3) <<" ***in JNI getSimpleKVPairs with int keys, start-position= " << start_position
		 << " end-position = " << end_position;

         //I only have one position in this situation
         for (size_t p=start_position; p<end_position; p++) {
	   VLOG(3) << "*****in JNI getKVPairs with int keys, retrieved position: "
		   << resultHolder.kvaluesGroups[p].position_in_start_buffer
		   << " value size: " << resultHolder.kvaluesGroups[p].value_size;
           vBufferManager->retrieve(resultHolder.kvaluesGroups[p], buffer);
           buffer += resultHolder.kvaluesGroups[p].value_size; 
           accumulated_size  += resultHolder.kvaluesGroups[p].value_size;

	 }

         voffsetsArray[i] = accumulated_size;
	 VLOG(3) << "*****in JNI getKVPairs with int keys, final offset position: " << voffsetsArray[i];
       }
     }
     
   }//end of ordering.

  jintArray kvaluesArrayVal = env->NewIntArray(actualNumberOfKVs);
  if (kvaluesArrayVal == NULL) {
    LOG(FATAL) <<"cannot create kvalues int arrary" <<endl;
    return -1;
  }

  env->SetIntArrayRegion(kvaluesArrayVal, 0, actualNumberOfKVs, kvaluesArray);
  VLOG(2) << "***in JNI getSimpleKVPairs with int keys, finished assigning the kvalues array***"<< endl;   

  jintArray voffsetsArrayVal = env->NewIntArray(actualNumberOfKVs);
  if (voffsetsArrayVal == NULL) {
    LOG(FATAL) << "can not create voffsets int array" << endl;
    return -1;
  }
  env->SetIntArrayRegion(voffsetsArrayVal, 0, actualNumberOfKVs, voffsetsArray); 

  VLOG(2) << "***in JNI getKVPairs with int keys, finished assigning voffset array***";   


  //(3) set all of the fields
  env->SetObjectField (mergeResult, kvaluesArrayFieldId, kvaluesArrayVal);
  env->SetObjectField (mergeResult, voffsetsArrayFieldId, voffsetsArrayVal);
  //with extensible bytebuffer manager, we will never get the buffer exceeded. what will be extended
  //might be the non-extensible byte buffer that works a a carrier for this batch of the data.

  //(4) convert the values stored in extensible server to the linear single bytebuffer.
  //at this time, I take care of it by throw exception when the buffer exceeds the size at C++ side.
  env->SetBooleanField(mergeResult, bufferExceededFieldId, bufferExceeded);

  VLOG(2) << "***in JNI getKVPairs with int keys, retrieved buffer exceeded flag is: " << (bool)bufferExceeded;
 
  //(5) finally free some intermediate pointers
  free(kvaluesArray);
  free(voffsetsArray);
  
  VLOG(2) << "***in JNI getSimpleKVPairs with int keys, free allocated kvalues and voffsets array: "; 

  return actualNumberOfKVs;
}


/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    nGetKVPairsWithFloatKeys
 * Signature: (JLjava/nio/ByteBuffer;IILcom/hp/hpl/firesteel/shuffle/ShuffleDataModel/MergeSortedResult;)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_nGetKVPairsWithFloatKeys
(JNIEnv *env, jobject obj, jlong ptrToShuffleStore, jobject byteBuffer, jint buffer_capacity,
  jint knumbers, jobject mergeResult){

  {

    const char *exClassName = "java/lang/UnsupportedOperationException";
    jclass ecls = env->FindClass (exClassName);
    if (ecls != NULL){
      env->ThrowNew(ecls, "nstoreKVPairs for arbitrary <k,v> is not supported");
    }

  }
  return 0;
}


/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    nGetSimpleKVPairsWithFloatKeys
 * Signature: (JLjava/nio/ByteBuffer;IILcom/hp/hpl/firesteel/shuffle/ShuffleDataModel/MergeSortedResult;)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_nGetSimpleKVPairsWithFloatKeys
(JNIEnv *env, jobject obj, jlong ptrToShuffleStore, jobject byteBuffer, jint buffer_capacity,
   jint knumbers, jobject mergeResult){

  {

    const char *exClassName = "java/lang/UnsupportedOperationException";
    jclass ecls = env->FindClass (exClassName);
    if (ecls != NULL){
      env->ThrowNew(ecls, "nstoreKVPairs for arbitrary <k,v> is not supported");
    }

  }
  return 0;
}



/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    nGetKVPairsWithLongKeys
 * Signature: (JLjava/nio/ByteBuffer;IILcom/hp/hpl/firesteel/shuffle/ShuffleDataModel/MergeSortedResult;)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_nGetKVPairsWithLongKeys
(JNIEnv *env, jobject obj, jlong ptrToReduceStore, jobject byteBuffer, jint buffer_capacity,
 jint kmaxNumbers, jobject mergeResult) {

  int actualNumberOfKVs =0;
  //NOTE: byteBuffer is not used, this is assumed to be the same as the one passed to sort-merge method call earlier.
  VLOG(2) << "***in JNI getKVPairs with long keys, specified max number of KVs: " << kmaxNumbers;

  //(1)now populate the merge result object. it is with type of ShuffleDataModel.MergeSortedResult in Java.
  jclass cls = env->GetObjectClass(mergeResult);
  //(2)I only care about int kValues  and Voffsets and bufferExceeded
  static jfieldID kvaluesArrayFieldId = NULL;
  if (kvaluesArrayFieldId == NULL) {
     kvaluesArrayFieldId = env->GetFieldID(cls, "longKvalues", "[J");
     if (kvaluesArrayFieldId == NULL) {
       LOG(FATAL) << "can not find field longKvalues" <<endl;
       return -1;
     }
  }

  static jfieldID voffsetsArrayFieldId = NULL; 
  if (voffsetsArrayFieldId == NULL) {
     voffsetsArrayFieldId = env->GetFieldID(cls, "voffsets", "[I");
     if (voffsetsArrayFieldId  == NULL) {
      LOG(FATAL) << "can not find field voffsets" <<endl;
      return -1;
     }
  }

  //"Z" is for boolean
  static jfieldID bufferExceededFieldId = NULL; 
  if (bufferExceededFieldId == NULL) {
    bufferExceededFieldId  = env->GetFieldID(cls, "bufferExceeded", "Z");
    if (bufferExceededFieldId == NULL) {
      LOG(FATAL) << "can not find field bufferExceeded" << endl;
      return -1;
    }
  }

  GenericReduceShuffleStore *gResultStore = 
                  reinterpret_cast<GenericReduceShuffleStore *> (ptrToReduceStore);
  if (gResultStore->getKValueType().typeId == KValueTypeId::Long) {
   
     //for sure, this is a long-key based reduce shuffle store, return the actual number of the k-vs.
     ReduceShuffleStoreWithLongKey *shuffleStoreWithLongKeys = 
                       dynamic_cast<ReduceShuffleStoreWithLongKey *> (gResultStore);
    // we can not do reset for each batch of get k-values, as the pending elements in merge-sort network 
    // requires the buffer to not be reset (otherwise, data gets lost).
    // BufferManager *bufferManager = shuffleStoreWithIntKeys->getBufferMgr();
    // bufferManager->reset(); //to get to the ByteBuffer's initial position.

    //int reducerId = shuffleStoreWithIntKeys->getReducerId();
    LongKeyWithFixedLength::MergeSortedMapBuckets& resultHolder=shuffleStoreWithLongKeys->getMergedResultHolder();
    shuffleStoreWithLongKeys->reset_mergedresult();
    actualNumberOfKVs = shuffleStoreWithLongKeys->retrieve_mergesortedmapbuckets(kmaxNumbers);

    VLOG(2) << "***in JNI getKVPairs with long keys, finished retrieve_mergesort with number of KVs: " << actualNumberOfKVs;   
  
    //(2.1) kvalues with long type
    jlong *kvaluesArray = (jlong*) malloc (actualNumberOfKVs* sizeof(long));
    for (int i=0; i<actualNumberOfKVs; i++) {
      kvaluesArray[i]= resultHolder.keys[i].key;
      VLOG(3) << "***in JNI getKVPairs with long keys, retrieved i=" << i<< " key value: " << kvaluesArray[i];
   }

    jlongArray kvaluesArrayVal = env->NewLongArray(actualNumberOfKVs);
    if (kvaluesArrayVal == NULL) {
       LOG(FATAL) <<"cannot create kvalues long arrary" <<endl;
       return -1;
    }

    env->SetLongArrayRegion(kvaluesArrayVal, 0, actualNumberOfKVs, kvaluesArray);

    VLOG(2) << "***in JNI getKVPairs with long keys, finished assigning the kvalues array***";   

    //(2.2)voffsets. Note that we only need the boundary of the two value groups, as the de-serialization
    // knows how to do de-serialization by itself.
    unsigned char *buffer = (unsigned char*)env->GetDirectBufferAddress(byteBuffer);
    jint *voffsetsArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));

    //(3) convert the values stored in extensible server to the linear single bytebuffer.
    jboolean bufferExceeded = false;
    int accumulated_size=0;
    ExtensibleByteBuffers *bufferManager = shuffleStoreWithLongKeys->getBufferMgr();

    for (int i=0; i<actualNumberOfKVs; i++) {
       size_t start_position = resultHolder.keys[i].start_position;
       size_t end_position = resultHolder.keys[i].end_position;
       //check buffer will not exceed 
       for (size_t p=start_position; p<end_position; p++) {
         accumulated_size += resultHolder.kvaluesGroups[p].value_size;
       }

       if (accumulated_size > buffer_capacity) {
          bufferExceeded = true;
          break; //abort the update of the bytebuffer array.
       }

       VLOG(3) << "***in JNI getKVPairs with long keys, retrieved i=" << i<< " key value with corresponding value vector: ";
       VLOG(3) <<" ***in JNI getKVPairs with long keys, start-position= " << start_position << " end-position = " << end_position;
       for (size_t p=start_position; p<end_position; p++) {
	   VLOG(3) << "*****in JNI getKVPairs with long keys, retrieved position: "
                   << resultHolder.kvaluesGroups[p].position_in_start_buffer 
                  << " value size: " << resultHolder.kvaluesGroups[p].value_size; 
           bufferManager->retrieve(resultHolder.kvaluesGroups[p], buffer);
           buffer += resultHolder.kvaluesGroups[p].value_size; 
       }

       voffsetsArray[i] = accumulated_size; 
       //the problem that I have at this time: 
       VLOG(3) << "*****in JNI getKVPairs with long keys, final offset position: " << voffsetsArray[i];
    }

    VLOG(2) << "***in JNI getKVPairs with long keys, finished constructing the voffset array***";   

    jintArray voffsetsArrayVal = env->NewIntArray(actualNumberOfKVs);
    if (voffsetsArrayVal == NULL) {
      LOG(FATAL) << "can not create voffsets int array" << endl;
      return -1;
    }
    env->SetIntArrayRegion(voffsetsArrayVal, 0, actualNumberOfKVs, voffsetsArray); 

    VLOG(2) << "***in JNI getKVPairs with long keys, finished assigning voffset array***";   

    //(3) set all of the fields
    env->SetObjectField (mergeResult, kvaluesArrayFieldId, kvaluesArrayVal);
    env->SetObjectField (mergeResult, voffsetsArrayFieldId, voffsetsArrayVal);
    //with extensible bytebuffer manager, we will never get the buffer exceeded. what will be extended
    //might be the non-extensible byte buffer that works a a carrier for this batch of the data.
    env->SetBooleanField(mergeResult, bufferExceededFieldId, bufferExceeded);

    VLOG(2) << "***in JNI getKVPairs with long keys, retrieved buffer exceeded flag is: " << (bool)bufferExceeded;
 
    //(4) finally free some intermediate pointers
    free(kvaluesArray);
    free(voffsetsArray);
  
    VLOG(2) << "***in JNI getKVPairs with long keys, free allocated kvalues and voffsets array: "; 
  }
  else {
     //NOTE: what is done in this scope is to create empty arrays as the return to the Java side.
 
     VLOG(2) << "***in Jni GetKVParis with long keys, actual reduce-shuffle store is with int-key due to empty buckets";
     //actualNumberOfKVs is 0.
     VLOG(2) << "***in JNI getKVPairs with long keys, finished retrieve_mergesort with number of KVs: " << actualNumberOfKVs;   
  
     //(2.1) kvalues with long type
     jlong *kvaluesArray = (jlong*) malloc (actualNumberOfKVs* sizeof(long));

     jlongArray kvaluesArrayVal = env->NewLongArray(actualNumberOfKVs);
     if (kvaluesArrayVal == NULL) {
         LOG(FATAL) <<"cannot create kvalues long arrary with size of: " << actualNumberOfKVs <<endl;
         return -1;
     }

     env->SetLongArrayRegion(kvaluesArrayVal, 0, actualNumberOfKVs, kvaluesArray);

     VLOG(2) << "***in JNI getKVPairs with long keys, finished assigning the kvalues array***";   

     //(2.2)voffsets. Note that we only need the boundary of the two value groups, as the de-serialization
     // knows how to do de-serialization by itself.
     jint *voffsetsArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));

     //(3) convert the values stored in extensible server to the linear single bytebuffer.
     jboolean bufferExceeded = false;

     VLOG(2) << "***in JNI getKVPairs with long keys, finished constructing the voffset array***";   

     jintArray voffsetsArrayVal = env->NewIntArray(actualNumberOfKVs);
     if (voffsetsArrayVal == NULL) {
       LOG(FATAL) << "can not create voffsets int array" << endl;
       return -1;
     }
     env->SetIntArrayRegion(voffsetsArrayVal, 0, actualNumberOfKVs, voffsetsArray); 

     VLOG(2) << "***in JNI getKVPairs with int keys, finished assigning voffset array***";   

     //(3) set all of the fields
     env->SetObjectField (mergeResult, kvaluesArrayFieldId, kvaluesArrayVal);
     env->SetObjectField (mergeResult, voffsetsArrayFieldId, voffsetsArrayVal);
     //with extensible bytebuffer manager, we will never get the buffer exceeded. what will be extended
     //might be the non-extensible byte buffer that works a a carrier for this batch of the data.
     env->SetBooleanField(mergeResult, bufferExceededFieldId, bufferExceeded);

     VLOG(2) << "***in JNI getKVPairs with long keys, retrieved buffer exceeded flag is: " << (bool)bufferExceeded;
 
     //(4) finally free some intermediate pointers
     free(kvaluesArray);
     free(voffsetsArray);
  
     VLOG(2) << "***in JNI getKVPairs with long keys, free allocated kvalues and voffsets array: "; 

  }

  return actualNumberOfKVs;

}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    nGetSimpleKVPairsWithLongKeys
 * Signature: (JLjava/nio/ByteBuffer;IILcom/hp/hpl/firesteel/shuffle/ShuffleDataModel/MergeSortedResult;)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_nGetSimpleKVPairsWithLongKeys
(JNIEnv *env, jobject obj, jlong ptrToReduceStore, jobject byteBuffer, jint buffer_capacity, 
  jint kmaxNumbers, jobject mergeResult){

  int actualNumberOfKVs = 0;

  //NOTE: byteBuffer is not used, this is assumed to be the same as the one passed to sort-merge method call earlier.
  VLOG(2) << "***in JNI getSimpleKVPairs with long keys, specified max number of KVs: " << kmaxNumbers;
 
   //(1)now populate the merge result object. it is with type of ShuffleDataModel.MergeSortedResult in Java.
  jclass cls = env->GetObjectClass(mergeResult);
  //(2)I only care about int kValues  and Voffsets and bufferExceeded
  static jfieldID kvaluesArrayFieldId = NULL;
  if (kvaluesArrayFieldId == NULL) {
     kvaluesArrayFieldId = env->GetFieldID(cls, "longKvalues", "[J");
     if (kvaluesArrayFieldId == NULL) {
       LOG(FATAL) << "can not find field longKvalues" <<endl;
       return -1;
     }
  }

  static jfieldID voffsetsArrayFieldId = NULL; 
  if (voffsetsArrayFieldId == NULL) {
     voffsetsArrayFieldId = env->GetFieldID(cls, "voffsets", "[I");
     if (voffsetsArrayFieldId  == NULL) {
      LOG(FATAL) << "can not find field voffsets" <<endl;
      return -1;
     }
  }

  //"Z" is for boolean
  static jfieldID bufferExceededFieldId = NULL; 
  if (bufferExceededFieldId == NULL) {
    bufferExceededFieldId  = env->GetFieldID(cls, "bufferExceeded", "Z");
    if (bufferExceededFieldId == NULL) {
      LOG(FATAL) << "can not find field bufferExceeded" << endl;
      return -1;
    }
  }
 
   GenericReduceShuffleStore *gResultStore = 
                       reinterpret_cast<GenericReduceShuffleStore *> (ptrToReduceStore);

   jlong *kvaluesArray = nullptr;
   jint *voffsetsArray = nullptr;
   jboolean bufferExceeded = false;
   
   if (gResultStore->getKValueType().typeId == KValueTypeId::Long) {
     //for sure, this is an int-key based reduce shuffle store, return the actual number of the k-vs.
     ReduceShuffleStoreWithLongKey *shuffleStoreWithLongKeys = 
                       dynamic_cast<ReduceShuffleStoreWithLongKey *> (gResultStore);
     // we can not do reset for each batch of get k-values, as the pending elements in merge-sort network 
     // requires the buffer to not be reset (otherwise, data gets lost).
     // BufferManager *bufferManager = shuffleStoreWithIntKeys->getBufferMgr();
     // bufferManager->reset(); //to get to the ByteBuffer's initial position.

     //before doing the retrieval, make sure that the buffer capacity is what has been allocated and expected with size 
     //CHECK_EQ (buffer_capacity, SHMShuffleGlobalConstants::SERIALIZATION_BUFFER_SIZE)
     //        << " buffer capacity: " << buffer_capacity << " does not match specifized size: "
     //        << SHMShuffleGlobalConstants::SERIALIZATION_BUFFER_SIZE;
 
     //int reducerId = shuffleStoreWithIntKeys->getReducerId();
     bool ordering = shuffleStoreWithLongKeys->needsOrdering();
    
     if (!ordering) {
        LongKeyWithFixedLength::PassThroughMapBuckets& resultHolder = shuffleStoreWithLongKeys->getPassThroughResultHolder();
        shuffleStoreWithLongKeys->reset_passthroughresult();
        actualNumberOfKVs = shuffleStoreWithLongKeys->retrieve_passthroughmapbuckets(kmaxNumbers);

        VLOG(2) << "***in JNI getSimpleKVPairs with long keys, finished retrieve_passthroughmapbuckets with number of KVs: " 
             << actualNumberOfKVs;   

  
        //(2.1) kvalues
        kvaluesArray = (jlong*) malloc (actualNumberOfKVs* sizeof(long));
        //(2.2)voffsets. Note that we only need the boundary of the values, as the de-serialization
        // knows how to do de-serialization by itself.
        voffsetsArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));

        for (int i=0; i<actualNumberOfKVs; i++) {
            kvaluesArray[i]= resultHolder.keyAndValueOffsets[i].key;
            voffsetsArray[i] = resultHolder.keyAndValueOffsets[i].offset;
            VLOG(3) << "***in JNI getSimpleKVPairs with long keys, retrieved i=" << i<< " key value: " << kvaluesArray[i]
                    << " with value at offset: " << voffsetsArray[i] <<endl;
        }
     }
     else{
         LongKeyWithFixedLength::MergeSortedMapBuckets& resultHolder= shuffleStoreWithLongKeys->getMergedResultHolder();
         shuffleStoreWithLongKeys->reset_mergedresult();
         actualNumberOfKVs = shuffleStoreWithLongKeys->retrieve_mergesortedmapbuckets(kmaxNumbers);
         VLOG(2) << "***in JNI getSimpleKVPairs with long keys, ordering required, finished retrieving number of KVs: "
		 << actualNumberOfKVs;

        //(2.1) kvalues
        kvaluesArray = (jlong*) malloc (actualNumberOfKVs* sizeof(long));
        //(2.2)voffsets. Note that we only need the boundary of the values, as the de-serialization
        // knows how to do de-serialization by itself.
        voffsetsArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));
        
	unsigned char *buffer = (unsigned char*)env->GetDirectBufferAddress(byteBuffer);
	int accumulated_size=0;

	ExtensibleByteBuffers *vBufferManager = shuffleStoreWithLongKeys->getBufferMgr();

        for (int i=0; i<actualNumberOfKVs; i++) {
	  kvaluesArray[i] = resultHolder.keys[i].key;
          {
	    size_t start_position = resultHolder.keys[i].start_position;
	    size_t end_position = resultHolder.keys[i].end_position;
	    //check buffer will not exceed                                                                                                                
	    int vaccumulated_size = accumulated_size;
	    //I only have one position in this situation                                                                                                  
	    for (size_t p=start_position; p<end_position;p++) {
	      vaccumulated_size  += resultHolder.kvaluesGroups[p].value_size;
	    }

	    if (vaccumulated_size > buffer_capacity) {
	      bufferExceeded = true;
	      break;
	    }

	    VLOG(3) << "***in JNI getSimpleKVPairs with long keys, retrieved i=" << i
		    << " key value with corresponding value vector: ";
	    //I only have one position in  this situation, thus start-position equal to end_position 
            VLOG(3) <<" ***in JNI getSimpleKVPairs with long keys, start-position= " << start_position
		    << " end-position = " << end_position;
            
            //I only have one position in this situation 
            for (size_t p=start_position; p<end_position; p++) {
	      VLOG(3) << "*****in JNI getKVPairs with long keys, retrieved position: "
		      << resultHolder.kvaluesGroups[p].position_in_start_buffer
		      << " value size: " << resultHolder.kvaluesGroups[p].value_size;
	      vBufferManager->retrieve(resultHolder.kvaluesGroups[p], buffer);
	      buffer += resultHolder.kvaluesGroups[p].value_size;
	      accumulated_size  += resultHolder.kvaluesGroups[p].value_size;

	    }

            voffsetsArray[i] = accumulated_size;
	    VLOG(3) << "*****in JNI getKVPairs with long keys, final offset position: " << voffsetsArray[i];


	  }
	}
         
     }//end of ordering


    jlongArray kvaluesArrayVal = env->NewLongArray(actualNumberOfKVs);
    if (kvaluesArrayVal == NULL) {
      LOG(FATAL) <<"cannot create kvalues long arrary" <<endl;
      return -1;
    }

    env->SetLongArrayRegion(kvaluesArrayVal, 0, actualNumberOfKVs, kvaluesArray);
    VLOG(2) << "***in JNI getSimpleKVPairs with long keys, finished assigning the kvalues array***"<< endl;   

    jintArray voffsetsArrayVal = env->NewIntArray(actualNumberOfKVs);
    if (voffsetsArrayVal == NULL) {
       LOG(FATAL) << "can not create voffsets int array" << endl;
       return -1;
    }
    env->SetIntArrayRegion(voffsetsArrayVal, 0, actualNumberOfKVs, voffsetsArray); 

    VLOG(2) << "***in JNI getKVPairs with long keys, finished assigning voffset array***";   


    //(3) set all of the fields
    env->SetObjectField (mergeResult, kvaluesArrayFieldId, kvaluesArrayVal);
    env->SetObjectField (mergeResult, voffsetsArrayFieldId, voffsetsArrayVal);
    //with extensible bytebuffer manager, we will never get the buffer exceeded. what will be extended
    //might be the non-extensible byte buffer that works a a carrier for this batch of the data.

    //(4) convert the values stored in extensible server to the linear single bytebuffer.
    //at this time, I take care of it by throw exception when the buffer exceeds the size at C++ side.
    bufferExceeded = false; 
    env->SetBooleanField(mergeResult, bufferExceededFieldId, bufferExceeded);

    VLOG(2) << "***in JNI getKVPairs with long keys, retrieved buffer exceeded flag is: " << (bool)bufferExceeded;
 
    //(5) finally free some intermediate pointers
    free(kvaluesArray);
    free(voffsetsArray);
  
    VLOG(2) << "***in JNI getSimpleKVPairs with long keys, free allocated kvalues and voffsets array: "; 
  }
  else {
    //NOTE: what is done in this scope is to create empty arrays as the return to the Java side.
    VLOG(2) << "***in Jni GetSimpleKVParis with long keys, actual reduce-shuffle store is with int-key due to empty buckets";
    // actualNumberOfKVs is 0;   
  
    //(2.1) kvalues
    kvaluesArray = (jlong*) malloc (actualNumberOfKVs* sizeof(long));
    //(2.2)voffsets. Note that we only need the boundary of the values, as the de-serialization
    // knows how to do de-serialization by itself.
    voffsetsArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));

    jlongArray kvaluesArrayVal = env->NewLongArray(actualNumberOfKVs);
    if (kvaluesArrayVal == NULL) {
      LOG(FATAL) <<"cannot create kvalues long arrary" <<endl;
      return -1;
    }

    env->SetLongArrayRegion(kvaluesArrayVal, 0, actualNumberOfKVs, kvaluesArray);
    VLOG(2) << "***in JNI getSimpleKVPairs with long keys, finished assigning the kvalues array***"<< endl;   

    jintArray voffsetsArrayVal = env->NewIntArray(actualNumberOfKVs);
    if (voffsetsArrayVal == NULL) {
       LOG(FATAL) << "can not create voffsets int array" << endl;
       return -1;
    }
    env->SetIntArrayRegion(voffsetsArrayVal, 0, actualNumberOfKVs, voffsetsArray); 

    VLOG(2) << "***in JNI getSimpleKVPairs with long keys, finished assigning voffset array***";   


    //(3) set all of the fields
    env->SetObjectField (mergeResult, kvaluesArrayFieldId, kvaluesArrayVal);
    env->SetObjectField (mergeResult, voffsetsArrayFieldId, voffsetsArrayVal);
    //with extensible bytebuffer manager, we will never get the buffer exceeded. what will be extended
    //might be the non-extensible byte buffer that works a a carrier for this batch of the data.

    //(4) convert the values stored in extensible server to the linear single bytebuffer.
    //at this time, I take care of it by throw exception when the buffer exceeds the size at C++ side.
    bufferExceeded = false; 
    env->SetBooleanField(mergeResult, bufferExceededFieldId, bufferExceeded);

    VLOG(2) << "***in JNI getSimpleKVPairs with long keys, retrieved buffer exceeded flag is: " << (bool)bufferExceeded;
 
    //(5) finally free some intermediate pointers
    free(kvaluesArray);
    free(voffsetsArray);
  
    VLOG(2) << "***in JNI getSimpleKVPairs with long keys, free allocated kvalues and voffsets array: "; 
  }

  return actualNumberOfKVs;
}


/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    nGetKVPairsWithByteArrayKeys
 * Signature: (JLjava/nio/ByteBuffer;IILcom/hp/hpl/firesteel/shuffle/ShuffleDataModel/MergeSortedResult;)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_nGetKVPairsWithByteArrayKeys
(JNIEnv *env, jobject obj, jlong ptrToReduceStore, jobject byteBuffer, jint buffer_capacity,
 jint kmaxNumbers, jobject mergeResult) {

  int actualNumberOfKVs =0;
  //NOTE: byteBuffer is not used, this is assumed to be the same as the one passed to sort-merge method call earlier.
  VLOG(2) << "***in JNI getKVPairs with byte-array keys, specified max number of KVs: " << kmaxNumbers;

  //(1)now populate the merge result object. it is with type of ShuffleDataModel.MergeSortedResult in Java.
  jclass cls = env->GetObjectClass(mergeResult);
  //(2)I only care about int kValues  and Voffsets and bufferExceeded
  static jfieldID koffsetsArrayFieldId = NULL;
  if (koffsetsArrayFieldId  == NULL) {
     koffsetsArrayFieldId  = env->GetFieldID(cls, "koffsets", "[I");
     if (koffsetsArrayFieldId == NULL) {
       LOG(FATAL) << "can not find field koffsets" <<endl;
       return -1;
     }
  }

  static jfieldID voffsetsArrayFieldId = NULL; 
  if (voffsetsArrayFieldId == NULL) {
     voffsetsArrayFieldId = env->GetFieldID(cls, "voffsets", "[I");
     if (voffsetsArrayFieldId  == NULL) {
      LOG(FATAL) << "can not find field voffsets" <<endl;
      return -1;
     }
  }

  //"Z" is for boolean
  static jfieldID bufferExceededFieldId = NULL; 
  if (bufferExceededFieldId == NULL) {
    bufferExceededFieldId  = env->GetFieldID(cls, "bufferExceeded", "Z");
    if (bufferExceededFieldId == NULL) {
      LOG(FATAL) << "can not find field bufferExceeded" << endl;
      return -1;
    }
  }

  GenericReduceShuffleStore *gResultStore = 
                  reinterpret_cast<GenericReduceShuffleStore *> (ptrToReduceStore);
  if (gResultStore->getKValueType().typeId == KValueTypeId::ByteArray) {
   
     //for sure, this is a long-key based reduce shuffle store, return the actual number of the k-vs.
     ReduceShuffleStoreWithByteArrayKey *shuffleStoreWithByteArrayKeys = 
                       dynamic_cast<ReduceShuffleStoreWithByteArrayKey *> (gResultStore);
    // we can not do reset for each batch of get k-values, as the pending elements in merge-sort network 
    // requires the buffer to not be reset (otherwise, data gets lost).
    // BufferManager *bufferManager = shuffleStoreWithIntKeys->getBufferMgr();
    // bufferManager->reset(); //to get to the ByteBuffer's initial position.

    //int reducerId = shuffleStoreWithIntKeys->getReducerId();
    ByteArrayKeyWithVariableLength::MergeSortedMapBuckets& resultHolder=
                                              shuffleStoreWithByteArrayKeys->getMergedResultHolder();
    shuffleStoreWithByteArrayKeys->reset_mergedresult();
    actualNumberOfKVs = shuffleStoreWithByteArrayKeys->retrieve_mergesortedmapbuckets(kmaxNumbers);

    VLOG(2) << "***in JNI getKVPairs with byte-array keys, finished retrieve_mergesort with number of KVs: " 
            << actualNumberOfKVs;   

   
    unsigned char *buffer = (unsigned char*)env->GetDirectBufferAddress(byteBuffer);
  
    //(2.1) koffsets.
    jint *koffsetsArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));
    jint *voffsetsArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));

    jboolean bufferExceeded = false;
    int accumulated_size = 0;

    ExtensibleByteBuffers *kBufferManager = shuffleStoreWithByteArrayKeys->getKBufferMgr();
    ExtensibleByteBuffers *vBufferManager = shuffleStoreWithByteArrayKeys->getVBufferMgr();


    for (int i=0; i<actualNumberOfKVs; i++) {
      PositionInExtensibleByteBuffer cachedKeyValue = resultHolder.keys[i].cachedKeyValue;      
      kBufferManager->retrieve(cachedKeyValue, buffer);
      buffer += cachedKeyValue.value_size;
      accumulated_size   += cachedKeyValue.value_size; 

      if (accumulated_size > buffer_capacity) {
	bufferExceeded = true;
	break; //abort the update of the bytebuffer array.                                                                                
      }

      koffsetsArray[i] = accumulated_size; 

      VLOG(3) << "***in JNI getKVPairs with byte-array keys, retrieved i=" << i
              << " key value size: " << cachedKeyValue.value_size;

      //now the values corresponding to the same K.
      {
	size_t start_position = resultHolder.keys[i].start_position;
        size_t end_position = resultHolder.keys[i].end_position;
        //check buffer will not exceed
        int vaccumulated_size = accumulated_size; 
        for (size_t p=start_position; p<end_position; p++) {
	  vaccumulated_size += resultHolder.kvaluesGroups[p].valueSize;
	}

	if (vaccumulated_size > buffer_capacity) {
          bufferExceeded = true;
          break; //abort the update of the bytebuffer array
	}

	VLOG(3) << "***in JNI getKVPairs with byte-array keys, retrieved i=" << i 
                << " key value with corresponding value vector: ";
	VLOG(3) <<" ***in JNI getKVPairs with byte-array keys, start-position= " << start_position 
                << " end-position = " << end_position;

	for (size_t p=start_position; p<end_position; p++) {
	  VLOG(3) << "*****in JNI getKVPairs with byte-array keys, retrieved position: "
		  << resultHolder.kvaluesGroups[p].cachedValueValue.position_in_start_buffer
                  << " value size: " << resultHolder.kvaluesGroups[p].cachedValueValue.value_size;

          //NOTE: by-pass value reading from channel earlier, thus we will need to do memcpy here
          //vBufferManager->retrieve(resultHolder.kvaluesGroups[p].cachedValueValue, buffer);
          memcpy(buffer, resultHolder.kvaluesGroups[p].value,resultHolder.kvaluesGroups[p].cachedValueValue.value_size);
	  buffer += resultHolder.kvaluesGroups[p].cachedValueValue.value_size;
          accumulated_size += resultHolder.kvaluesGroups[p].cachedValueValue.value_size;
	}

        voffsetsArray[i]=accumulated_size;
        VLOG(3) << "*****in JNI getKVPairs with byte-array keys, final offset position: " << voffsetsArray[i];

      }

   }

    VLOG(2) << "***in JNI getKVPairs with byte-array keys, finished assigning the koffsets and voffsets array***";   


    jintArray koffsetsArrayVal = env->NewIntArray(actualNumberOfKVs);
    if (koffsetsArrayVal == NULL) {
      LOG(FATAL) << "can not create koffsets int array" << endl;
      return -1;
    }
    env->SetIntArrayRegion(koffsetsArrayVal, 0, actualNumberOfKVs, koffsetsArray); 

    VLOG(2) << "***in JNI getKVPairs with byte-array keys, finished assigning koffset array***";   

    jintArray voffsetsArrayVal = env->NewIntArray(actualNumberOfKVs);
    if (voffsetsArrayVal == NULL) {
      LOG(FATAL) << "can not create voffsets int array" << endl;
      return -1;
    }
    env->SetIntArrayRegion(voffsetsArrayVal, 0, actualNumberOfKVs, voffsetsArray); 

    VLOG(2) << "***in JNI getKVPairs with byte-array keys, finished assigning voffset array***";   


    //(3) set all of the fields
    env->SetObjectField (mergeResult, koffsetsArrayFieldId, koffsetsArrayVal);
    env->SetObjectField (mergeResult, voffsetsArrayFieldId, voffsetsArrayVal);
    //with extensible bytebuffer manager, we will never get the buffer exceeded. what will be extended
    //might be the non-extensible byte buffer that works a a carrier for this batch of the data.
    env->SetBooleanField(mergeResult, bufferExceededFieldId, bufferExceeded);

    VLOG(2) << "***in JNI getKVPairs with long keys, retrieved buffer exceeded flag is: " << (bool)bufferExceeded;
 
    //(4) finally free some intermediate pointers
    free(koffsetsArray);
    free(voffsetsArray);
  
    VLOG(2) << "***in JNI getKVPairs with byte-array keys, free allocated koffsets and voffsets array: "; 
  }
  else {
     //NOTE: what is done in this scope is to create empty arrays as the return to the Java side.
 
     VLOG(2) << "***in Jni GetKVParis with byte-array keys, actual reduce-shuffle store is with int-key due to empty buckets";
     //actualNumberOfKVs is 0.
     VLOG(2) << "***in JNI getKVPairs with byte-array keys, finished retrieve_mergesort with number of KVs: " << actualNumberOfKVs;   
  
     //(2.1) kvalue offset with int type
     jint *koffsetsArray = (jint*) malloc(actualNumberOfKVs*sizeof(int));
     jintArray koffsetsArrayVal = env->NewIntArray(actualNumberOfKVs);
     if (koffsetsArrayVal == NULL) {
       LOG(FATAL) << "can not create koffsets int array" << endl;
       return -1;
     }
     env->SetIntArrayRegion(koffsetsArrayVal, 0, actualNumberOfKVs, koffsetsArray); 

     VLOG(2) << "***in JNI getKVPairs with long keys, finished assigning the koffsets array***";   

     //(2.2)voffsets. Note that we only need the boundary of the two value groups, as the de-serialization
     // knows how to do de-serialization by itself.
     jint *voffsetsArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));

     //(3) convert the values stored in extensible server to the linear single bytebuffer.
     jboolean bufferExceeded = false;

     VLOG(2) << "***in JNI getKVPairs with long keys, finished constructing the voffset array***";   

     jintArray voffsetsArrayVal = env->NewIntArray(actualNumberOfKVs);
     if (voffsetsArrayVal == NULL) {
       LOG(FATAL) << "can not create voffsets int array" << endl;
       return -1;
     }
     env->SetIntArrayRegion(voffsetsArrayVal, 0, actualNumberOfKVs, voffsetsArray); 

     VLOG(2) << "***in JNI getKVPairs with byte-array keys, finished assigning voffset array***";   

     //(3) set all of the fields
     env->SetObjectField (mergeResult, koffsetsArrayFieldId, koffsetsArrayVal);
     env->SetObjectField (mergeResult, voffsetsArrayFieldId, voffsetsArrayVal);
     //with extensible bytebuffer manager, we will never get the buffer exceeded. what will be extended
     //might be the non-extensible byte buffer that works a a carrier for this batch of the data.
     env->SetBooleanField(mergeResult, bufferExceededFieldId, bufferExceeded);

     VLOG(2) << "***in JNI getKVPairs with byte-array keys, retrieved buffer exceeded flag is: " << (bool)bufferExceeded;
 
     //(4) finally free some intermediate pointers
     free(koffsetsArray);
     free(voffsetsArray);
  
     VLOG(2) << "***in JNI getKVPairs with byte-array keys, free allocated kvalues and voffsets array: "; 

  }

  return actualNumberOfKVs;

}


/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    nGetSimpleKVPairsWithByteArrayKeys
 * Signature: (JLjava/nio/ByteBuffer;IILcom/hp/hpl/firesteel/shuffle/ShuffleDataModel/MergeSortedResult;)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_nGetSimpleKVPairsWithByteArrayKeys
(JNIEnv *env, jobject obj, jlong ptrToReduceStore, jobject byteBuffer, jint buffer_capacity, 
  jint kmaxNumbers, jobject mergeResult){

  int actualNumberOfKVs = 0;

  //NOTE: byteBuffer is not used, this is assumed to be the same as the one passed to sort-merge method call earlier.
  VLOG(2) << "***in JNI getSimpleKVPairs with byte-array keys, specified max number of KVs: " << kmaxNumbers;
 
   //(1)now populate the merge result object. it is with type of ShuffleDataModel.MergeSortedResult in Java.
  jclass cls = env->GetObjectClass(mergeResult);
  //(2)I only care about int kValues  and Voffsets and bufferExceeded
  static jfieldID koffsetsArrayFieldId = NULL;
  if (koffsetsArrayFieldId  == NULL) {
     koffsetsArrayFieldId  = env->GetFieldID(cls, "koffsets", "[I");
     if (koffsetsArrayFieldId == NULL) {
       LOG(FATAL) << "can not find field koffsets" <<endl;
       return -1;
     }
  }

  static jfieldID voffsetsArrayFieldId = NULL; 
  if (voffsetsArrayFieldId == NULL) {
     voffsetsArrayFieldId = env->GetFieldID(cls, "voffsets", "[I");
     if (voffsetsArrayFieldId  == NULL) {
      LOG(FATAL) << "can not find field voffsets" <<endl;
      return -1;
     }
  }

  //"Z" is for boolean
  static jfieldID bufferExceededFieldId = NULL; 
  if (bufferExceededFieldId == NULL) {
    bufferExceededFieldId  = env->GetFieldID(cls, "bufferExceeded", "Z");
    if (bufferExceededFieldId == NULL) {
      LOG(FATAL) << "can not find field bufferExceeded" << endl;
      return -1;
    }
  }
 
   GenericReduceShuffleStore *gResultStore = 
                       reinterpret_cast<GenericReduceShuffleStore *> (ptrToReduceStore);
   if (gResultStore->getKValueType().typeId == KValueTypeId::ByteArray) {
     //for sure, this is an int-key based reduce shuffle store, return the actual number of the k-vs.
     ReduceShuffleStoreWithByteArrayKey *shuffleStoreWithByteArrayKeys = 
                       dynamic_cast<ReduceShuffleStoreWithByteArrayKey *> (gResultStore);
     // we can not do reset for each batch of get k-values, as the pending elements in merge-sort network 
     // requires the buffer to not be reset (otherwise, data gets lost).
     // BufferManager *bufferManager = shuffleStoreWithIntKeys->getBufferMgr();
     // bufferManager->reset(); //to get to the ByteBuffer's initial position.

     //before doing the retrieval, make sure that the buffer capacity is what has been allocated and expected with size 
     //CHECK_EQ (buffer_capacity, SHMShuffleGlobalConstants::SERIALIZATION_BUFFER_SIZE)
     //        << " buffer capacity: " << buffer_capacity << " does not match specifized size: "
     //        << SHMShuffleGlobalConstants::SERIALIZATION_BUFFER_SIZE;
 
     //int reducerId = shuffleStoreWithIntKeys->getReducerId();
     jint *koffsetsArray = nullptr;
     jint *voffsetsArray = nullptr;
     jboolean bufferExceeded = false;

     bool ordering = shuffleStoreWithByteArrayKeys->needsOrdering();
     if (!ordering) {
       ByteArrayKeyWithVariableLength::PassThroughMapBuckets&  resultHolder
                                          = shuffleStoreWithByteArrayKeys->getPassThroughResultHolder();
       shuffleStoreWithByteArrayKeys->reset_passthroughresult();
       actualNumberOfKVs = shuffleStoreWithByteArrayKeys->retrieve_passthroughmapbuckets(kmaxNumbers);

       VLOG(2) << "***in JNI getSimpleKVPairs with byte-array keys, no ordering requied, finished retrieving number of KVs: " 
             << actualNumberOfKVs;   

  
       //(2.1) kvalues
       koffsetsArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));
       //(2.2)voffsets. Note that we only need the boundary of the values, as the de-serialization
       // knows how to do de-serialization by itself.
       voffsetsArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));

       for (int i=0; i<actualNumberOfKVs; i++) {
         koffsetsArray[i]= resultHolder.keyAndValueOffsets[i].end_keyoffset;
         voffsetsArray[i] = resultHolder.keyAndValueOffsets[i].end_valueoffset;
         if (VLOG_IS_ON(3)) {
          VLOG(3) << "***in JNI getSimpleKVPairs with byte-array keys, retrieved i=" 
               << i<< " byte-array key value size: " 
               << (resultHolder.keyAndValueOffsets[i].end_keyoffset -resultHolder.keyAndValueOffsets[i].start_keyoffset)
               << " with value size:  " 
               << (resultHolder.keyAndValueOffsets[i].end_valueoffset -resultHolder.keyAndValueOffsets[i].start_valueoffset);
         }
      }
     }
     else {
      ByteArrayKeyWithVariableLength::MergeSortedMapBuckets& resultHolder=
  	                             shuffleStoreWithByteArrayKeys->getMergedResultHolder();
      shuffleStoreWithByteArrayKeys->reset_mergedresult();
      actualNumberOfKVs = shuffleStoreWithByteArrayKeys->retrieve_mergesortedmapbuckets(kmaxNumbers);

      //Use the following LOG statement to see whether the logic is invoked correctly.
      VLOG(2) << "***in JNI getSimpleKVPairs with byte-array keys, ordering required, finished retrieving number of KVs: "
	      << actualNumberOfKVs;

      unsigned char *buffer = (unsigned char*)env->GetDirectBufferAddress(byteBuffer);
      //(2.1) koffsets.
      koffsetsArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));
      voffsetsArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));

      int accumulated_size = 0;

      ExtensibleByteBuffers *kBufferManager = shuffleStoreWithByteArrayKeys->getKBufferMgr();
      ExtensibleByteBuffers *vBufferManager = shuffleStoreWithByteArrayKeys->getVBufferMgr();

      for (int i=0; i<actualNumberOfKVs; i++) {
	PositionInExtensibleByteBuffer cachedKeyValue = resultHolder.keys[i].cachedKeyValue;
	kBufferManager->retrieve(cachedKeyValue, buffer);
	buffer += cachedKeyValue.value_size;
	accumulated_size   += cachedKeyValue.value_size;
      
        if (accumulated_size > buffer_capacity) {
          bufferExceeded = true;
          break; //abort the update of the bytebuffer array.
	}

        koffsetsArray[i] = accumulated_size;

	VLOG(3) << "***in JNI getSimpleKVPairs with byte-array keys, retrieved i=" << i
		<< " key value size: " << cachedKeyValue.value_size;
	//now the values corresponding to the same K. 
	{
	  size_t start_position = resultHolder.keys[i].start_position;
	  size_t end_position = resultHolder.keys[i].end_position;
	  //check buffer will not exceed 
          int vaccumulated_size = accumulated_size;
          //I only have one position in  this situation
	  for (size_t p=start_position; p<end_position; p++) {
	    vaccumulated_size += resultHolder.kvaluesGroups[p].valueSize;
	  }

	  if (vaccumulated_size > buffer_capacity) {
	    bufferExceeded = true;
            break; //abort the update of the bytebuffer array 
	  }

	  VLOG(3) << "***in JNI getSimpleKVPairs with byte-array keys, retrieved i=" << i
		  << " key value with corresponding value vector: ";

          //I only have one position in  this situation, thus start-position equal to end_position
	  VLOG(3) <<" ***in JNI getSimpleKVPairs with byte-array keys, start-position= " << start_position
		  << " end-position = " << end_position;

          //I only have one position in  this situation
	  for (size_t p=start_position; p<end_position; p++) {
	    VLOG(3) << "*****in JNI getKVPairs with byte-array keys, retrieved position: "
		    << resultHolder.kvaluesGroups[p].cachedValueValue.position_in_start_buffer
		    << " value size: " << resultHolder.kvaluesGroups[p].cachedValueValue.value_size;
            //NOTE: by-pass value reading from channel earlier, thus we will need to do memcpy here
            //vBufferManager->retrieve(resultHolder.kvaluesGroups[p].cachedValueValue, buffer); 
            memcpy(buffer, resultHolder.kvaluesGroups[p].value,resultHolder.kvaluesGroups[p].cachedValueValue.value_size);
	    buffer += resultHolder.kvaluesGroups[p].cachedValueValue.value_size;
	    accumulated_size += resultHolder.kvaluesGroups[p].cachedValueValue.value_size;
	  }

	  voffsetsArray[i]=accumulated_size;
	  VLOG(3) << "*****in JNI getKVPairs with byte-array keys, final offset position: " << voffsetsArray[i];

	}      

     }
    }//with ordering

    jintArray koffsetsArrayVal = env->NewIntArray(actualNumberOfKVs);
    if (koffsetsArrayVal == NULL) {
      LOG(FATAL) <<"cannot create koffsets int arrary" <<endl;
      return -1;
    }

    env->SetIntArrayRegion(koffsetsArrayVal, 0, actualNumberOfKVs, koffsetsArray);
    VLOG(2) << "***in JNI getSimpleKVPairs with byte-array keys, finished assigning the koffsets array***";

    jintArray voffsetsArrayVal = env->NewIntArray(actualNumberOfKVs);
    if (voffsetsArrayVal == NULL) {
       LOG(FATAL) << "can not create voffsets int array";
       return -1;
    }
    env->SetIntArrayRegion(voffsetsArrayVal, 0, actualNumberOfKVs, voffsetsArray); 

    VLOG(2) << "***in JNI getKVPairs with long keys, finished assigning voffset array***";   


    //(3) set all of the fields
    env->SetObjectField (mergeResult, koffsetsArrayFieldId, koffsetsArrayVal);
    env->SetObjectField (mergeResult, voffsetsArrayFieldId, voffsetsArrayVal);
    //with extensible bytebuffer manager, we will never get the buffer exceeded. what will be extended
    //might be the non-extensible byte buffer that works a a carrier for this batch of the data.

    //(4) convert the values stored in extensible server to the linear single bytebuffer.
    //at this time, I take care of it by throw exception when the buffer exceeds the size at C++ side.
    env->SetBooleanField(mergeResult, bufferExceededFieldId, bufferExceeded);

    VLOG(2) << "***in JNI getKVPairs with long keys, retrieved buffer exceeded flag is: " << (bool)bufferExceeded;
 
    //(5) finally free some intermediate pointers
    free(koffsetsArray);
    free(voffsetsArray);
  
    VLOG(2) << "***in JNI getSimpleKVPairs with byte-array keys, free allocated koffset and voffsets array: "; 
  }
  else {
    //NOTE: what is done in this scope is to create empty arrays as the return to the Java side.
    VLOG(2) << "***in Jni GetSimpleKVParis with byte-array keys, actual reduce-shuffle store is with int-key due to empty buckets";
    // actualNumberOfKVs is 0;   
  
    //(2.1) kvalues
    jint *koffsetsArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));
    //(2.2)voffsets. Note that we only need the boundary of the values, as the de-serialization
    // knows how to do de-serialization by itself.
    jint *voffsetsArray = (jint*) malloc (actualNumberOfKVs* sizeof(int));

    jintArray koffsetsArrayVal = env->NewIntArray(actualNumberOfKVs);
    if (koffsetsArrayVal == NULL) {
      LOG(FATAL) <<"cannot create koffset int arrary";
      return -1;
    }

    env->SetIntArrayRegion(koffsetsArrayVal, 0, actualNumberOfKVs, koffsetsArray);
    VLOG(2) << "***in JNI getSimpleKVPairs with byte-array keys, finished assigning the koffsets array***";

    jintArray voffsetsArrayVal = env->NewIntArray(actualNumberOfKVs);
    if (voffsetsArrayVal == NULL) {
       LOG(FATAL) << "can not create voffsets int array" ;
       return -1;
    }
    env->SetIntArrayRegion(voffsetsArrayVal, 0, actualNumberOfKVs, voffsetsArray); 

    VLOG(2) << "***in JNI getSimpleKVPairs with byte-array keys, finished assigning voffset array***";   


    //(3) set all of the fields
    env->SetObjectField (mergeResult, koffsetsArrayFieldId, koffsetsArrayVal);
    env->SetObjectField (mergeResult, voffsetsArrayFieldId, voffsetsArrayVal);
    //with extensible bytebuffer manager, we will never get the buffer exceeded. what will be extended
    //might be the non-extensible byte buffer that works a a carrier for this batch of the data.

    //(4) convert the values stored in extensible server to the linear single bytebuffer.
    //at this time, I take care of it by throw exception when the buffer exceeds the size at C++ side.
    jboolean bufferExceeded = false; 
    env->SetBooleanField(mergeResult, bufferExceededFieldId, bufferExceeded);

    VLOG(2) << "***in JNI getSimpleKVPairs with byte-array keys, retrieved buffer exceeded flag is: " << (bool)bufferExceeded;
 
    //(5) finally free some intermediate pointers
    free(koffsetsArray);
    free(voffsetsArray);
  
    VLOG(2) << "***in JNI getSimpleKVPairs with byte-array keys, free allocated koffsets and voffsets array: "; 
  }

  return actualNumberOfKVs;
}


/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    nGetKVPairsWithStringKeys
 * Signature: (JLjava/nio/ByteBuffer;IILcom/hp/hpl/firesteel/shuffle/ShuffleDataModel/MergeSortedResult;)I
 */

JNIEXPORT jint JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_nGetKVPairsWithStringKeys
(JNIEnv *env, jobject obj, jlong ptrToShuffleStore, jobject byteBuffer, jint buffer_capacity,
 jint knumbers, jobject mergeResult){
  {

    const char *exClassName = "java/lang/UnsupportedOperationException";
    jclass ecls = env->FindClass (exClassName);
    if (ecls != NULL){
      env->ThrowNew(ecls, "nstoreKVPairs for arbitrary <k,v> is not supported");
    }

  }
  return 0;
}


/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    nGetSimpleKVPairsWithStringKeys
 * Signature: (JLjava/nio/ByteBuffer;IILcom/hp/hpl/firesteel/shuffle/ShuffleDataModel/MergeSortedResult;)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_nGetSimpleKVPairsWithStringKeys
(JNIEnv *env, jobject obj, jlong ptrToShuffleStore, jobject byteBuffer, jint buffer_capacity, jint knumbers, jobject mergeResult){
  {
    const char *exClassName = "java/lang/UnsupportedOperationException";
    jclass ecls = env->FindClass (exClassName);
    if (ecls != NULL){
      env->ThrowNew(ecls, "nstoreKVPairs for arbitrary <k,v> is not supported");
    }

  }
  
  return 0;
}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore
 * Method:    nGetSimpleKVPairs
 * Signature: (Ljava/nio/ByteBuffer;I[II)I
 */
JNIEXPORT jint JNICALL Java_com_hp_hpl_firesteel_shuffle_ReduceSHMShuffleStore_nGetSimpleKVPairs
(JNIEnv *env, jobject obj, jlong ptrToReduceStore, jobjectArray okvalues, jobject byteBuffer,
 jint buffer_capacity, jintArray voffsetsArray, jint knumbers){
  ReduceShuffleStoreWithObjKeys* reduceShuffleStore =
    reinterpret_cast<ReduceShuffleStoreWithObjKeys*> (ptrToReduceStore);

  vector<ReduceKVPair> pairs {reduceShuffleStore->fetch(knumbers)};
  int actualNumKVPairs = static_cast<int>(pairs.size());

  // leverage laziness to save unnecessary deserialization.
  if (reduceShuffleStore->isPassThrough()) {
    auto start = chrono::system_clock::now();
    shuffle::deserializeKeys(env, pairs);
    auto end = chrono::system_clock::now();
    chrono::duration<double> elapsed_s = end - start;
    LOG(INFO) << "deserializing " << pairs.size() << " keys took " << elapsed_s.count() << "s";
  }

  byte* buffer = (byte*)env->GetDirectBufferAddress(byteBuffer);
  int currentBufferSize {0};

  vector<jint> valueOffsets;
  int actualOffset {0};
  for (int i=0; i<actualNumKVPairs; ++i) {
    env->SetObjectArrayElement(okvalues, i, pairs[i].getKey());

    int serValueSize = pairs[i].getSerValueSize();
    if (currentBufferSize + serValueSize > buffer_capacity) {
      throw length_error("direct buffer is almost full.");
    }

    memcpy(buffer, pairs[i].getSerValue(), serValueSize);
    buffer += serValueSize;

    actualOffset += serValueSize;
    valueOffsets.push_back(actualOffset);
  }
  reduceShuffleStore->deleteJobjectKeys(env, pairs);

  env->SetIntArrayRegion(voffsetsArray, 0, actualNumKVPairs, valueOffsets.data());

  return static_cast<int>(actualNumKVPairs);
}
