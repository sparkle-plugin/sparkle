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
#include <iostream>
#include <memory>
#include "com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore.h"
#include "MapShuffleStoreManager.h"
#include "ShuffleStoreManager.h"
#include "MapShuffleStoreWithIntKeys.h"
#include "MapShuffleStoreWithLongKeys.h"
#include "MapShuffleStoreWithStringKeys.h"
#include "MapShuffleStoreWithByteArrayKeys.h"
#include "MapShuffleStoreWithObjKeys.h"
#include "GenericMapShuffleStore.h"
#include "SimpleUtils.h"

using namespace std;

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    ninitialize
 * Signature: (JIIIIZ)J
 */
JNIEXPORT jlong JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_ninitialize
(JNIEnv *env , jobject obj, jlong shuffleStoreMgrPtr, jint shuffleId, jint mapId, jint numberOfPartitions, jint keyType,
    jboolean ordering) {
    //to initialize the map shuffle store that is associated with the shuffle id, map id, and number of partitions.
    //also the key type is passed in.
    void *storePtr= nullptr; 

    ShuffleStoreManager *shuffleStoreManager = reinterpret_cast <ShuffleStoreManager *> (shuffleStoreMgrPtr); 
    //at this time, map shuffle store manager is already initialized when shuffle store manager is initialied.
    MapShuffleStoreManager *mapShuffleStoreManager =  shuffleStoreManager->getMapShuffleStoreManager();
    LOG(INFO) << "***get map shuffle store manager with address: 0x" << mapShuffleStoreManager <<endl;

    int kvalueTypeId = keyType;
    VLOG(2) <<  "map shuffle store initialization, with ordering: " << ordering 
            << " and type id: " << kvalueTypeId << endl; 

    KValueTypeId resultTypeId  = KValueTypeId::Unknown;
    switch(kvalueTypeId) {
      case 0:
	{
          resultTypeId = KValueTypeId::Int;
          break;
	}

    case 1:
      {
         resultTypeId  = KValueTypeId::Long;
         break;
      }

    case 2:
      {
         resultTypeId  = KValueTypeId::Float;
         break;
      }

    case 3:
      {
         resultTypeId  = KValueTypeId::Double;
         break;
      }

    case 4:
      {
        resultTypeId  = KValueTypeId::String;
        break;
      }

    case 5:
      {
        resultTypeId  = KValueTypeId::ByteArray;
        break;
      }

    case 6:
      {
        resultTypeId  = KValueTypeId::Object;
        break;
      }

    case 7:
      {
        resultTypeId  = KValueTypeId::Unknown;
        break;
      }
    }

    storePtr = (void*)mapShuffleStoreManager->createStore(shuffleId, mapId, resultTypeId, ordering); 

    long result = reinterpret_cast<long> (storePtr);
    LOG(INFO) << "***return from ninitialization." <<endl;

    return result;
    
}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nstop
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nstop
(JNIEnv *env , jobject obj, jlong shuffleStorePtr) {
    //to stop the store, and reclaim the DRAM resource.
    GenericMapShuffleStore *shuffleStore = reinterpret_cast <GenericMapShuffleStore *> (shuffleStorePtr); 
    shuffleStore->stop();
}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nshutdown
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nshutdown
(JNIEnv *env , jobject obj, jlong shuffleStoreMgrPtr, jlong shuffleStorePtr) {
    //to stop the store, and reclaim the DRAM resource.
    GenericMapShuffleStore *shuffleStore = reinterpret_cast <GenericMapShuffleStore *> (shuffleStorePtr); 
    ShuffleStoreManager *shuffleStoreManager = reinterpret_cast <ShuffleStoreManager *> (shuffleStoreMgrPtr);
    shuffleStoreManager->getMapShuffleStoreManager()->shutdownShuffleStore(shuffleStore);
}


/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nstoreKVPairs
 * Signature: (JLjava/nio/ByteBuffer;[I[I[II)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nstoreKVPairs
(JNIEnv *env, jobject obj, jlong ptrToStore, jobject byteBuffer, jintArray koffsets, jintArray voffsets, 
              jintArray partitions, jint numberofPairs){
     //NOTE: this store is pretty sure an object key based store
     //void *shuffleStore = reinterpret_cast <void *> (ptrToStore); 
     //we need to get the byte buffer via the specialized api. 
     //unsigned char *buf = (unsigned char* )env->GetDirectBufferAddress(byteBuffer);
     jint  *ko = env->GetIntArrayElements(koffsets, NULL); 
     jint  *vo = env->GetIntArrayElements (voffsets, NULL);
     jint  *par =env->GetIntArrayElements (partitions, NULL);
     
     //WARNING: this is incomplete, as we do not have object store for arbitrary <k,v> pairs at this time.
     {
     
        const char *exClassName = "java/lang/UnsupportedOperationException";
        jclass ecls = env->FindClass (exClassName);
        if (ecls != NULL){ 
           env->ThrowNew(ecls, "nstoreKVPairs for arbitrary <k,v> is not supported");
	}

     }
    
     //release local objects
     env->ReleaseIntArrayElements(koffsets, ko, 0); 
     env->ReleaseIntArrayElements (voffsets,vo, 0);
     env->ReleaseIntArrayElements (partitions, par, 0);

}

JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nCopyToNativeStore
(JNIEnv* env, jobject obj, jlong ptrToStore, jobject byteBuffer, jintArray voffsets,
 jobjectArray keys, jintArray keyHashes, jintArray partitions, jint numPairs) {
  MapShuffleStoreWithObjKeys *shuffleStore =
    reinterpret_cast<MapShuffleStoreWithObjKeys*> (ptrToStore);
  unsigned char *buf = (unsigned char * )env->GetDirectBufferAddress(byteBuffer);

  int* par = env->GetIntArrayElements (partitions, NULL);
  int* vo = env->GetIntArrayElements (voffsets, NULL);
  int* okhashes = env->GetIntArrayElements (keyHashes, NULL);

  vector<jobject> keysVec(numPairs);
  for (auto i=0; i<numPairs; ++i) {
    jobject key = env->GetObjectArrayElement(keys, i);
    keysVec[i] = env->NewGlobalRef(key);
  }
  shuffleStore->storeKVPairs(keysVec, okhashes, buf, vo, par, numPairs);

  env->ReleaseIntArrayElements(partitions, par, 0);
  env->ReleaseIntArrayElements(voffsets, vo, 0);
  env->ReleaseIntArrayElements(keyHashes, okhashes, 0);
}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nstoreKVPairsWithIntKeys
 * Signature: (JLjava/nio/ByteBuffer;[I[I[II)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nstoreKVPairsWithIntKeys
(JNIEnv *env, jobject obj, jlong ptrToStore, jobject byteBuffer , jintArray voffsets, jintArray kvalues,
               jintArray partitions, jint numberOfPairs){
     //NOTE: this store is pretty sure an object key based store
     MapShuffleStoreWithIntKey *shuffleStore = reinterpret_cast< MapShuffleStoreWithIntKey *> (ptrToStore);
     unsigned char *buf = (unsigned char * )env->GetDirectBufferAddress(byteBuffer);
     int *vo = env->GetIntArrayElements (voffsets, NULL);
     int *kv = env->GetIntArrayElements (kvalues, NULL);
     int *par = env->GetIntArrayElements (partitions, NULL);
     
     shuffleStore->storeKVPairsWithIntKeys (buf, vo, kv, par, numberOfPairs); 

     //release local objects
     env->ReleaseIntArrayElements(kvalues, kv, 0); 
     env->ReleaseIntArrayElements (voffsets,vo, 0);
     env->ReleaseIntArrayElements (partitions, par, 0);
}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nstoreKVPairsWithFloatKeys
 * Signature: (JLjava/nio/ByteBuffer;[I[F[II)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nstoreKVPairsWithFloatKeys
(JNIEnv * env, jobject obj, jlong ptrToStore, jobject byteBuffer, jintArray voffsets, jfloatArray kvalues,
          jintArray partitions, jint numberOfPairs) {
     //NOTE: this store is pretty sure a float key based store
     //void *shuffleStore = reinterpret_cast <void *> (ptrToStore); 
     //we need to get the byte buffer via the specialized api. 
     //unsigned char *buf = (unsigned char * )env->GetDirectBufferAddress(byteBuffer);
     int  *vo = env->GetIntArrayElements (voffsets, NULL);
     float  *kv = env->GetFloatArrayElements(kvalues, NULL); 

     int  *par =env->GetIntArrayElements (partitions, NULL);
     
     //WARNING: this is incomplete, as we do not have object store for arbitrary <k,v> pairs at this time.
     {
     
        const char *exClassName = "java/lang/UnsupportedOperationException";
        jclass ecls = env->FindClass (exClassName);
        if (ecls != NULL) {
          env->ThrowNew(ecls, "nstoreKVPairs for float key based  <k,v> is not supported");
	}
     }

     //release local objects
     env->ReleaseFloatArrayElements(kvalues, kv, 0); 
     env->ReleaseIntArrayElements (voffsets,vo, 0);
     env->ReleaseIntArrayElements (partitions, par, 0);
}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nstoreKVPairsWithLongKeys
 * Signature: (JLjava/nio/ByteBuffer;[I[J[II)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nstoreKVPairsWithLongKeys
(JNIEnv *env, jobject obj, jlong ptrToStore, jobject byteBuffer, jintArray voffsets, jlongArray kvalues,
         jintArray partitions, jint numberOfPairs) {


     //NOTE: this store is pretty sure an object key based store
     MapShuffleStoreWithLongKey *shuffleStore = reinterpret_cast< MapShuffleStoreWithLongKey *> (ptrToStore);
     unsigned char *buf = (unsigned char * )env->GetDirectBufferAddress(byteBuffer);
     int *vo = env->GetIntArrayElements (voffsets, NULL);
     long *kv = env->GetLongArrayElements (kvalues, NULL);
     int *par = env->GetIntArrayElements (partitions, NULL);
     
     shuffleStore->storeKVPairsWithLongKeys (buf, vo, kv, par, numberOfPairs); 

     //release local objects
     env->ReleaseLongArrayElements(kvalues, kv, 0); 
     env->ReleaseIntArrayElements (voffsets,vo, 0);
     env->ReleaseIntArrayElements (partitions, par, 0);
}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nstoreKVPairsWithStringKeys
 * Signature: (JLjava/nio/ByteBuffer;[I[Ljava/lang/String;[I[II)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nstoreKVPairsWithStringKeys
(JNIEnv *env, jobject obj, jlong ptrToStore, jobject byteBuffer, jintArray voffsets, 
    jobjectArray stringValues, jintArray stringValueLengths, jintArray partitions, jint numberOfPairs) {
     //Note: this store is pretty sure a string key based store 
     MapShuffleStoreWithStringKey *shuffleStore =  reinterpret_cast < MapShuffleStoreWithStringKey *> (ptrToStore);
     unsigned char *buf  =(unsigned char*) env->GetDirectBufferAddress(byteBuffer);
     int *vo = env->GetIntArrayElements (voffsets, NULL);
     int *kvalueLengths = env->GetIntArrayElements(stringValueLengths, NULL);
     //WARNING: the following implmentation show that: string is not an efficient way, we may be better to unify string 
     //with the same treatment as arbitray keys.
     char **params = (char**) malloc (numberOfPairs*sizeof(char*));
     for (int i=0; i<numberOfPairs; i++) {
       jstring stringV = (jstring)env->GetObjectArrayElement (stringValues, i);
       //NOTE: I do not need the last value, which is the terminated "\0" value
       int len = kvalueLengths[i];
       //params[i] = (unsigned char*) env->GetStringUTFChars(stringV, NULL);
       //WARNING: should we use std::free, in order to have tcmalloc to work?
       params[i] = (char*)malloc(len);
       env->GetStringUTFRegion(stringV, 0, len, params[i]);
       env->DeleteLocalRef(stringV);
     }

     int  *par =env->GetIntArrayElements (partitions, NULL);

     shuffleStore->storeKVPairsWithStringKeys(buf, vo, params, kvalueLengths,  par, (int)numberOfPairs);

     for (int i=0; i<numberOfPairs; i++) {
        //WARNING: should we use std::free, in order to have tcmalloc to work?
        free (params[i]); 
        params[i] = nullptr; 
     }
    
     //finally, free the pointer array
     free (params); 

     //release local array elements, object array is locally freed already. 
     env->ReleaseIntArrayElements(partitions, par, 0);
     env->ReleaseIntArrayElements(voffsets, vo, 0);
}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nstoreKVPairs
 * Signature: (JLjava/nio/ByteBuffer;[I[I[II)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nstoreKVPairsWithByteArrayKeys
(JNIEnv *env, jobject obj, jlong ptrToStore, jobject byteBuffer, jintArray koffsets, jintArray voffsets, 
              jintArray partitions, jint numberofPairs){
     MapShuffleStoreWithByteArrayKey *shuffleStore =  reinterpret_cast < MapShuffleStoreWithByteArrayKey *> (ptrToStore);
     unsigned char *buf = (unsigned char* )env->GetDirectBufferAddress(byteBuffer);
     int  *ko = env->GetIntArrayElements(koffsets, NULL); 
     int  *vo = env->GetIntArrayElements (voffsets, NULL);
     int  *par =env->GetIntArrayElements (partitions, NULL);
     
     shuffleStore->storeKVPairsWithByteArrayKeys(buf, ko, vo, par, (int)numberofPairs);

     //release local objects
     env->ReleaseIntArrayElements(koffsets, ko, 0); 
     env->ReleaseIntArrayElements (voffsets,vo, 0);
     env->ReleaseIntArrayElements (partitions, par, 0);

}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nsortAndStore
 * Signature: (JILcom/hp/hpl/firesteel/shuffle/ShuffleDataModel/MapStatus;)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nsortAndStore
(JNIEnv *env, jobject obj, jlong ptrToStore, jint totalNumberOfPartitions, jobject mStatus) {
    GenericMapShuffleStore* gstore = reinterpret_cast <GenericMapShuffleStore *> (ptrToStore);
    KValueTypeId  keyTypeId =  gstore->getKValueType().typeId;
    switch (keyTypeId) {
      case KValueTypeId::Int:  
        {
 	   MapShuffleStoreWithIntKey *storePtr = dynamic_cast<MapShuffleStoreWithIntKey *>(gstore);
           //sort becomes optional
           if (storePtr->needsOrdering()) {
	      VLOG(2) << "****writeShuffleData with int keys, key ordering is required****"<<endl; 
	   }
           else {
	      VLOG(2) << "****writeShuffleData with int keys, key ordering is not required****"<<endl; 
	   }
           
           //the total number of partitions are passed from the sort method, so we will need this method
           //independent of whether ordering is required or not.
           storePtr->sort(totalNumberOfPartitions,storePtr->needsOrdering());

           MapStatus mapStatus = storePtr-> writeShuffleData(); 
   
           VLOG(2) << "****Finish JNI call to writeShuffleData with int keys****"<<endl; 

           //(1)now populate the data back. 
           long offsetOfIndexChunk = mapStatus.getOffsetOfIndexBucket();
           VLOG(2) << "****after writeShuffleData, offsetOfIndexChunk is: "<< (void*)offsetOfIndexChunk << endl;
           //(2)populate the shm region name. 
           long shmRegionId = mapStatus.getRegionId(); //the returned is uint64_t.
           VLOG(2)<< "****after writeShuffleData with int keys, shmRegionId is: "<< shmRegionId;

           //(3)populate the passed in long array 
           jlong *cArray = (jlong*) malloc (totalNumberOfPartitions* sizeof(long));
           //jlong cArray[1000];

           vector<int> &bucketSizes = mapStatus.getBucketSizes();
           for (int i=0; i<totalNumberOfPartitions; i++) {
   	      cArray[i] = (long) bucketSizes[i];
	   }

	   //now get the field id, and then set object field on it. 
           //(4) get the reference to object's class, it is a local reference, which can not be cached. 
           jclass cls = env->GetObjectClass(mStatus);
           //(5) look for fields in the object. J stands for long. 
           static jfieldID mapStatusArrayFieldId = NULL; //cached field id for mapstatuSrray
           if (mapStatusArrayFieldId == NULL ){ 
             mapStatusArrayFieldId = env->GetFieldID(cls, "mapStatus", "[J");
             if (mapStatusArrayFieldId == NULL) {
  	       LOG(FATAL) << "cannot find field: mapstatus with long type" <<endl;
               return;
	     }
	   }

           static jfieldID regionIdOfIndexBucketFieldId = NULL; //cached field id, with long type.
           if (regionIdOfIndexBucketFieldId == NULL) {
             regionIdOfIndexBucketFieldId = env->GetFieldID(cls, "regionIdOfIndexBucket", "J"); 
             if (regionIdOfIndexBucketFieldId == NULL) {
	        LOG(FATAL) <<"cannot find field: region id of index bucket with long type" <<endl;
                return;
	     }
	   }

           static jfieldID offsetOfIndexBucketFieldId =NULL ;//cached field id for offsetOfIndexBucket
           if (offsetOfIndexBucketFieldId == NULL ) { 
             offsetOfIndexBucketFieldId = env->GetFieldID(cls, "offsetOfIndexBucket", "J");
             if (offsetOfIndexBucketFieldId == NULL) {
	        LOG(FATAL) <<"cannot find field: offsetOfIndexBucket with long type" << endl;
                return;
	     }
	   }

           jlongArray mstatusLongArrayVal = env->NewLongArray(totalNumberOfPartitions);
           if (mstatusLongArrayVal == NULL) {
	     LOG(FATAL) <<"cannot create map status long arrary" <<endl;
             return;
	   }

           env->SetLongArrayRegion(mstatusLongArrayVal, 0, totalNumberOfPartitions, cArray);

           VLOG(2) << "****after writeShuffleData with int keys, assign C++ long array of map status to Java long array ***";

           //(7) assign to the corresponding field 
           env->SetObjectField (mStatus, mapStatusArrayFieldId, mstatusLongArrayVal);
           env->SetLongField (mStatus, regionIdOfIndexBucketFieldId, shmRegionId);
           env->SetLongField (mStatus, offsetOfIndexBucketFieldId, offsetOfIndexChunk);
	   {
	     jfieldID fidWrittenTime {env->GetFieldID(cls, "dataChunkWrittenTimeNs", "J")};
	     env->SetLongField(mStatus, fidWrittenTime, mapStatus.getWrittenTimeNs());
	   }

           VLOG(2) << "****after writeShuffleData with int keys, finish object field assignment "; 

           //(8)then free the local array.
           free (cArray);

           break; 
	}

        case KValueTypeId::Long: 
        {
 	   MapShuffleStoreWithLongKey *storePtr = dynamic_cast<MapShuffleStoreWithLongKey *>(gstore);
           //sort becomes optional
           if (storePtr->needsOrdering()) {
	      VLOG(2) << "****writeShuffleData with long keys, key ordering is required****"<<endl; 
	   }
           else {
	      VLOG(2) << "****writeShuffleData with long keys, key ordering is not required****"<<endl; 
	   }
           
           //the total number of partitions are passed from the sort method, so we will need this method
           //independent of whether ordering is required or not.
           storePtr->sort(totalNumberOfPartitions,storePtr->needsOrdering());

           MapStatus mapStatus = storePtr-> writeShuffleData(); 
   
           VLOG(2) << "****Finish JNI call to writeShuffleData with long keys****"<<endl; 

           //(1)now populate the data back. 
           long offsetOfIndexChunk = mapStatus.getOffsetOfIndexBucket();
           VLOG(2) << "****after writeShuffleData, offsetOfIndexChunk is: "<< (void*)offsetOfIndexChunk << endl;
           //(2)populate the shm region name. 
           long shmRegionId = mapStatus.getRegionId(); //how to return this to Java
           VLOG(2)<< "****after writeShuffleData with long keys, shmRegionId is: "<< shmRegionId;

           //(3)populate the passed in long array 
           jlong *cArray = (jlong*) malloc (totalNumberOfPartitions* sizeof(long));
           //jlong cArray[1000];

           vector<int> &bucketSizes = mapStatus.getBucketSizes();
           for (int i=0; i<totalNumberOfPartitions; i++) {
   	      cArray[i] = (long) bucketSizes[i];
	   }

	   //now get the field id, and then set object field on it. 
           //(4) get the reference to object's class, it is a local reference, which can not be cached. 
           jclass cls = env->GetObjectClass(mStatus);
           //(5) look for fields in the object. J stands for long. 
           static jfieldID mapStatusArrayFieldId = NULL; //cached field id for mapstatuSrray
           if (mapStatusArrayFieldId == NULL ){ 
             mapStatusArrayFieldId = env->GetFieldID(cls, "mapStatus", "[J");
             if (mapStatusArrayFieldId == NULL) {
  	       LOG(FATAL) << "cannot find field: mapstatus with long type" <<endl;
               return;
	     }
	   }

           static jfieldID regionIdOfIndexBucketFieldId = NULL; //cached field id 
           if (regionIdOfIndexBucketFieldId == NULL) {
             regionIdOfIndexBucketFieldId = env->GetFieldID(cls, "regionIdOfIndexBucket", "J"); 
             if (regionIdOfIndexBucketFieldId == NULL) {
	        LOG(FATAL) <<"cannot find field: region id of index bucket with long type" <<endl;
                return;
	     }
	   }

           static jfieldID offsetOfIndexBucketFieldId =NULL ;//cached field id for offsetOfIndexBucket
           if (offsetOfIndexBucketFieldId == NULL ) { 
             offsetOfIndexBucketFieldId = env->GetFieldID(cls, "offsetOfIndexBucket", "J");
             if (offsetOfIndexBucketFieldId == NULL) {
	        LOG(FATAL) <<"cannot find field: offsetofIndexBucket with long type" << endl;
                return;
	     }
	   }

           jlongArray mstatusLongArrayVal = env->NewLongArray(totalNumberOfPartitions);
           if (mstatusLongArrayVal == NULL) {
	     LOG(FATAL) <<"cannot create map status long arrary" <<endl;
             return;
	   }

           env->SetLongArrayRegion(mstatusLongArrayVal, 0, totalNumberOfPartitions, cArray);

           VLOG(2) << "****after writeShuffleData with long keys, assign C++ long array of map status to Java long array ***";

           //(7) assign to the corresponding field 
           env->SetObjectField (mStatus, mapStatusArrayFieldId, mstatusLongArrayVal);
           env->SetLongField (mStatus, regionIdOfIndexBucketFieldId, shmRegionId);
           env->SetLongField (mStatus, offsetOfIndexBucketFieldId, offsetOfIndexChunk);

           VLOG(2) << "****after writeShuffleData with long keys, finish object field assignment "; 

           //(8)then free the local array.
           free (cArray);

          break;          
        }
        case KValueTypeId::Float: 
        {
          break; 
        }

        case KValueTypeId::Double: 
        {
          break; 
        }
        case KValueTypeId::String: 
        {
          break; 
        }

        case KValueTypeId::ByteArray:
        {
 	   MapShuffleStoreWithByteArrayKey *storePtr = dynamic_cast<MapShuffleStoreWithByteArrayKey *>(gstore);
           //sort becomes optional
           if (storePtr->needsOrdering()) {
	      VLOG(2) << "****writeShuffleData with byte-array keys, key ordering is required****";
	   }
           else {
	      VLOG(2) << "****writeShuffleData with byte-array keys, key ordering is not required****"; 
	   }
           
           //the total number of partitions are passed from the sort method, so we will need this method
           //independent of whether ordering is required or not.
           storePtr->sort(totalNumberOfPartitions,storePtr->needsOrdering());

           MapStatus mapStatus = storePtr-> writeShuffleData(); 
   
           VLOG(2) << "****Finish JNI call to writeShuffleData with byte-array keys****"; 

           //(1)now populate the data back. 
           long offsetOfIndexChunk = mapStatus.getOffsetOfIndexBucket();
           VLOG(2) << "****after writeShuffleData, offsetOfIndexChunk is: "<< (void*)offsetOfIndexChunk;
           //(2)populate the shm region name. 
           long shmRegionId = mapStatus.getRegionId(); //how to return this to Java
           VLOG(2)<< "****after writeShuffleData with byte-array keys, shmRegionId is: "<< shmRegionId;

           //(3)populate the passed in long array 
           jlong *cArray = (jlong*) malloc (totalNumberOfPartitions* sizeof(long));
           //jlong cArray[1000];

           vector<int> &bucketSizes = mapStatus.getBucketSizes();
           for (int i=0; i<totalNumberOfPartitions; i++) {
   	      cArray[i] = (long) bucketSizes[i];
	   }

	   //now get the field id, and then set object field on it. 
           //(4) get the reference to object's class, it is a local reference, which can not be cached. 
           jclass cls = env->GetObjectClass(mStatus);
           //(5) look for fields in the object. J stands for long. 
           static jfieldID mapStatusArrayFieldId = NULL; //cached field id for mapstatuSrray
           if (mapStatusArrayFieldId == NULL ){ 
             mapStatusArrayFieldId = env->GetFieldID(cls, "mapStatus", "[J");
             if (mapStatusArrayFieldId == NULL) {
  	       LOG(FATAL) << "cannot find field: mapstatus with long type";
               return;
	     }
	   }

           static jfieldID regionIdOfIndexBucketFieldId = NULL; //cached field id 
           if (regionIdOfIndexBucketFieldId == NULL) {
             regionIdOfIndexBucketFieldId = env->GetFieldID(cls, "regionIdOfIndexBucket", "J"); 
             if (regionIdOfIndexBucketFieldId == NULL) {
	        LOG(FATAL) <<"cannot find field: region id of index bucket with long type";
                return;
	     }
	   }

           static jfieldID offsetOfIndexBucketFieldId =NULL ;//cached field id for offsetOfIndexBucket
           if (offsetOfIndexBucketFieldId == NULL ) { 
             offsetOfIndexBucketFieldId = env->GetFieldID(cls, "offsetOfIndexBucket", "J");
             if (offsetOfIndexBucketFieldId == NULL) {
	        LOG(FATAL) <<"cannot find field: offsetofIndexBucket with long type";
                return;
	     }
	   }

           jlongArray mstatusLongArrayVal = env->NewLongArray(totalNumberOfPartitions);
           if (mstatusLongArrayVal == NULL) {
	     LOG(FATAL) <<"cannot create map status long arrary";
             return;
	   }

           env->SetLongArrayRegion(mstatusLongArrayVal, 0, totalNumberOfPartitions, cArray);

           VLOG(2) << "****after writeShuffleData with long keys, assign C++ long array of map status to Java long array ***";

           //(7) assign to the corresponding field 
           env->SetObjectField (mStatus, mapStatusArrayFieldId, mstatusLongArrayVal);
           env->SetLongField (mStatus, regionIdOfIndexBucketFieldId, shmRegionId);
           env->SetLongField (mStatus, offsetOfIndexBucketFieldId, offsetOfIndexChunk);

           VLOG(2) << "****after writeShuffleData with long keys, finish object field assignment "; 

           //(8)then free the local array.
           free (cArray);

           break;
        }
        case KValueTypeId::Object:
        {
          MapShuffleStoreWithObjKeys* storePtr
            = dynamic_cast<MapShuffleStoreWithObjKeys*>(gstore);

          unique_ptr<MapStatus> mapStatus {storePtr->write(env)};

          // update Java-side MapStatus.
          jclass retClazz = env->GetObjectClass(mStatus);
          {
            jfieldID fidRegionId {env->GetFieldID(retClazz, "regionIdOfIndexBucket", "J")};
            env->SetLongField(mStatus, fidRegionId, mapStatus->getRegionId());
          }
          {
            jfieldID fidOffset {env->GetFieldID(retClazz, "offsetOfIndexBucket", "J")};
            env->SetLongField(mStatus, fidOffset, mapStatus->getOffsetOfIndexBucket());
          }
          {
            jfieldID fidWrittenTime {env->GetFieldID(retClazz, "dataChunkWrittenTimeNs", "J")};
            env->SetLongField(mStatus, fidWrittenTime, mapStatus->getWrittenTimeNs());
          }
          {
            unique_ptr<jlong[]> tmp(new jlong[totalNumberOfPartitions]);
            vector<int>& sizes = mapStatus->getBucketSizes();
            for (int i=0; i<totalNumberOfPartitions; ++i) {
              tmp[i] = (sizes.size() == 0) ? 0 : (long)sizes[i];
            }

            jlongArray bucketSizes {env->NewLongArray(totalNumberOfPartitions)};
            env->SetLongArrayRegion(bucketSizes, 0, totalNumberOfPartitions, tmp.get());

            jfieldID fidMapStatus {env->GetFieldID(retClazz, "mapStatus", "[J")};
            env->SetObjectField(mStatus, fidMapStatus, bucketSizes);
          }

          storePtr->deleteJobjectKeys(env);
          break;
        }
        case KValueTypeId::Unknown:
        {
          break;
        }
       //we will later fill in other kinds of stores. 

    }

    VLOG(2) << "****Finish JNI sort-and-store *************************"<<endl;
}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nstoreVValueType
 * Signature: (J[BI)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nstoreVValueType
(JNIEnv *env, jobject obj, jlong ptrToStore, jbyteArray vvalueType, jint vtypeLength) {
  //this will be arbitrary (k,v) type. not implemented yet.
  GenericMapShuffleStore* gstore = reinterpret_cast <GenericMapShuffleStore *> (ptrToStore); 

  KValueTypeId  keyTypeId = gstore->getKValueType().typeId;
  switch (keyTypeId) {
     case KValueTypeId::Int:  
      {
	   MapShuffleStoreWithIntKey *storePtr = dynamic_cast<MapShuffleStoreWithIntKey *>(gstore);
           jbyte* byteArrayPtr = env->GetByteArrayElements(vvalueType, NULL);

           storePtr->setValueType((unsigned char*) byteArrayPtr, vtypeLength); 

           VLOG(2) << "in JNI nstoreVValueType: "  << " with length: " << vtypeLength 
                   <<" and value: " << ByteArrayUtil::to_str((unsigned char*) byteArrayPtr, vtypeLength);

           //free local reference
           env->ReleaseByteArrayElements(vvalueType, byteArrayPtr, 0);

           break;
      }

      case KValueTypeId::Long: 
        {
	   MapShuffleStoreWithLongKey *storePtr = dynamic_cast<MapShuffleStoreWithLongKey *>(gstore);
           jbyte* byteArrayPtr = env->GetByteArrayElements(vvalueType, NULL);
           storePtr->setValueType((unsigned char*) byteArrayPtr, vtypeLength); 

           VLOG(2) << "in JNI nstoreVValueType: "  << " with length: " << vtypeLength 
                   <<" and value: " << ByteArrayUtil::to_str((unsigned char*) byteArrayPtr, vtypeLength);

           //free local reference
           env->ReleaseByteArrayElements(vvalueType, byteArrayPtr, 0);

           break; 
        }
      case KValueTypeId::Float: 
        {
          break; 
        }

      case KValueTypeId::Double: 
        {
          break; 
        }
      case KValueTypeId::String: 
        {
          break; 
        }

      case KValueTypeId::ByteArray: 
        {
	   MapShuffleStoreWithByteArrayKey *storePtr = dynamic_cast<MapShuffleStoreWithByteArrayKey *>(gstore);
           jbyte* byteArrayPtr = env->GetByteArrayElements(vvalueType, NULL);
           storePtr->setValueType((unsigned char*) byteArrayPtr, vtypeLength); 

           VLOG(2) << "in JNI nstoreVValueType: "  << " with length: " << vtypeLength
                   <<" and value: " << ByteArrayUtil::to_str((unsigned char*) byteArrayPtr, vtypeLength);

           //free local reference
           env->ReleaseByteArrayElements(vvalueType, byteArrayPtr, 0);

          break; 
        }

      case KValueTypeId::Object:
        {
          break; 
        }
      case KValueTypeId::Unknown:
        {
          break; 
        }

      //need to fill in more cases.
  }
}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nstoreKVTypes
 * Signature: (JII[BI[BI)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nstoreKVTypes
(JNIEnv *env, jobject obj, jlong shuffleStoreMgrPtr, jint shuffleId, jint mapTaskId, jbyteArray kvalueType,
 jint kvalueTypeLength,  jbyteArray vvalueType, jint vvalueTypeLength) {
     //WARNING: this is incomplete, as we do not have object store for arbitrary <k,v> pairs at this time.
   {
     
      const char *exClassName = "java/lang/UnsupportedOperationException";
      jclass ecls = env->FindClass (exClassName);
      if (ecls != NULL){ 
          env->ThrowNew(ecls, "nstoreKVPairs for arbitrary <k,v> is not supported");
     }

   }

}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nwriteToHeap
 * Signature: (JI[ILjava/nio/ByteBuffer;Lcom/hp/hpl/firesteel/shuffle/ShuffleDataModel/MapStatus;)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nwriteToHeap
(JNIEnv *env, jobject obj, jlong ptrToStore, jint numPartitions, jintArray partitionLengths,
 jobject holder, jobject mStatus) {
  MapShuffleStoreWithObjKeys* storePtr
    = reinterpret_cast<MapShuffleStoreWithObjKeys*>(ptrToStore);

  byte *buff = static_cast<byte*>(env->GetDirectBufferAddress(holder));
  int *len = env->GetIntArrayElements(partitionLengths, NULL);
  unique_ptr<MapStatus> mapStatus {storePtr->write(buff, numPartitions, len)};

  env->ReleaseIntArrayElements(partitionLengths, len, 0);

  // update Java-side MapStatus.
  jclass retClazz = env->GetObjectClass(mStatus);
  {
    jfieldID fidRegionId {env->GetFieldID(retClazz, "regionIdOfIndexBucket", "J")};
    env->SetLongField(mStatus, fidRegionId, mapStatus->getRegionId());
  }
  {
    jfieldID fidOffset {env->GetFieldID(retClazz, "offsetOfIndexBucket", "J")};
    env->SetLongField(mStatus, fidOffset, mapStatus->getOffsetOfIndexBucket());
  }
  {
    jfieldID fidWrittenTime {env->GetFieldID(retClazz, "dataChunkWrittenTimeNs", "J")};
    env->SetLongField(mStatus, fidWrittenTime, mapStatus->getWrittenTimeNs());
  }
  {
    unique_ptr<jlong[]> tmp(new jlong[numPartitions]);
    vector<int>& sizes = mapStatus->getBucketSizes();
    for (int i=0; i<numPartitions; ++i) {
      tmp[i] = (sizes.size() == 0) ? 0 : (long)sizes[i];
    }

    jlongArray bucketSizes {env->NewLongArray(numPartitions)};
    env->SetLongArrayRegion(bucketSizes, 0, numPartitions, tmp.get());

    jfieldID fidMapStatus {env->GetFieldID(retClazz, "mapStatus", "[J")};
    env->SetObjectField(mStatus, fidMapStatus, bucketSizes);
  }
}
