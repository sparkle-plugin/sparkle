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
#include "com_hp_hpl_firesteel_offheapstore_OffHeapStore.h"
#include "OffHeapStoreManager.h"
#include "ShuffleStoreManager.h"

/*
 * Class:     com_hp_hpl_firesteel_offheapstore_OffHeapStore
 * Method:    ninitialize
 * Signature: (Ljava/lang/String;I)J
 */
JNIEXPORT jlong JNICALL Java_com_hp_hpl_firesteel_offheapstore_OffHeapStore_ninitialize
  (JNIEnv *env, jobject obj, jstring globalHeapName, jint executorId) {

    OffHeapStoreManager *ptrMgr = OffHeapStoreManager::getInstance();

    const char *string_val = env->GetStringUTFChars(globalHeapName, NULL);

    string heapName(string_val);
    ptrMgr->initialize(heapName, executorId);

    long ptrValue = reinterpret_cast <long> (ptrMgr);
    return ptrValue;
}

/*
 * Class:     com_hp_hpl_firesteel_offheapstore_OffHeapStore
 * Method:    createAttributePartition
 * Signature: (JJILcom/hp/hpl/firesteel/offheapstore/ShmAddress;)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_offheapstore_OffHeapStore_createAttributePartition
  (JNIEnv *env, jobject obj, jlong ptrOffHeapStoreMgr, jlong addr, jint size, jobject shmAddrObj) {
	OffHeapStoreManager *mgr = reinterpret_cast <OffHeapStoreManager *> (ptrOffHeapStoreMgr);

    ShmAddress shmAddr;
	mgr->createAttributeTable(addr, size, shmAddr);

    jclass clazz = env->GetObjectClass( shmAddrObj );

    static jfieldID regionIdFieldId = NULL;
	if(regionIdFieldId == NULL) {
		regionIdFieldId = env->GetFieldID( clazz, "regionId", "I" );
    	if(regionIdFieldId == NULL) {
        	LOG(FATAL) << "cannot find field: regionId" <<endl;
	    	return;
    	}
	}
    static jfieldID offsetAttrTblFieldId = NULL;
	if(offsetAttrTblFieldId == NULL) {
		offsetAttrTblFieldId = env->GetFieldID( clazz, "offset_attrTable", "J" );
    	if(offsetAttrTblFieldId == NULL) {
        	LOG(FATAL) << "cannot find field: offset_attrTable" <<endl;
	    	return;
    	}
	}

    env->SetIntField( shmAddrObj, regionIdFieldId, shmAddr.regionId );
    env->SetLongField( shmAddrObj, offsetAttrTblFieldId, shmAddr.offset_attrTable );
}

/*
 * Class:     com_hp_hpl_firesteel_offheapstore_OffHeapStore
 * Method:    createAttributeHashPartition
 * Signature: (JJIILcom/hp/hpl/firesteel/offheapstore/ShmAddress;)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_offheapstore_OffHeapStore_createAttributeHashPartition
  (JNIEnv *env, jobject obj, jlong ptrOffHeapStoreMgr, jlong addr, jint size, jint valuesize, jobject shmAddrObj) {

    OffHeapStoreManager *mgr = reinterpret_cast <OffHeapStoreManager *> (ptrOffHeapStoreMgr);

    ShmAddress shmAddr;
	mgr->createAttributeHashTable(addr, size, valuesize, shmAddr);

    jclass clazz = env->GetObjectClass( shmAddrObj );

    static jfieldID regionIdFieldId = NULL;
    if(regionIdFieldId == NULL) {
        regionIdFieldId = env->GetFieldID( clazz, "regionId", "I" );
        if(regionIdFieldId == NULL) {
            LOG(FATAL) << "cannot find field: regionId" <<endl;
            return;
        }
    }
    static jfieldID offsetAttrTblFieldId = NULL;
    if(offsetAttrTblFieldId == NULL) {
        offsetAttrTblFieldId = env->GetFieldID( clazz, "offset_attrTable", "J" );
        if(offsetAttrTblFieldId == NULL) {
            LOG(FATAL) << "cannot find field: offset_attrTable" <<endl;
            return;
        }
    }
    static jfieldID offsetHash1stTblFieldId = NULL;
    if(offsetHash1stTblFieldId == NULL) {
        offsetHash1stTblFieldId = env->GetFieldID( clazz, "offset_hash1stTable", "J" );
        if(offsetHash1stTblFieldId == NULL) {
            LOG(FATAL) << "cannot find field: offset_hash1stTable" <<endl;
            return;
        }
    }
    static jfieldID offsetHash2ndTblFieldId = NULL;
    if(offsetHash2ndTblFieldId == NULL) {
        offsetHash2ndTblFieldId = env->GetFieldID( clazz, "offset_hash2ndTable", "J" );
        if(offsetHash2ndTblFieldId == NULL) {
            LOG(FATAL) << "cannot find field: offset_hash2ndTable" <<endl;
            return;
        }
    }
    static jfieldID bucketCntFieldId = NULL;
    if(bucketCntFieldId == NULL) {
        bucketCntFieldId = env->GetFieldID( clazz, "bucket_cnt", "I" );
        if(bucketCntFieldId == NULL) {
            LOG(FATAL) << "cannot find field: bucket_cnt" <<endl;
            return;
        }
    }

    env->SetIntField( shmAddrObj, regionIdFieldId, shmAddr.regionId );
    env->SetLongField( shmAddrObj, offsetAttrTblFieldId, shmAddr.offset_attrTable );
    env->SetLongField( shmAddrObj, offsetHash1stTblFieldId, shmAddr.offset_hash1stTable );
    env->SetLongField( shmAddrObj, offsetHash2ndTblFieldId, shmAddr.offset_hash2ndTable );
    env->SetIntField( shmAddrObj, bucketCntFieldId, shmAddr.bucket_cnt );

}

/*
 * Class:     com_hp_hpl_firesteel_offheapstore_OffHeapStore
 * Method:    getAttributePartition
 * Signature: (JLcom/hp/hpl/firesteel/offheapstore/ShmAddress;)J
 */
JNIEXPORT jlong JNICALL Java_com_hp_hpl_firesteel_offheapstore_OffHeapStore_getAttributePartition
  (JNIEnv *env, jobject obj, jlong ptrOffHeapStoreMgr, jobject shmAddrObj) {


    OffHeapStoreManager *mgr = reinterpret_cast <OffHeapStoreManager *> (ptrOffHeapStoreMgr);

    jclass clazz = env->GetObjectClass( shmAddrObj );

    static jfieldID regionIdFieldId = NULL;
    if(regionIdFieldId == NULL) {
        regionIdFieldId = env->GetFieldID( clazz, "regionId", "I" );
        if(regionIdFieldId == NULL) {
            LOG(FATAL) << "cannot find field: regionId" <<endl;
            return -1;
        }
    }
    static jfieldID offsetAttrTblFieldId = NULL;
    if(offsetAttrTblFieldId == NULL) {
        offsetAttrTblFieldId = env->GetFieldID( clazz, "offset_attrTable", "J" );
        if(offsetAttrTblFieldId == NULL) {
            LOG(FATAL) << "cannot find field: offset_attrTable" <<endl;
            return -1;
        }
    }

	ShmAddress shmAddr;

    shmAddr.regionId = env->GetIntField( shmAddrObj, regionIdFieldId );
    shmAddr.offset_attrTable = env->GetLongField( shmAddrObj, offsetAttrTblFieldId );

	return mgr->getAttributeTable(shmAddr);
}

/*
 * Class:     com_hp_hpl_firesteel_offheapstore_OffHeapStore
 * Method:    getAttributeOffsetInHashPartition
 * Signature: (JLcom/hp/hpl/firesteel/offheapstore/ShmAddress;J)J
 */
JNIEXPORT jlong JNICALL Java_com_hp_hpl_firesteel_offheapstore_OffHeapStore_getAttributeOffsetInHashPartition
  (JNIEnv *env, jobject obj, jlong ptrOffHeapStoreMgr, jobject shmAddrObj, jlong key) {

    OffHeapStoreManager *mgr = reinterpret_cast <OffHeapStoreManager *> (ptrOffHeapStoreMgr);

    jclass clazz = env->GetObjectClass( shmAddrObj );

    static jfieldID regionIdFieldId = NULL;
    if(regionIdFieldId == NULL) {
        regionIdFieldId = env->GetFieldID( clazz, "regionId", "I" );
        if(regionIdFieldId == NULL) {
            LOG(FATAL) << "cannot find field: regionId" <<endl;
            return -1;
        }
    }
    static jfieldID offsetAttrTblFieldId = NULL;
    if(offsetAttrTblFieldId == NULL) {
        offsetAttrTblFieldId = env->GetFieldID( clazz, "offset_attrTable", "J" );
        if(offsetAttrTblFieldId == NULL) {
            LOG(FATAL) << "cannot find field: offset_attrTable" <<endl;
            return -1;
        }
    }
    static jfieldID offsetHash1stTblFieldId = NULL;
    if(offsetHash1stTblFieldId == NULL) {
        offsetHash1stTblFieldId = env->GetFieldID( clazz, "offset_hash1stTable", "J" );
        if(offsetHash1stTblFieldId == NULL) {
            LOG(FATAL) << "cannot find field: offset_hash1stTable" <<endl;
            return -1;
        }
    }
    static jfieldID offsetHash2ndTblFieldId = NULL;
    if(offsetHash2ndTblFieldId == NULL) {
        offsetHash2ndTblFieldId = env->GetFieldID( clazz, "offset_hash2ndTable", "J" );
        if(offsetHash2ndTblFieldId == NULL) {
            LOG(FATAL) << "cannot find field: offset_hash2ndTable" <<endl;
            return -1;
        }
    }
    static jfieldID bucketCntFieldId = NULL;
    if(bucketCntFieldId == NULL) {
        bucketCntFieldId = env->GetFieldID( clazz, "bucket_cnt", "I" );
        if(bucketCntFieldId == NULL) {
            LOG(FATAL) << "cannot find field: bucket_cnt" <<endl;
            return -1;
        }
    }

    ShmAddress shmAddr;

    shmAddr.regionId = env->GetIntField( shmAddrObj, regionIdFieldId );
    shmAddr.offset_attrTable = env->GetLongField( shmAddrObj, offsetAttrTblFieldId );
    shmAddr.offset_hash1stTable = env->GetLongField( shmAddrObj, offsetHash1stTblFieldId );
    shmAddr.offset_hash2ndTable = env->GetLongField( shmAddrObj, offsetHash2ndTblFieldId );
    shmAddr.bucket_cnt = env->GetIntField( shmAddrObj, bucketCntFieldId );

    return mgr->getAttribute(shmAddr, key);

}
