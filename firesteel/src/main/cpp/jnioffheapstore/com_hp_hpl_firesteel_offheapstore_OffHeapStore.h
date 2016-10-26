/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_hp_hpl_firesteel_offheapstore_OffHeapStore */

#ifndef _Included_com_hp_hpl_firesteel_offheapstore_OffHeapStore
#define _Included_com_hp_hpl_firesteel_offheapstore_OffHeapStore
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_hp_hpl_firesteel_offheapstore_OffHeapStore
 * Method:    ninitialize
 * Signature: (Ljava/lang/String;I)J
 */
JNIEXPORT jlong JNICALL Java_com_hp_hpl_firesteel_offheapstore_OffHeapStore_ninitialize
  (JNIEnv *, jobject, jstring, jint);

/*
 * Class:     com_hp_hpl_firesteel_offheapstore_OffHeapStore
 * Method:    shutdown
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_offheapstore_OffHeapStore_shutdown
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_hp_hpl_firesteel_offheapstore_OffHeapStore
 * Method:    createAttributePartition
 * Signature: (JJILcom/hp/hpl/firesteel/offheapstore/ShmAddress;)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_offheapstore_OffHeapStore_createAttributePartition
  (JNIEnv *, jobject, jlong, jlong, jint, jobject);

/*
 * Class:     com_hp_hpl_firesteel_offheapstore_OffHeapStore
 * Method:    createAttributeHashPartition
 * Signature: (JJIILcom/hp/hpl/firesteel/offheapstore/ShmAddress;)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_offheapstore_OffHeapStore_createAttributeHashPartition
  (JNIEnv *, jobject, jlong, jlong, jint, jint, jobject);

/*
 * Class:     com_hp_hpl_firesteel_offheapstore_OffHeapStore
 * Method:    getAttributePartition
 * Signature: (JLcom/hp/hpl/firesteel/offheapstore/ShmAddress;)J
 */
JNIEXPORT jlong JNICALL Java_com_hp_hpl_firesteel_offheapstore_OffHeapStore_getAttributePartition
  (JNIEnv *, jobject, jlong, jobject);

/*
 * Class:     com_hp_hpl_firesteel_offheapstore_OffHeapStore
 * Method:    getAttributeOffsetInHashPartition
 * Signature: (JLcom/hp/hpl/firesteel/offheapstore/ShmAddress;J)J
 */
JNIEXPORT jlong JNICALL Java_com_hp_hpl_firesteel_offheapstore_OffHeapStore_getAttributeOffsetInHashPartition
  (JNIEnv *, jobject, jlong, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif