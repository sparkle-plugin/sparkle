/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore */

#ifndef _Included_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
#define _Included_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    ninitialize
 * Signature: (JIIIIZ)J
 */
JNIEXPORT jlong JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_ninitialize
  (JNIEnv *, jobject, jlong, jint, jint, jint, jint, jboolean);

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nstop
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nstop
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nshutdown
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nshutdown
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nCopyToNativeStore
 * Signature: (JLjava/nio/ByteBuffer;[I[Ljava/lang/Object;[I[II)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nCopyToNativeStore
  (JNIEnv *, jobject, jlong, jobject, jintArray, jobjectArray, jintArray, jintArray, jint);

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nstoreKVPairsWithIntKeys
 * Signature: (JLjava/nio/ByteBuffer;[I[I[II)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nstoreKVPairsWithIntKeys
  (JNIEnv *, jobject, jlong, jobject, jintArray, jintArray, jintArray, jint);

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nstoreKVPairsWithFloatKeys
 * Signature: (JLjava/nio/ByteBuffer;[I[F[II)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nstoreKVPairsWithFloatKeys
  (JNIEnv *, jobject, jlong, jobject, jintArray, jfloatArray, jintArray, jint);

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nstoreKVPairsWithLongKeys
 * Signature: (JLjava/nio/ByteBuffer;[I[J[II)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nstoreKVPairsWithLongKeys
  (JNIEnv *, jobject, jlong, jobject, jintArray, jlongArray, jintArray, jint);

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nstoreKVPairsWithStringKeys
 * Signature: (JLjava/nio/ByteBuffer;[I[Ljava/lang/String;[I[II)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nstoreKVPairsWithStringKeys
  (JNIEnv *, jobject, jlong, jobject, jintArray, jobjectArray, jintArray, jintArray, jint);

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nstoreKVPairsWithByteArrayKeys
 * Signature: (JLjava/nio/ByteBuffer;[I[I[II)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nstoreKVPairsWithByteArrayKeys
  (JNIEnv *, jobject, jlong, jobject, jintArray, jintArray, jintArray, jint);

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nsortAndStore
 * Signature: (JILcom/hp/hpl/firesteel/shuffle/ShuffleDataModel/MapStatus;)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nsortAndStore
  (JNIEnv *, jobject, jlong, jint, jobject);

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nwriteToHeap
 * Signature: (JI[ILjava/nio/ByteBuffer;Lcom/hp/hpl/firesteel/shuffle/ShuffleDataModel/MapStatus;)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nwriteToHeap
  (JNIEnv *, jobject, jlong, jint, jintArray, jobject, jobject);

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nstoreVValueType
 * Signature: (J[BI)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nstoreVValueType
  (JNIEnv *, jobject, jlong, jbyteArray, jint);

/*
 * Class:     com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore
 * Method:    nstoreKVTypes
 * Signature: (JII[BI[BI)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_MapSHMShuffleStore_nstoreKVTypes
  (JNIEnv *, jobject, jlong, jint, jint, jbyteArray, jint, jbyteArray, jint);

#ifdef __cplusplus
}
#endif
#endif
