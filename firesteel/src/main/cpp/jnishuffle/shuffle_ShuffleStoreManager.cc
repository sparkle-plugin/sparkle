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

#include "com_hp_hpl_firesteel_shuffle_ShuffleStoreManager.h"
#include "ShuffleStoreManager.h"


/*
 * Class:     com_hp_hpl_firesteel_shuffle_ShuffleStoreManager
 * Method:    ninitialize
 * Signature: (Ljava/lang/String;I)J
 */
JNIEXPORT jlong JNICALL Java_com_hp_hpl_firesteel_shuffle_ShuffleStoreManager_ninitialize
(JNIEnv *env , jobject obj, jstring globalHeapName, jint executorId) {
    //NOTE: the first call should be called at the begining of shm shuffle manager in Spark core, when it is guaranteed to
    //be single threaded. In this call, let's have all of the other singleton object in C++ shuffle engine to be instantiated
    //as well.
    ShuffleStoreManager *ptr= ShuffleStoreManager::getInstance();     
    //need to initialize the store manager, which further trigger the initialization of other singleton objects.
    const char *string_val = env->GetStringUTFChars(globalHeapName, NULL);
    string heapName(string_val); 
    env->ReleaseStringUTFChars(globalHeapName, string_val);

    ptr->initialize(heapName, executorId);

    long ptrValue = reinterpret_cast <long> (ptr);
    return ptrValue; 
}
/*                                                                                                                                                          
 * Class:     com_hp_hpl_firesteel_shuffle_ShuffleStoreManager                                                                                              
 * Method:    shutdown                                                                                                                                      
 * Signature: (J)V                                                                                                                                          
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_ShuffleStoreManager_shutdown
(JNIEnv *env , jobject obj, jlong ptrStoreManagerVal) {
    ShuffleStoreManager *ptr= reinterpret_cast <ShuffleStoreManager *> (ptrStoreManagerVal);
    //shutdown the map shuffle store manager.
    ptr->shutdown();
}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_ShuffleStoreManager
 * Method:    nformatshm
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_ShuffleStoreManager_nformatshm
(JNIEnv *env, jobject obj, jlong ptrStoreManagerVal) {
    ShuffleStoreManager *ptr= reinterpret_cast <ShuffleStoreManager *> (ptrStoreManagerVal);
    //format the shared-memory region
    ptr->format_shm();
}


/*
 * Class:     com_hp_hpl_firesteel_shuffle_ShuffleStoreManager
 * Method:    nregistershm
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_ShuffleStoreManager_nregistershm
(JNIEnv *env, jobject obj, jlong ptrStoreManagerVal){
  ShuffleStoreManager *ptr= reinterpret_cast <ShuffleStoreManager *> (ptrStoreManagerVal);
    //format the shared-memory region
    ptr->register_new_shm();
  
}

/*
 * Class:     com_hp_hpl_firesteel_shuffle_ShuffleStoreManager
 * Method:    ncleanup
 * Signature: (JII)V
 */
JNIEXPORT void JNICALL Java_com_hp_hpl_firesteel_shuffle_ShuffleStoreManager_ncleanup
(JNIEnv *env, jobject obj, jlong ptrToShuffleManager, jint shuffleId, jint numMaps) {
     //need to be implemented;
}
