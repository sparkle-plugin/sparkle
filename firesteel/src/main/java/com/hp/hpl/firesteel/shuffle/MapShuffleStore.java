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

package com.hp.hpl.firesteel.shuffle;

/**
 * to allow the Map task to store the serialized data records from Java into the C++ shuffle
 * engine, along with the partitioner number assigned for each (k,v) pair
 */
public interface MapShuffleStore {

        /**
         * to initialize the storage space for a particular shuffle stage's map instance
         * @param shuffleId the current stage id
         * @param mapTaskId  the current map task Id
         * @param numberOfPartitions  the total number of the partitions chosen for the reducer.
         * @param keyType the type of the key to be created for the shuffle store in C++ shuffle engine
         * @param sizeOfBatchSerialization: the predefined size to conduct the batched serialization from Spark.
         * @param ordering, to specify whether the keys need to be ordered or not for map shuffle.
         * @return sessionId, which is the pointer to the native C++ shuffle store.
         */
         void initialize  (int shuffleId, int mapTaskId, int numberOfPartitions,
                                                ShuffleDataModel.KValueTypeId keyType,
                                                int sizeOfBatchSerialization,
                                                boolean ordering);

         /**
          * to stop the current map store
          */
         void stop();

        /**
         * to reclaim the resources required for shuffling this map task. This one
         * is done
         *
         * NOTE: who will issue this shutdown at what time
         *
         */
         void shutdown();


        /**
         * to serialize the (K,V) pair that have the K values to be with type of
         * int, float, long, string that are supported by  C++, along with value that has arbitrary type.
         * @param kvalue key's value
         * @param vvalue the Value
         * @param partitionId the partition id corresponding to the key based on partitioning strategy.
         * @indexPosition the position of the (K,V) pair in the batch of map-side processing.
         * 
         * @param scode the key's type code: int, long, float, double, string, byte-array, object. 
         */
         void serializeKVPair (Object kvalue, Object vvalue, int partitionId, int indexPosition, int scode);


         /**
          * To store the (K,V) pairs that have the K values to be with key type of:
          * Int, long, float, double, string, byte-array, object, and the value that is with arbitrary type.
          *
          * @param numberOfPairs the number of <K,V> pairs that gets stored in the current batch
          * @param scode: the type code of the key: int, long, float, double, string, byte-array, and object
          */
         void storeKVPairs (int numberOfPairs, int scode);


        /**
         * to sort and store the sorted data into non-volatile memory that is ready for  the reducer
         * to fetch
         * @return status information that represents the map processing status
         */
         ShuffleDataModel.MapStatus sortAndStore();

        /**
        * to query what is the K value type used in this shuffle store
        * @return the K value type used for this shuffle operation.
        */
        ShuffleDataModel.KValueTypeId getKValueTypeId();

       /**
        * to set the K value type used in this shuffle store
        * @param ktype
        */
        void setKValueTypeId(ShuffleDataModel.KValueTypeId ktype);


         /**
          * based on a given particular V value, to store its value type.
          * @param Vvalue
          */
        void storeVValueType(Object Vvalue);

         /**
          * based on a given particular object based (K,V) pair, to store the corresponding types.
          * @param Kvalue
          * @param VValue
          */
        void storeKVTypes (Object Kvalue, Object VValue);

        /**
         * to support when K value type is an arbitrary object type. to retrieve the serialized
         * type information for K values that can be de-serialized by Java/Scala
        */
        byte[] getKValueType();

         /**
          * to retrieve the serialized type information for the V values that can be
          * de-serialized by Java/Scala
          */
        byte[] getVValueType();
        
        /**
         * retrieve the unique store id. 
         * @return
         */
        int getStoreId(); 

}
