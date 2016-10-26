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

import java.util.List;
import java.util.ArrayList;

/**
 * to gather shuffle data contributed from all of the Map tasks
 */
public interface ReduceShuffleStore {

       /**
         * to initialize the shuffle store at the reducer side, based on the Map status information
         * retrieved from the Map Tracker.
         *
        *  NOTE: status only records the sizes of the map buckets that belong to this reducer.
        *  we still need some hand-shake mechanism to know exactly the addresses where we can
        *  make direct fetching of the data.
        *  
        *  @param ordering to specify whether the keys at the reduce shuffle store need to be ordered or not.
        *  @param aggregation to specify whether the reducer will have the key with multiple values to 
        *  be aggregated.
        *
        */
        void initialize (int shuffleId, int reduceTaskId, int numberOfPartitions,
        		                                           boolean ordering, boolean aggregation);

        /**
         * to clean up the DRAM related resources after the reduce is done?
         */
        void stop();

        /**
         * to shutdown the reducer side store, and reclaim the resources consumed by the store for NVRAM.
         */
        void shutdown ();

        /**
         * to merge the sorted shuffle data that belongs to the this reducer. We may just prepare for
         * the merge sort step, and maybe the first key value, and the actual merge will be
         * pulled on-demand later.
         * @param statuses the map status information passed from the MapOutputTracker
         */
        void mergeSort(ShuffleDataModel.ReduceStatus statuses);


        /**
         * The K value type information will have to be passed from the map to the reducer.
         *
         * @return the type id
         */
        ShuffleDataModel.KValueTypeId getKValueTypeId();

        /**
         * to support when K value type is an arbitrary object type. to retrieve the serialized
         * type information for K values that can be de-serialized by Java/Scala. The type information
         * will be retrieved from the map side.
         */
        byte[] getKValueType();

        /**
         * to further retrieve the class definition, based on the type definition stored in the input parameter.
         * @param typeDefinition holding the value type definition
         * @return the class that represents the type definition.
         */
        Class getVValueTypeClass(byte[] typeDefinition);
        
        /**
         * NOTE: to set the K value type used in this shuffle store. This can not be set by the reducer. It is 
         * retrieved from the data produced in NVM region by the map side. 
         * @param ktype
         */
        //void setKValueTypeId(ShuffleDataModel.KValueTypeId ktype);

        /**
         * to retrieve the serialized type information for the V values that can be
         * de-serialized by Java/Scala. The type information will be retrieved from the map side.
         */
        byte[] getVValueType();


        /**
         * to retrieve a specified number, for example, 100, of Key values, along with the multi-v-values
         * that belong to each of the corresponding keys. Each such multi-v-values that belong to the same
         * K value should not be empty--it has at least one element.
         *
         * We are using bulk retrieval to amortize the cost of the JNI.
         *
         * @param kvalues  the actual Key values that are retrieved
         * @param vvalues  the actual multi-v-values that correspond to the keys retrieved. The Scala
         *                 side will have the value combiner to combine the multi-v-values.
         * @param knumbers the specified number of K values to be retrieved in bulk, by the reducer
         *                 upper layer.
         * @return the actual key/value pairs that are available (it can be the last processing
         * batch.
         */
        int getKVPairs (ArrayList<Object> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers);

        /**
         * for special arbitrary Key, to retrieve  key/value pairs.
         * @param kvalues
         * @param vvalues
         * @param knumbers
         *
         * @return the actual key/values pairs that are available (it can be the last processing batch). 
         */
        int getSimpleKVPairs (ArrayList<Object> kvalues, ArrayList<Object> vvalues, int knumbers);
        
        /**
         * for special K value type that is int, to retrieve  key/multiple values pairs.
         * @param kvalues
         * @param vvalues
         * @param knumbers
         *
         * @return the actual key/values pairs that are available (it can be the last processing batch). 
         */
        int getKVPairsWithIntKeys (ArrayList<Integer> kvalues,  ArrayList<ArrayList<Object>> vvalues, int knumbers);
        
        /**
         * for special K value type that is int, to retrieve key/value pairs
         * @param kvalues
         * @param values
         * @param knumbers  
         * @return the actual simple key/value pairs that are available (it can be the last processing batch) 
         */
        int getSimpleKVPairsWithIntKeys (ArrayList<Integer>kvalues, ArrayList<Object> values, int knumbers);

        /**
         * for special K value type that is float
         * @param kvalues
         * @param vvalues
         * @param knumbers
         * @return the actual key/value pairs that are available (it can be the last processing
         * batch.
         */
       int getKVPairsWithFloatKeys (ArrayList<Float> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers);
       
       /**
        * for special K value type that is float, to retrieve key/value pairs
        * @param kvalues
        * @param values
        * @param knumbers
        * @return the actual simple key/value pairs that are available (it can be the last processing batch) 
        */
       int getSimpleKVPairsWithFloatKeys (ArrayList<Float> kvalues, ArrayList<Object> values, int knumbers); 
       

        /**
         * for special K value type that is long
         * @param kvalues
         * @param vvalues
         * @param knumbers
         * @return the actual key/value pairs that are available (it can be the last processing
         * batch.
         */
        int getKVPairsWithLongKeys ( ArrayList<Long> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers);
        
        /**
         * for special K value type that is long, to retrieve key/value pairs
         * @param kvalues
         * @param values
         * @param knumbers
         * @return the actual simple key/value pairs that are available (it can be the last processing batch) 
         */
        int getSimpleKVPairsWithLongKeys (ArrayList<Long> kvalues, ArrayList<Object> values, int knumbers); 

        /**
         * for special K value type that is string
         * @param kvalues
         * @param vvalues
         * @param knumbers
         * @return the actual key/value pairs that are available (it can be the last processing
         * batch.
         */
        int getKVPairsWithStringKeys (ArrayList<String> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers);
        
        
        /**
         * for special K value type that is long, to retrieve key/value pairs
         * @param kvalues
         * @param values
         * @param knumbers
         * @return the actual simple key/value pairs that are available (it can be the last processing batch) 
         */
        int getSimpleKVPairsWithStringKeys (ArrayList<String> kvalues, ArrayList<Object> values, int knumbers); 
        
        /**
         * for speical K value type that is byte array, to retrieve key/values pairs
         * @param kvalues
         * @param vvalues
         * @param knumbers
         * @return
         */
        public int getKVPairsWithByteArrayKeys (
                            ArrayList<byte[]> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers);


       /**
        * for speical K value type that is byte array, to retrieve key/value pairs
        * @param kvalues
        * @param values
        * @param knumbers
        * @return
        */
        public int getSimpleKVPairsWithByteArrayKeys (
                                     ArrayList<byte[]> kvalues, ArrayList<Object> values, int knumbers);
 
        
        
        /**
         * retrieve the unique store id. 
         * @return
         */
        int getStoreId(); 
}
