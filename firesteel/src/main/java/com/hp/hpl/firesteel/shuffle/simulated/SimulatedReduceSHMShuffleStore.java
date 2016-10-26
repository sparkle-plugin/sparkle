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

package com.hp.hpl.firesteel.shuffle.simulated;

import com.hp.hpl.firesteel.shuffle.*;
import com.esotericsoftware.kryo.Kryo;
import com.hp.hpl.firesteel.shuffle.ReduceShuffleStore;
import com.hp.hpl.firesteel.shuffle.ShuffleDataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;


/**
 * To simualted reduce side shuffle engine. For unit testing purpose.
 */
public class SimulatedReduceSHMShuffleStore implements ReduceShuffleStore {
    private static final Logger LOG= LoggerFactory.getLogger(SimulatedReduceSHMShuffleStore.class.getName());


    public SimulatedReduceSHMShuffleStore
                     (Kryo kryo, ByteBuffer byteBuffer, SimulatedShuffleStoreManager shuffleStoreManager) {

    }


    @Override
    public void initialize (int shuffleId, int reduceId, int numberOfPartitions, boolean ordering, boolean aggregation) {
        LOG.info("Map-side shared-memory based shuffle store started.");

    }

    @Override
    public void stop () {
        LOG.info("Reduce-side shared-memory based shuffle store stooped.");

    }

    @Override
    public void shutdown (){
        LOG.info("Reduce-side shared-memory based shuffle store stopped with id:");
    }


    @Override
    public void mergeSort(ShuffleDataModel.ReduceStatus statuses) {
        LOG.info("Reduce-side shared-memory based shuffle store perform merge-sort with id:");

    }

    private ShuffleDataModel.KValueTypeId kvalueTypeId = ShuffleDataModel.KValueTypeId.Object;

    @Override
    public ShuffleDataModel.KValueTypeId getKValueTypeId() {
         return this.kvalueTypeId;
    }

    //for unit testing purpose
    //@Override
    public void setKValueTypeId(ShuffleDataModel.KValueTypeId ktype) {
       this.kvalueTypeId = ktype;
    }

    @Override
    public byte[] getKValueType() {
        throw new UnsupportedOperationException("not implemented yet.");
    }


    @Override
    public byte[] getVValueType() {
        throw new UnsupportedOperationException("not implemented yet.");
    }

    @Override
    public Class getVValueTypeClass(byte[] typeDefinition) {
    	throw new UnsupportedOperationException("not implemented yet.");
    }
    
    //we need to pass in the value holder and key holder, given the number of maximum knumbers
    //we will get.
    @Override
    public int getKVPairs (ArrayList<Object> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers) {
        int actualKVPairs = 0;
        //when actual kv pairs smaller than the specified knumbers, it indicates the end of the de-serialization
        //stream.
        if (counterKVPairsWithIntKeys++ < 3) {
            actualKVPairs=knumbers;
        }
        else {
            actualKVPairs = 2;
        }

        //NOTE: this example is to simulate word count example, k is string, v is integer value.
        Random randomNumber = new Random(System.currentTimeMillis());
        for (int i=0; i<knumbers; i++) {
            kvalues.add(new Integer(i).toString());
            ArrayList<Object> multipleValues = new ArrayList<Object>();
            for (int j=0; j<5; j++) {
                int x =  (i+j);
                multipleValues.add(x);
            }
            vvalues.add(multipleValues);
        }

        return actualKVPairs;
    }

    public int getSimpleKVPairs (ArrayList<Object> kvalues, ArrayList<Object> vvalues, int knumbers) {
    	 int actualKVPairs = 0;
         //when actual kv pairs smaller than the specified knumbers, it indicates the end of the de-serialization
         //stream.
         if (counterKVPairsWithIntKeys++ < 3) {
             actualKVPairs=knumbers;
         }
         else {
             actualKVPairs = 2;
         }

         //NOTE: this example is to simulate word count example, k is string, v is integer value.
         
         for (int i=0; i<knumbers; i++) {
             kvalues.add(new Integer(i).toString());
             int x =  (i+2);
           
             vvalues.add(x);
         }

         return actualKVPairs;
    }

    private static int counterKVPairsWithIntKeys = 0;
    //vvalues only has the first layer of element populated with empty object. the second level
    //of object array will have to be created by Java.
    @Override
    public int getKVPairsWithIntKeys (ArrayList<Integer> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers) {
        int actualKVPairs = 0;
        //when actual kv pairs smaller than the specified knumbers, it indicates the end of the de-serialization
        //stream.
        if (counterKVPairsWithIntKeys++ < 3) {
            actualKVPairs=knumbers;
        }
        else {
            actualKVPairs = 2;
        }

        Random randomNumber = new Random(System.currentTimeMillis());
        for (int i=0; i<knumbers; i++) {
            kvalues.add(i);
            ArrayList<Object> multipleValues = new ArrayList<Object>();
            for (int j=0; j<5; j++) {
                Integer x = i+j;
                multipleValues.add(x);
            }
            vvalues.add(multipleValues);
        }


        return actualKVPairs;
    }

    @Override 
    public int getSimpleKVPairsWithIntKeys (ArrayList<Integer>kvalues, ArrayList<Object> values, int knumbers) {
    	 int actualKVPairs = 0;
         //when actual kv pairs smaller than the specified knumbers, it indicates the end of the de-serialization
         //stream.
         if (counterKVPairsWithIntKeys++ < 3) {
             actualKVPairs=knumbers;
         }
         else {
             actualKVPairs = 2;
         }

         for (int i=0; i<knumbers; i++) {
             kvalues.add(i);
             Integer x = i+2;
             values.add(x);
         }


         return actualKVPairs;
    }


    @Override
    public int getKVPairsWithFloatKeys (ArrayList<Float> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers) {
        int actualKVPairs = 0;
        //when actual kv pairs smaller than the specified knumbers, it indicates the end of the de-serialization
        //stream.
        if (counterKVPairsWithIntKeys++ < 3) {
            actualKVPairs=knumbers;
        }
        else {
            actualKVPairs = 2;
        }

        Random randomNumber = new Random(System.currentTimeMillis());
        for (int i=0; i<knumbers; i++) {
            kvalues.add((float)i);
            ArrayList<Object> multipleValues = new ArrayList<Object>();
            for (int j=0; j<5; j++) {
                float x = (float) (i+j);
                multipleValues.add(x);
            }
            vvalues.add(multipleValues);
        }


        return actualKVPairs;
    }

    
    @Override
    public int getSimpleKVPairsWithFloatKeys (ArrayList<Float> kvalues, ArrayList<Object> values, int knumbers) {
        int actualKVPairs = 0;
        //when actual kv pairs smaller than the specified knumbers, it indicates the end of the de-serialization
        //stream.
        if (counterKVPairsWithIntKeys++ < 3) {
            actualKVPairs=knumbers;
        }
        else {
            actualKVPairs = 2;
        }


        for (int i=0; i<knumbers; i++) {
            kvalues.add((float)i);
 
            float x = (float) (i+2);
            values.add(x);
        }

        return actualKVPairs;
    }
    
    

    @Override
    public int getKVPairsWithLongKeys (ArrayList<Long> kvalues,  ArrayList<ArrayList<Object>> vvalues, int knumbers){
        int actualKVPairs = 0;
        //when actual kv pairs smaller than the specified knumbers, it indicates the end of the de-serialization
        //stream.
        if (counterKVPairsWithIntKeys++ < 3) {
            actualKVPairs=knumbers;
        }
        else {
            actualKVPairs = 2;
        }

        Random randomNumber = new Random(System.currentTimeMillis());
        for (int i=0; i<knumbers; i++) {
            kvalues.add((long)i);
            ArrayList<Object> multipleValues = new ArrayList<Object>();
            for (int j=0; j<5; j++) {
                long x = (long) (i+j);
                multipleValues.add(x);
            }
            vvalues.add(multipleValues);
        }


        return actualKVPairs;
    }
    
    
    @Override
    public int getSimpleKVPairsWithLongKeys (ArrayList<Long> kvalues, ArrayList<Object> values, int knumbers) {
    	   int actualKVPairs = 0;
           //when actual kv pairs smaller than the specified knumbers, it indicates the end of the de-serialization
           //stream.
           if (counterKVPairsWithIntKeys++ < 3) {
               actualKVPairs=knumbers;
           }
           else {
               actualKVPairs = 2;
           }

           for (int i=0; i<knumbers; i++) {
               kvalues.add((long)i);
               long x = (long) (i+2);
               values.add(x);
           }

           return actualKVPairs;
    }

    
    @Override
    public int getKVPairsWithStringKeys (ArrayList<String> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers){
        int actualKVPairs = 0;
        //when actual kv pairs smaller than the specified knumbers, it indicates the end of the de-serialization
        //stream.
        if (counterKVPairsWithIntKeys++ < 3) {
            actualKVPairs=knumbers;
        }
        else {
            actualKVPairs = 2;
        }

        Random randomNumber = new Random(System.currentTimeMillis());
        for (int i=0; i<knumbers; i++) {
            kvalues.add(new Integer(i).toString());
            ArrayList<Object> multipleValues = new ArrayList<Object>();
            for (int j=0; j<5; j++) {
                int x =  (i+j);
                multipleValues.add(new Integer(x).toString());
            }
            vvalues.add(multipleValues);
        }


        return actualKVPairs;
    }
    
 
    @Override
    public int getSimpleKVPairsWithStringKeys (ArrayList<String> kvalues, ArrayList<Object> values, int knumbers) {
    	  int actualKVPairs = 0;
          //when actual kv pairs smaller than the specified knumbers, it indicates the end of the de-serialization
          //stream.
          if (counterKVPairsWithIntKeys++ < 3) {
              actualKVPairs=knumbers;
          }
          else {
              actualKVPairs = 2;
          }

          for (int i=0; i<knumbers; i++) {
              kvalues.add(new Integer(i).toString());
              int x =  (i+2);
              values.add(x);
          }

          return actualKVPairs;
    }


    @Override
    public int getKVPairsWithByteArrayKeys (
            ArrayList<byte[]> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers) {
    	return 0;
    }


    @Override
    public int getSimpleKVPairsWithByteArrayKeys (
                     ArrayList<byte[]> kvalues, ArrayList<Object> values, int knumbers){
    	return 0;
    }

    @Override
    public int getStoreId() {
    	return 0; 
    }
}
