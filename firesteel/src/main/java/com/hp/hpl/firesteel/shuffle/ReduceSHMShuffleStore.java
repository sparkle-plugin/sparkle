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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class ReduceSHMShuffleStore implements ReduceShuffleStore {
    private static final Logger LOG=LoggerFactory.getLogger(ReduceSHMShuffleStore.class.getName());

    //to allow testing in Spark shm-shuffle package. 
    public static class  LocalKryoByteBufferBasedDeserializer {
        private ByteBufferInput input = null;
        private Kryo kryo = null;
        private ByteBuffer originalBuffer = null;

        //NOTE:if passed-in bytebuffer is too small to hold the object, the de-serializer will have
        //kryo exception thrown. Since we de-serialize objects in a much smaller chunk, we will not
        // need to have a buffer of say, 1 GB, like what is required by Spark.
        public  LocalKryoByteBufferBasedDeserializer (Kryo kryo, ByteBuffer  buffer) {
            this.input = new ByteBufferInput(buffer);
            this.kryo = kryo;
            this.originalBuffer = buffer;
        }

        /**
         * for de-serializer, the byte buffer will never gets changed.
         * @return
         */
        public ByteBuffer getByteBuffer(){
            return this.input.getByteBuffer();
        }
        /**
         * we can simplify it later using write/read class and write/read object separately.
         * NOTE: asm-5.0.jar is required for read object: http://forge.ow2.org/projects/asm/ 
         * @return
         */
        public Object readObject() {
        	//NOTE: this needs to be paired with LocalKryoByteBufferBasedSerializer
            return (this.kryo.readClassAndObject(this.input));
        }

        public Class readClass() {
            return (this.kryo.readClass(this.input).getType());
        }

        /**
         * direct read the specified bytes from the underlying input buffer. 
         * @param val the buffer to be filled with the de-serialized bytes.
         * @return the number of the bytes that get read. 
         */
        public int directRead(byte[] val) {
        	return (this.input.read (val));
        }

        /**
         * position the bytebuffer to position=0
         */
        public void init() {
            this.input.getByteBuffer().rewind();
            this.input.setBuffer(this.input.getByteBuffer());
        }

        public void close() {
            this.input.close();
        }
    }

    private Kryo kryo=null;
    private ByteBuffer byteBuffer = null;
    private LocalKryoByteBufferBasedDeserializer deserializer = null;
    private ShuffleStoreManager shuffleStoreManager= null;

    private static AtomicInteger storeCounter = new AtomicInteger(0);
    private int storeId;  
    

    public ReduceSHMShuffleStore(Kryo kryo, ByteBuffer byteBuffer, ShuffleStoreManager shuffleStoreManager) {
        this.kryo = kryo;
        this.byteBuffer = byteBuffer;
        this.deserializer = new LocalKryoByteBufferBasedDeserializer (kryo, byteBuffer);
        this.shuffleStoreManager= shuffleStoreManager;
        this.storeId = storeCounter.getAndIncrement(); 
   
    }

    private long pointerToStore=0;
    private int shuffleId = 0;
    private int reduceId =0;
    private int numberOfPartitions = 0;
    //to keep track of the key ordering property
    private boolean ordering; 
    //to keep trakc of the aggregation property 
    private boolean aggregation; 


    @Override
    public void initialize (int shuffleId, int reduceId, int numberOfPartitions, boolean ordering, boolean aggregation) {
        this.shuffleId = shuffleId;
        this.reduceId= reduceId;
        this.numberOfPartitions = numberOfPartitions;
        //defer its use until merge-sort.
        this.ordering = ordering; 
        //defer its use until merge, to decide whether we will have straight-forward pass through
        this.aggregation = aggregation; 
        
        ninitialize(this.shuffleStoreManager.getPointer(), shuffleId, reduceId, numberOfPartitions);
        LOG.info("store id " +  this.storeId + " reduce-side shared-memory based shuffle store started with id:"
                                                      + this.shuffleId + "-" + this.reduceId);
    }

    //NOTE: the key type and value type definition goes through the shuffle channel in C++. We do not need
    //to pass it here.
    //NOTE sure whether we will create the reduce side shuffle store from Java side or not.
    //but stop and shutdown can be. We need to check out Spark's implementation on who drives this.
    private native void ninitialize(
            long ptrToShuffleManager, int shuffleId, int reduceId, int numberOfPartitions);

    //Note: this call currently can only be invoked after mergesort, as that is when the actual
    //reduce shuffle store gets created.
    @Override
    public void stop() {
        LOG.info("store id " + this.storeId + " reduce-side shared-memory based shuffle store stopped with id:"
                + this.shuffleId + "-" + this.reduceId);
        
        //to return shuffle related resource back to the tracker. 
        //ThreadLocalShuffleResourceHolder holder = new ThreadLocalShuffleResourceHolder();
        //ThreadLocalShuffleResourceHolder.ShuffleResource resource = holder.getResource();
        //if (resource != null) {
           //return it to the shuffle resource tracker
	//  this.shuffleStoreManager.getShuffleResourceTracker().recycleSerializationResource(resource);
        //}
        //else {
	// LOG.info("store id " + this.storeId + " reduce-side shared-memory based shuffle store stopped with id:"
	//	    + this.shuffleId + "-" + this.reduceId + " has re-usable shuffle resource == null");
	//}
        
        //then return the native resources as well. 
        nstop(this.pointerToStore);
    }

    //shuffle store manager is the creator of map shuffle manager and reduce shuffle manager
    private native void nstop(long ptrToStore);


    @Override
    public void shutdown (){
        LOG.info("store id " + this.storeId + "reduce-side shared-memory based shuffle store shutdown with id:"
                         + this.shuffleId + "-" + this.reduceId);
        nshutdown(this.pointerToStore);
    }

    private native void nshutdown(long ptrToStore);

 
    @Override
    public void mergeSort(ShuffleDataModel.ReduceStatus statuses) {
    	if (LOG.isDebugEnabled()) { 
	        LOG.debug ("store id " + this.storeId + " reduce-side shared-memory based shuffle store perform merge-sort with id:"
	                       + this.shuffleId + "-" + this.reduceId);
	        int retrieved_mapIds[] = statuses.getMapIds(); 
	        LOG.debug ("store id " + this.storeId + " in mergeSort, total number of maps is: " + retrieved_mapIds.length);
	        for (int i=0; i<retrieved_mapIds.length; i++) {
	        	LOG.debug ("store id " + this.storeId + " ***in mergeSort " + i + "-th map id is: " + retrieved_mapIds[i]);
	        }
	        long retrieved_regionIdsOfIndexChunks[] = statuses.getRegionIdsOfIndexChunks();
	        for (int i=0; i<retrieved_regionIdsOfIndexChunks.length; i++) {
	        	LOG.debug ("store id " + this.storeId + " ***in mergeSort " + i + "-th region id is: " + retrieved_regionIdsOfIndexChunks[i]);
	        }
	        long retrieved_chunkoffsets[] = statuses.getOffsetsOfIndexChunks();
	        for (int i=0; i<retrieved_chunkoffsets.length; i++) {
	        	LOG.debug ("store id " + this.storeId + 
	        			      " ***in mergeSort " + i + "-th chunk offset is: 0x " + retrieved_chunkoffsets[i]);
	        }
	        long retrieved_sizes[] = statuses.getSizes();
	        for (int i=0; i<retrieved_sizes.length; i++) {
	        	LOG.debug ("store id " + this.storeId + " ***in mergeSort " + i + "-th bucket size is: " + retrieved_sizes[i]);
	        }
    	}
        int totalBuckets = statuses.getMapIds().length;
        this.pointerToStore = nmergeSort(this.shuffleStoreManager.getPointer(), shuffleId, reduceId,
                                   statuses, totalBuckets, this.numberOfPartitions, this.byteBuffer,
                                   this.byteBuffer.capacity(), this.ordering, this.aggregation);
    }

    public void createShuffleStore(ShuffleDataModel.ReduceStatus statuses) {
        int totalBuckets = statuses.getMapIds().length;
        this.pointerToStore = ncreateShuffleStore(this.shuffleStoreManager.getPointer(), shuffleId, reduceId,
                                                  statuses, totalBuckets, this.byteBuffer,
                                                  this.byteBuffer.capacity(), this.ordering, this.aggregation);
    }

    /**
     * @param buffer is to passed in the de-serialization buffer's pointer.
     * @param ordering to specify whether the keys need to be ordered at the reduce shuffle store side.
     * @param aggregation to specify whether the values associated with the key needs to be aggregated.
     * if not, we will choose the pass-through. 
     * @return the created reduce shuffle store native pointer
     */
    private native long nmergeSort(long ptrToShuffleManager, int shuffleId, int reduceId,
                                         ShuffleDataModel.ReduceStatus statuses, int totalBuckets,
                                         int numberOfPartitions, ByteBuffer buffer, int bufferCapacity,
                                         boolean ordering, boolean aggregation);

    private native long ncreateShuffleStore(long ptrToShuffleManager, int shuffleId, int reduceId,
                                            ShuffleDataModel.ReduceStatus statuses,
                                            int totalBuckets, ByteBuffer buffer, int bufferCapacity,
                                            boolean ordering, boolean aggregation);

    /**
     * TODO: we will need to design the shared-memory region data structure, so that we can carry
     * the key/value type definition over from map shuffle store via the shuffle channel, plus this enum
     * category information for key.
     * @return
     */
    @Override
    public ShuffleDataModel.KValueTypeId getKValueTypeId() {
       int val = ngetKValueTypeId(this.pointerToStore);
       
       if (val==ShuffleDataModel.KValueTypeId.Int.state){
           return ShuffleDataModel.KValueTypeId.Int;
       }
       else if (val == ShuffleDataModel.KValueTypeId.Long.state){
            return ShuffleDataModel.KValueTypeId.Long;
       }
       else if (val == ShuffleDataModel.KValueTypeId.Float.state){
           return ShuffleDataModel.KValueTypeId.Float;
       }
       else if (val == ShuffleDataModel.KValueTypeId.Double.state){
           return ShuffleDataModel.KValueTypeId.Double;
       }
       else if (val == ShuffleDataModel.KValueTypeId.String.state){
           return ShuffleDataModel.KValueTypeId.String;
       }
       else if (val == ShuffleDataModel.KValueTypeId.ByteArray.state){
           return ShuffleDataModel.KValueTypeId.ByteArray;
       }
       else if (val ==  ShuffleDataModel.KValueTypeId.Object.state){
           return ShuffleDataModel.KValueTypeId.Object;
       }
       else if (val == ShuffleDataModel.KValueTypeId.Unknown.state) {
           throw new RuntimeException("Unknown key value type encountered");
       }
       else {
    	   throw new RuntimeException("unsupported key value type encountered");
       }
    }

    /**
     * Current implementation in C++ has the Key adn Value type definition retrieved from the merge-sort channel
     * associated with the map bucket. Thus this getKValueTypeID 
     * @return
     */
    private native int ngetKValueTypeId(long ptrToStore);


    @Override
    public byte[] getKValueType() {
        byte[] result = ngetKValueType(this.pointerToStore);
        return result;
    }

    /**
     * to retrieve from C++ side the key definition for the arbitrary object based key. 
     */
    private native byte[] ngetKValueType(long ptrToStore);

    @Override
    public byte[] getVValueType() {
    	byte[] result = ngetVValueType(this.pointerToStore);
    	return result;
    }

    @Override
    public Class getVValueTypeClass(byte[] typeDefinition) {
         //we make sure that the byte buffer has big enough size to hold typeDefinition
    	 if (this.deserializer.getByteBuffer().capacity() < typeDefinition.length) {
    		 throw new RuntimeException (
    				 "store id " + this.storeId + 
    				 " shm shuffle deserialization has too small byte-buffer to deserialize value type.");
    	 }
    	 this.deserializer.getByteBuffer().put(typeDefinition);
    	 this.deserializer.getByteBuffer().position(0);
    	 this.deserializer.getByteBuffer().limit(typeDefinition.length);
		 
    	 this.deserializer.init();
		 
    	 Class retrievedClass = deserializer.readClass();
		 deserializer.close();
    	
    	 return retrievedClass;
    }
    
    /**
     * to retrieve from C++ side the value type definition for the arbitrary object based value. 
     */
    private native byte[] ngetVValueType(long ptrToStore);

    @Override
    public int getKVPairs (ArrayList<Object> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers) {
        // prep key holders and value offsets in the direct buffer.
        Object okvalues[] = new Object[knumbers];
        int pvVOffsets[] = new int[knumbers];

        this.deserializer.init();
        int actualKVPairs = nGetKVPairs(this.pointerToStore, okvalues,
                                        this.deserializer.getByteBuffer(),
                                        this.byteBuffer.capacity(), pvVOffsets, knumbers);

        for (int i=0; i<actualKVPairs; i++) {
            Object key = okvalues[i];
            kvalues.set(i, key);

            ArrayList<Object> holder = new ArrayList<Object>();

            ByteBuffer populatedByteBuffer = this.deserializer.getByteBuffer();
            int endPosition = pvVOffsets[i];
            while (populatedByteBuffer.position() < endPosition) {
                Object s = this.deserializer.readObject();//readClassObject at this time.
                holder.add(s);
            }

            vvalues.set(i, holder);
        }
        this.deserializer.close();

        return actualKVPairs;
    }

    /**
     * No need to specify the key offsets, as there is always only one key
     * per (k, {vp,1,vp,2...vp,k}}.
     * @param byteBuffer holds the information on values that
     * @param voffsets
     * @param knumbers
     * @return
     */
    private native int nGetKVPairs(long ptrToStore, Object kvalues[], ByteBuffer
                                   byteBuffer, int buffer_capacity,
                                   int voffsets[], int knumbers);

    /**
     * return unordered kv pairs to ShuffleReader's Iterator.
     * @param kvalues
     * @param vvalues
     * @param knumbers
     * @return the number of kv pairs which are deserialized from the global heap.
     */
    public int getSimpleKVPairs (ArrayList<Object> kvalues, ArrayList<Object> vvalues, int knumbers) {
        // prep key holders and value offsets in the direct buffer.
        Object okvalues[] = new Object[knumbers];
        int pvVOffsets[] = new int[knumbers];

        this.deserializer.init();
        int actualKVPairs = nGetSimpleKVPairs(this.pointerToStore, okvalues,
                                              this.deserializer.getByteBuffer(),
                                              this.byteBuffer.capacity(), pvVOffsets, knumbers);

        for (int i=0; i<actualKVPairs; i++) {
            Object key = okvalues[i];
            kvalues.set(i, key);

            Object value = null;
            ByteBuffer populatedByteBuffer = this.deserializer.getByteBuffer();
            {
                int endPosition = pvVOffsets[i];
                if( populatedByteBuffer.position() < endPosition){
                    value = this.deserializer.readObject(); //readClassObject at this time.
                }
                else {
                    throw new RuntimeException ("deserializer cannot de-serialize object following the offset boundary");
                }
            }
            vvalues.set(i, value);
        }
        this.deserializer.close();

        return actualKVPairs;
    }

    private native int nGetSimpleKVPairs(long ptrToStore, Object kvalues[], ByteBuffer
                                         byteBuffer, int buffer_capacity,
                                         int voffsets[], int knumbers);

    //vvalues only has the first layer of element populated with empty object. the second level
    //of object array will have to be created by Java.
    @Override
    public int getKVPairsWithIntKeys (ArrayList<Integer> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers) {
      ShuffleDataModel.MergeSortedResult mergeResult = new ShuffleDataModel.MergeSortedResult();
      boolean bufferExceeded = mergeResult.getBufferExceeded();
      if (bufferExceeded) {
          LOG.error( "store id " + this.storeId +
        		            " deserialization buffer for shm shuffle reducer is exceeded; need to configure a bigger one");
          return 0;
      }

      //NOTE: in the init method, the byte buffer gets rewinded. 
      this.deserializer.init();
      //merge results get updated.
      int actualKVPairs = nGetKVPairsWithIntKeys(this.pointerToStore,
              this.deserializer.getByteBuffer(), this.byteBuffer.capacity(), knumbers, mergeResult);

      //
      //then populate back to the list the key values.
      int[] pkValues = mergeResult.getIntKvalues();
      int[] pvVOffsets =mergeResult.getVoffsets();//offset to each group of {vp,1, vp,2...,vp,k}.
      for (int i=0; i<actualKVPairs; i++){
          kvalues.set(i, pkValues[i]);
          //the corresponding value pairs
          ArrayList<Object> holder = new ArrayList<Object>();
          //WARNING: can we move this call out of the scope: 
          ByteBuffer populatedByteBuffer = this.deserializer.getByteBuffer();
          {

            int endPosition = pvVOffsets[i];
            while ( populatedByteBuffer.position() < endPosition){
                Object p = this.deserializer.readObject();//readClassObject at this time.
                holder.add(p);
            }
          }

          vvalues.set(i, holder);
      }
      this.deserializer.close();

      //for debug purpose 
      if (LOG.isDebugEnabled()) {
	      LOG.debug ( "store id " + this.storeId + 
	    		      " in method getKVPairsWithIntKeys, actual KV pairs received is: " + actualKVPairs);
	      
	      for (int i=0; i<actualKVPairs; i++)  {
	    	  LOG.debug ( i + "-th key: " + kvalues.get(i));
	    	  ArrayList<Object> rvvalues = vvalues.get(i);
	    	  for (int m=0; m < rvvalues.size(); m++) {
	    		  LOG.debug ( "store id " + this.storeId + " **" + m + "-th value: " + rvvalues.get(m)); 
	    	  }
	      } 
      }
      
      return actualKVPairs;
    }

    /**
     * the native call to pass the keys in an integer array, while having the byte buffer to hold
     * the values {v11,v12, ...v1n} for key k1, which will need to be de-serialized at the Java side
     * byte the Kryo serializer.
     *
     * @param byteBuffer holding the byte buffer that will be passed in for de-serialization. The byte buffer
     *                   is already allocated from the de-serializer side. We just need to copy data into
     *                   this buffer!
     * @param mergeResult the holder that holds the kvalue[] and voffsets[], and maybe the indicator
     *                    that the single buffer is not sufficient to hold the values for the number of
     *                    key-values retrieved.
     * @param knumbers the specified maximum number of keys retrieved
     * @return the actual number of keys retrieved
     */
    private native int nGetKVPairsWithIntKeys(long ptrToShuffleStore,
                      ByteBuffer byteBuffer, int buffer_capacity, 
                      int knumbers, ShuffleDataModel.MergeSortedResult mergeResult);
 
    
    @Override
    public  int getSimpleKVPairsWithIntKeys (ArrayList<Integer>kvalues, ArrayList<Object> values, int knumbers) {
    	//I can still use the same APIs for the simple key/value pairs retrieval 
        ShuffleDataModel.MergeSortedResult mergeResult = new ShuffleDataModel.MergeSortedResult();
        boolean bufferExceeded = mergeResult.getBufferExceeded();
        if (bufferExceeded) {
            LOG.error( "store id " + this.storeId +
          		            " deserialization buffer for shm shuffle reducer is exceeded; need to configure a bigger one");
            return 0;
        }

        //NOTE: in the init method, the byte buffer gets rewinded. 
        this.deserializer.init();
        //merge results get updated.
        int actualKVPairs = nGetSimpleKVPairsWithIntKeys(this.pointerToStore,
                this.deserializer.getByteBuffer(), this.byteBuffer.capacity(), knumbers, mergeResult);

        //
        //then populate back to the list the key values.
        int[] pkValues = mergeResult.getIntKvalues();
        int[] pvVOffsets =mergeResult.getVoffsets();//offset to each {vp,1, vp,2...,vp,k}
        ByteBuffer populatedByteBuffer = this.deserializer.getByteBuffer();
        for (int i=0; i<actualKVPairs; i++){
            kvalues.set(i, pkValues[i]);
            //the corresponding value pairs
            Object p = null; 
            {

              int endPosition = pvVOffsets[i];
              if ( populatedByteBuffer.position() < endPosition){
                  p = this.deserializer.readObject();//readClassObject at this time.
              }
              else {
            	  throw new RuntimeException ("deserializer cannot de-serialize object following the offset boundary");
              }
            }

            values.set(i, p);
        }
        this.deserializer.close();

        //for debug purpose 
        if (LOG.isDebugEnabled()) {
  	      LOG.debug ( "store id " + this.storeId + 
  	    		      " in method getSimpleKVPairsWithIntKeys, actual KV pairs received is: " + actualKVPairs);
  	      
  	      for (int i=0; i<actualKVPairs; i++)  {
  	    	  LOG.debug ( i + "-th key: " + kvalues.get(i));
  	    	  Object rvvalue = values.get(i);
  	    	  LOG.debug ( "store id " + this.storeId + " ** retrieved value: " + rvvalue); 
  	    	  
  	      } 
        }
        
        return actualKVPairs;
    }
    
    
    private native int nGetSimpleKVPairsWithIntKeys(long ptrToShuffleStore,
            ByteBuffer byteBuffer, int buffer_capacity, 
            int knumbers, ShuffleDataModel.MergeSortedResult mergeResult);
    

    @Override
    public int getKVPairsWithFloatKeys (ArrayList<Float> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers) {

        ShuffleDataModel.MergeSortedResult mergeResult = new ShuffleDataModel.MergeSortedResult();
        this.deserializer.init();
        int actualKVPairs = nGetKVPairsWithFloatKeys(this.pointerToStore,
                this.deserializer.getByteBuffer(), this.byteBuffer.capacity(), knumbers, mergeResult);

        float pkValues[] = mergeResult.getFloatKvalues();
        int pvVOffsets[] = mergeResult.getVoffsets();  //offset to each group of {vp,1, vp,2...,vp,k}.

        //then populate back to the list the key values.
        for (int i=0; i<actualKVPairs; i++){
            kvalues.set(i, pkValues[i]);
            //the corresponding value pairs
            ArrayList<Object> holder = new ArrayList<Object>();
            //WARNING: can we move this call out of the scope: 
            ByteBuffer populatedByteBuffer = this.deserializer.getByteBuffer();
            {

                int endPosition = pvVOffsets[i];
                while ( populatedByteBuffer.position() < endPosition){
                    Object p = this.deserializer.readObject();//readClassObject at this time.
                    holder.add(p);
                }
            }

            vvalues.set(i, holder);
        }
        this.deserializer.close();

        return actualKVPairs;
    }

    private native int nGetKVPairsWithFloatKeys(long ptrToShuffleStore,
                        ByteBuffer byteBuffer, int buffer_capacity,
                        int knumbers, ShuffleDataModel.MergeSortedResult mergeResult);
    
    @Override 
    public int getSimpleKVPairsWithFloatKeys (ArrayList<Float> kvalues, ArrayList<Object> values, int knumbers) {
    	 ShuffleDataModel.MergeSortedResult mergeResult = new ShuffleDataModel.MergeSortedResult();
         this.deserializer.init();
         int actualKVPairs = nGetSimpleKVPairsWithFloatKeys(this.pointerToStore,
                 this.deserializer.getByteBuffer(), this.byteBuffer.capacity(), knumbers, mergeResult);

         float pkValues[] = mergeResult.getFloatKvalues();
         int pvVOffsets[] = mergeResult.getVoffsets();  //offset to each group of {vp,1, vp,2...,vp,k}.

         //then populate back to the list the key values.
         ByteBuffer populatedByteBuffer = this.deserializer.getByteBuffer();
         for (int i=0; i<actualKVPairs; i++){
             kvalues.set(i, pkValues[i]);
             Object p = null; 
             {
                 int endPosition = pvVOffsets[i];
                 if (populatedByteBuffer.position() < endPosition){
                     p = this.deserializer.readObject();//readClassObject at this time.
                 }
                 else {
                	 throw new RuntimeException ("deserializer cannot de-serialize object following the offset boundary");
                 }
             }

             values.set(i, p);
         }
         this.deserializer.close();

         return actualKVPairs;
    }
    
    private native int nGetSimpleKVPairsWithFloatKeys(long ptrToShuffleStore,
            ByteBuffer byteBuffer, int buffer_capacity,
            int knumbers, ShuffleDataModel.MergeSortedResult mergeResult);
    

    @Override
    public int getKVPairsWithLongKeys (ArrayList<Long> kvalues,  ArrayList<ArrayList<Object>> vvalues, int knumbers){
        ShuffleDataModel.MergeSortedResult mergeResult = new ShuffleDataModel.MergeSortedResult();
       
        boolean bufferExceeded = mergeResult.getBufferExceeded();
        if (bufferExceeded) {
            LOG.error( "store id " + this.storeId +
          		            " deserialization buffer for shm shuffle reducer is exceeded; need to configure a bigger one");
            return 0;
        }
        
        this.deserializer.init();
        int actualKVPairs = nGetKVPairsWithLongKeys(this.pointerToStore,
                this.deserializer.getByteBuffer(), this.byteBuffer.capacity(), knumbers, mergeResult);

        long pkValues[] = mergeResult.getLongKvalues();
        int pvVOffsets[] = mergeResult.getVoffsets();  //offset to each group of {vp,1, vp,2...,vp,k}.


        //then populate back to the list the key values.
        for (int i=0; i<actualKVPairs; i++){
            kvalues.set(i, pkValues[i]);
            //the corresponding value pairs
            ArrayList<Object> holder = new ArrayList<Object>();
            //WARNING: can we move this call out of the scope: 
            ByteBuffer populatedByteBuffer = this.deserializer.getByteBuffer();
            {

                int endPosition = pvVOffsets[i];
                while ( populatedByteBuffer.position() < endPosition){
                    Object p = this.deserializer.readObject();//readClassObject at this time.
                    holder.add(p);
                }
            }

            vvalues.set(i, holder);
        }
        this.deserializer.close();

        //for debug purpose 
        if (LOG.isDebugEnabled()) {
  	      LOG.debug ( "store id " + this.storeId + 
  	    		      " in method getKVPairsWithLongKeys, actual KV pairs received is: " + actualKVPairs);
  	      
  	      for (int i=0; i<actualKVPairs; i++)  {
  	    	  LOG.debug ( i + "-th key: " + kvalues.get(i));
  	    	  ArrayList<Object> rvvalues = vvalues.get(i);
  	    	  for (int m=0; m < rvvalues.size(); m++) {
  	    		  LOG.debug ( "store id " + this.storeId + " **" + m + "-th value: " + rvvalues.get(m)); 
  	    	  }
  	      } 
        }
        
        return actualKVPairs;
    }

    private native int nGetKVPairsWithLongKeys(long pointerToStore,
                       ByteBuffer byteBuffer, int buffer_capacity, 
                       int knumbers, ShuffleDataModel.MergeSortedResult mergeResult);
    
    @Override  
    public int getSimpleKVPairsWithLongKeys (ArrayList<Long> kvalues, ArrayList<Object> values, int knumbers) {
    	   ShuffleDataModel.MergeSortedResult mergeResult = new ShuffleDataModel.MergeSortedResult();
    	   
    	   boolean bufferExceeded = mergeResult.getBufferExceeded();
           if (bufferExceeded) {
               LOG.error( "store id " + this.storeId +
             		            " deserialization buffer for shm shuffle reducer is exceeded; need to configure a bigger one");
               return 0;
           }

           this.deserializer.init();
           int actualKVPairs = nGetSimpleKVPairsWithLongKeys(this.pointerToStore,
                   this.deserializer.getByteBuffer(), this.byteBuffer.capacity(), knumbers, mergeResult);

           long pkValues[] = mergeResult.getLongKvalues();
           int pvVOffsets[] = mergeResult.getVoffsets();  //offset to each group of {vp,1, vp,2...,vp,k}.

           //then populate back to the list the key values.
           ByteBuffer populatedByteBuffer = this.deserializer.getByteBuffer();
           for (int i=0; i<actualKVPairs; i++){
               kvalues.set(i, pkValues[i]);
               //the corresponding value pairs
               Object p = null; 
               {
                   int endPosition = pvVOffsets[i];
                   if ( populatedByteBuffer.position() < endPosition){
                       p = this.deserializer.readObject();//readClassObject at this time.
                   }
                   else {
                	   throw new RuntimeException ("deserializer cannot de-serialize object following the offset boundary");
                   }
               }

               values.set(i, p);
           }
           this.deserializer.close();

           //for debug purpose 
           if (LOG.isDebugEnabled()) {
     	      LOG.debug ( "store id " + this.storeId + 
     	    		      " in method getSimpleKVPairsWithLongKeys, actual KV pairs received is: " + actualKVPairs);
     	      
     	      for (int i=0; i<actualKVPairs; i++)  {
     	    	  LOG.debug ( i + "-th key: " + kvalues.get(i));
     	    	  Object rvvalue = values.get(i);
     	    	  LOG.debug ( "store id " + this.storeId + " ** retrieved value: " + rvvalue); 
     	    	  
     	      } 
           }
           
           return actualKVPairs;
    }

    private native int nGetSimpleKVPairsWithLongKeys(long pointerToStore,
            ByteBuffer byteBuffer, int buffer_capacity, 
            int knumbers, ShuffleDataModel.MergeSortedResult mergeResult);
    

    @Override
    public int getKVPairsWithByteArrayKeys (
                         ArrayList<byte[]> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers){
        ShuffleDataModel.MergeSortedResult mergeResult = new ShuffleDataModel.MergeSortedResult();

        boolean bufferExceeded = mergeResult.getBufferExceeded();
        if (bufferExceeded) {
            LOG.error( "store id " + this.storeId +
                    " deserialization buffer for shm shuffle reducer is exceeded; need to configure a bigger one");
            return 0;
        }

        this.deserializer.init();
        int actualKVPairs = nGetKVPairsWithByteArrayKeys(this.pointerToStore,
                this.deserializer.getByteBuffer(), this.byteBuffer.capacity(), knumbers, mergeResult);

        int pkOffsets[] = mergeResult.getKoffsets();
        int pVOffsets[] = mergeResult.getVoffsets(); //offset to each group of {vp,1, vp,2...,vp,k}.

        ByteBuffer populatedByteBuffer = this.deserializer.getByteBuffer();
        //then populate back to the list the key values.
        for (int i=0; i<actualKVPairs; i++){
            int lengthOfKValue = 0;
            if (i==0) {
                lengthOfKValue = pkOffsets[i];
            }
            else {
                lengthOfKValue = pkOffsets[i] - pVOffsets[i-1];
            }

            byte[] kvalue = new byte[lengthOfKValue];

            this.deserializer.directRead(kvalue);
            kvalues.set(i, kvalue);
            
            //the corresponding value pairs
            ArrayList<Object> holder = new ArrayList<Object>();
            {
                //after K, bytebuffer holds the values.
                //the start position is pKoffsets[i], which has been advanced from the "get" above.
                int endPosition = pVOffsets[i];
                while ( populatedByteBuffer.position() < endPosition){
                    Object p = this.deserializer.readObject();//readClassObject at this time.
                    holder.add(p);
                }
            }

            vvalues.set(i, holder);
        }
        this.deserializer.close();

        return actualKVPairs;
    }

    private native int nGetKVPairsWithByteArrayKeys(long pointerToStore,
                                                 ByteBuffer byteBuffer, int buffer_capacity,
                                                 int knumbers, ShuffleDataModel.MergeSortedResult mergeResult);

    @Override
    public int getSimpleKVPairsWithByteArrayKeys (
                                     ArrayList<byte[]> kvalues, ArrayList<Object> values, int knumbers) {
        ShuffleDataModel.MergeSortedResult mergeResult = new ShuffleDataModel.MergeSortedResult();

        boolean bufferExceeded = mergeResult.getBufferExceeded();
        if (bufferExceeded) {
            LOG.error( "store id " + this.storeId +
                    " deserialization buffer for shm shuffle reducer is exceeded; need to configure a bigger one");
            return 0;
        }

        this.deserializer.init();
        int actualKVPairs = nGetSimpleKVPairsWithByteArrayKeys(this.pointerToStore,
                this.deserializer.getByteBuffer(), this.byteBuffer.capacity(), knumbers, mergeResult);

        int pkOffsets[] = mergeResult.getKoffsets();
        int pVOffsets[] = mergeResult.getVoffsets();

        //then populate back to the list the key values.
        ByteBuffer populatedByteBuffer = this.deserializer.getByteBuffer();
        for (int i=0; i<actualKVPairs; i++){
            int lengthOfKValue = 0;
            if (i==0) {
                lengthOfKValue = pkOffsets[i];
            }
            else {
                lengthOfKValue = pkOffsets[i] - pVOffsets[i-1];
            }

            byte[] kvalue = new byte[lengthOfKValue];
            //the buffer position will automatically advance with lengthOfKValue.
            this.deserializer.directRead (kvalue);
            kvalues.set(i, kvalue);
            //the corresponding value pairs
            Object p = null;
            {
                //after K, bytebuffer holds the values.
                //the start position is pKoffsets[i], which has been advanced from the "get" above.
                int endPosition = pVOffsets[i];
                if ( populatedByteBuffer.position() < endPosition){
                    p = this.deserializer.readObject();//readClassObject at this time.
                }
                else {
                    throw new RuntimeException ("deserializer cannot de-serialize object following the offset boundary");
                }
            }

            values.set(i, p);
        }
        this.deserializer.close();

        return actualKVPairs;
    }

    private native int nGetSimpleKVPairsWithByteArrayKeys(long pointerToStore,
                                                 ByteBuffer byteBuffer, int buffer_capacity,
                                                 int knumbers, ShuffleDataModel.MergeSortedResult mergeResult);

    @Override
    public int getKVPairsWithStringKeys (ArrayList<String> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers){
        ShuffleDataModel.MergeSortedResult mergeResult = new ShuffleDataModel.MergeSortedResult();
        this.deserializer.init();
        int actualKVPairs = nGetKVPairsWithStringKeys(this.pointerToStore,
                            this.deserializer.getByteBuffer(), this.byteBuffer.capacity(), knumbers, mergeResult);

        String pkValues[] = mergeResult.getStringKvalues();
        int pvVOffsets[] = new int[knumbers];  //offset to each group of {vp,1, vp,2...,vp,k}.

        //then populate back to the list the key values.
        for (int i=0; i<actualKVPairs; i++){
            kvalues.set(i, pkValues[i]);
            //the corresponding value pairs
            ArrayList<Object> holder = new ArrayList<Object>();
            //WARNING: can we move this call out of the scope: 
            ByteBuffer populatedByteBuffer = this.deserializer.getByteBuffer();
            {

                int endPosition = pvVOffsets[i];
                while ( populatedByteBuffer.position() < endPosition){
                    Object p = this.deserializer.readObject();//readClassObject at this time.
                    holder.add(p);
                }
            }

            vvalues.set(i, holder);
        }
        this.deserializer.close();

        return actualKVPairs;
    }

    private native int nGetKVPairsWithStringKeys(long pointerToStore,
                        ByteBuffer byteBuffer, int buffer_capacity,
                        int knumbers, ShuffleDataModel.MergeSortedResult mergeResult);
    
 
    
    
    @Override
    public int getSimpleKVPairsWithStringKeys (ArrayList<String> kvalues, ArrayList<Object> values, int knumbers) {
    	 ShuffleDataModel.MergeSortedResult mergeResult = new ShuffleDataModel.MergeSortedResult();
         this.deserializer.init();
         int actualKVPairs = nGetSimpleKVPairsWithStringKeys(this.pointerToStore,
                                this.deserializer.getByteBuffer(), this.byteBuffer.capacity(), knumbers, mergeResult);

         String pkValues[] = mergeResult.getStringKvalues();
         int pvVOffsets[] = new int[knumbers];  //offset to each group of {vp,1, vp,2...,vp,k}.

         //then populate back to the list the key values.
         ByteBuffer populatedByteBuffer = this.deserializer.getByteBuffer();
         for (int i=0; i<actualKVPairs; i++){
             kvalues.set(i, pkValues[i]);
             //the corresponding value pairs
             Object p = null;
             {

                 int endPosition = pvVOffsets[i];
                 if ( populatedByteBuffer.position() < endPosition){
                     p = this.deserializer.readObject();//readClassObject at this time.
                 }
                 else {
              	   throw new RuntimeException ("deserializer cannot de-serialize object following the offset boundary");
                 }
             }

             values.set(i, p);
         }
         this.deserializer.close();

         return actualKVPairs;
    }
    
    private native int nGetSimpleKVPairsWithStringKeys(long pointerToStore,
            ByteBuffer byteBuffer, int buffer_capacity,
            int knumbers, ShuffleDataModel.MergeSortedResult mergeResult);

    
    @Override
    public int getStoreId() {
    	return this.storeId; 
    }
    
    
    private static final String HEXES = "0123456789ABCDEF";

    public static String getHex( byte [] raw ) {
        if ( raw == null ) {
            return null;
        }
        final StringBuilder hex = new StringBuilder( 2 * raw.length );
        for ( final byte b : raw ) {
            hex.append(HEXES.charAt((b & 0xF0) >> 4))
                    .append(HEXES.charAt((b & 0x0F))).append(" ");
        }
        return hex.toString();
    }
}
