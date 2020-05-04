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

import java.util.Arrays;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import scala.reflect.ClassTag$;

import org.apache.spark.serializer.SerializationStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * to implement the Mapside Shuffle Store. We will use the Kryo serializer to do internal
 * object serialization/de-serialization
 * 
 */
public class MapSHMShuffleStore implements MapShuffleStore {

    private static final Logger LOG = LoggerFactory.getLogger(MapSHMShuffleStore.class.getName());
    
    /**
     * NOTE this class can only work on Kryo 3.0.0
     * to allow testing in Spark shm-shuffle package. 
     */
    public static class LocalKryoByteBufferBasedSerializer {

        private ByteBufferOutput output = null;
        private Kryo kryo = null;
        //to hold the original buffer, as buffer might be resized with a different buffer object.
        private ByteBuffer originalBuffer = null;

        /**
         * the buffer needs to be the nio based buffer.
         * @param kryo
         * @param buffer if the buffer can not hold the actual object(s) to be serialized. the
         *               buffer will grow using the nio buffer direct allocation. So when asking
         *               the position, we will need to use getByteBuffer to query the current position.
         */
        public LocalKryoByteBufferBasedSerializer (Kryo kryo, ByteBuffer buffer) {
            this.output= new ByteBufferOutput(buffer);
            this.kryo = kryo;
            this.originalBuffer=buffer;

        }

        public void writeObject(Object obj) {
            this.kryo.writeClassAndObject(this.output, obj);
        }

        public void writeClass(Class type) {
            this.kryo.writeClass(this.output, type);
        }
        
        public void directCopy(byte[] val) {
            this.output.writeBytes(val);//the cursor should advance afterwards.
        }


        /**
         * due to possible byte buffer resizing, we will have to use this API to accomodate possible
         * re-sizing of the byte buffer. this is per map task byte buffer, so we will not re-sizing again.
         *
         * @return
         */
        public ByteBuffer getByteBuffer(){
            return this.output.getByteBuffer();
        }

        /**
         * to copy the whole internal byte-buffer out.
         * @return the byte array containing the entire byte buffer's content.
         */
        public byte[] toBytes() {
        	return this.output.toBytes();
        }
        
        /**
         * position the bytebuffer to position=0
         */
        public void init() {
            //to avoid that the buffer gets expanded last time and then gets released at the time
            //of last marshalling.
            this.originalBuffer.rewind();
            this.output.setBuffer(this.originalBuffer);
            //The flip() method switches a Buffer from writing mode to reading mode. 
            //Calling flip() sets the position back to 0, and sets the limit to where position just was.
            //details: http://tutorials.jenkov.com/java-nio/buffers.html#capacity-position-limit. 
            //this.output.getByteBuffer().flip();
        }
        
        /**
         * NOTE: as long as we do not pass an output stream to this serializer, this method does
         * not do anything. Thus, in our current implementation, this method call does not do anything.
         */
        public void flush() {
            this.output.flush();
        }

        /**
         * As long as we do not pass an output stream to this serializer, this method does
         * not do anything.
         */
        public void close() {
            this.output.close();
            //if I call release, this is no action taken regarding byte buffer;
            //if I call release, then the pass-in bytebuffer will be release.
            //what I need is to check whether my byte buffer really gets expanded or not
            //and I only release the one that is created due to expansion.
            //NOTE: only bytebuffer is not null. the default buffer is actually null. 
            ByteBuffer newBuffer = this.output.getByteBuffer();
            //reference comparision
            if (newBuffer != this.originalBuffer) {
                //the passed-in buffer gets expanded. then we release the expanded/
            	LOG.warn("SHM Shuffle serializer bytebuffe automatically expanded. Bigger serializer buffer reguired!");
                this.output.release();
            }
            this.output.close();
        }
    }



    private Kryo kryo= null;
    private ByteBuffer byteBuffer = null;
    private LocalKryoByteBufferBasedSerializer serializer = null;

    private ShuffleStoreManager shuffleStoreManager=null;

    private static AtomicInteger storeCounter = new AtomicInteger(0);
    private int storeId;  
    //serializer creates serialization instance. for a long lived executor, the thread pool can
    //reuse per-thread serialization instance

    /**
     * To have the caller to pass the serializer instance, which is Kryo Serializer.
     * @param kryo the instance that needs to be created from the caller. need to re-usable from a thread pool
     * @param byteBuffer needs to have the reusable bytebuffer from a  re-usable pool
     * as well
     */
    public  MapSHMShuffleStore(Kryo kryo, ByteBuffer byteBuffer,ShuffleStoreManager shuffleStoreManager) {
        this.kryo = kryo;
        this.byteBuffer = byteBuffer;
        this.serializer = new LocalKryoByteBufferBasedSerializer (kryo, byteBuffer);
        this.shuffleStoreManager= shuffleStoreManager;
        this.storeId = storeCounter.getAndIncrement(); 
    }

    private long pointerToStore=0;
    private int shuffleId=0;
    private int mapTaskId = 0;
    private int numberOfPartitions =0;
    private ShuffleDataModel.KValueTypeId keyType;
    
    //record the size of the pre-defined batch serialization size
    private int sizeOfBatchSerialization=0; 

    //the following two sets of array declaration is such that in one map shuffle whole duration,
    //we can pre-allocate the data structure required once, and then keep re-use for each iteration of 
    //store key/value pairs to C++ shuffle engine
    private int koffsets[] = null; 
    private int voffsets[]  =null; 
    private int npartitions[] = null; 
    
    //the following targets different key/value pair, not all of the defined structures will be activated
    //as it depends on which key type the map store is to handle. 
    private int nkvalues[] = null; 
    private float fkvalues[] = null; 
    private long  lkvalues[]  = null; 
    private String skvalues[] = null; 
    private Object okvalues[] = null;
    private int okhashes[] = null;
    //for string length
    private int slkvalues[]=null; 
    
    //add key ordering specification for the map/reduce shuffle store
    private boolean ordering; 

    // enable JNI callbacks by the flag.
    private boolean enableJniCallback = false;
    public void setEnableJniCallback(boolean doJniCallback) {
        this.enableJniCallback = doJniCallback;
        LOG.info("Jni Callback: " + this.enableJniCallback);
    }

    public boolean isUnsafeRow = false;

    private SerializationStream unsafeRowSerializationStream = null;

    public void setUnsafeRowSerializer(SerializationStream ss) {
        this.unsafeRowSerializationStream = ss;
    }

    public void finalizeUnsafeRowSerializer() {
        this.unsafeRowSerializationStream.close();
        this.unsafeRowSerializationStream = null;
    }

    /**
     * to initialize the storage space for a particular shuffle stage's map instance
     * @param shuffleId the current stage id
     * @param mapTaskId  the current map task Id
     * @param numberOfPartitions  the total number of the partitions chosen for the reducer.
     * @param keyType the type of the key, so that we can create the corresponding one in C++.
     * @param batchSerialization, the size of the predefined serialization batch 
     * @param odering, to specify whether the keys need to be ordered or not, for the map shuffle.
     * @return sessionId, which is the pointer to the native C++ shuffle store.
     */
     @Override
     public void initialize (int shuffleId, int mapTaskId, int numberOfPartitions,
                                 ShuffleDataModel.KValueTypeId keyType, int batchSerialization,
                                 boolean ordering){

         LOG.info( "store id" + this.storeId + " map-side shared-memory based shuffle store started"  
                   + " with ordering: " + ordering);
         
         this.shuffleId = shuffleId;
         this.mapTaskId= mapTaskId;
         this.numberOfPartitions = numberOfPartitions;
         this.keyType = keyType;
         this.sizeOfBatchSerialization =  batchSerialization; 
         this.ordering = ordering;
         
         this.koffsets = new int[this.sizeOfBatchSerialization];
         this.voffsets = new int[this.sizeOfBatchSerialization];
         this.npartitions = new int[this.sizeOfBatchSerialization];
         
         //for different key types 
         //for different key types                                                                                                                                     
         if (keyType == ShuffleDataModel.KValueTypeId.Int) {
             this.nkvalues = new int[this.sizeOfBatchSerialization];
         }
         else if (keyType == ShuffleDataModel.KValueTypeId.Float) {
            this.fkvalues = new float [this.sizeOfBatchSerialization];
         }
         else if (keyType == ShuffleDataModel.KValueTypeId.Long) {
             this.lkvalues = new long [this.sizeOfBatchSerialization];
         }
		 else if (keyType == ShuffleDataModel.KValueTypeId.Double) {
             this.lkvalues = new long [this.sizeOfBatchSerialization];
         }
         else if (keyType == ShuffleDataModel.KValueTypeId.String) {
             this.skvalues = new String[this.sizeOfBatchSerialization];
             this.slkvalues = new int [this.sizeOfBatchSerialization];
         }
         else if (keyType == ShuffleDataModel.KValueTypeId.ByteArray){
             //do nothing, as it will use koffsets and voffsets.                                                                                                       
         }
         else if (keyType == ShuffleDataModel.KValueTypeId.Object){
             this.okvalues = new Object[this.sizeOfBatchSerialization];
             this.okhashes = new int[this.sizeOfBatchSerialization];
         }
         else {
            throw new UnsupportedOperationException ( "key type: " + keyType + " is not supported");
         }

         
         //also initialize the serializer
         this.serializer.init();
         
         //for object, it has been take care of by store kv pairs and thus use koffsets and voffsets
         this.pointerToStore=
                 ninitialize(this.shuffleStoreManager.getPointer(),
                         shuffleId, mapTaskId, numberOfPartitions, keyType.getState(),
                         ordering);
     }

     private native long ninitialize(
             long ptrToShuffleManager, int shuffleId, int mapTaskId, int numberOfPartitions,
             int keyType, boolean ordering);


    @Override
    public void stop() {
        LOG.info( "store id " + this.storeId + " map-side shared-memory based shuffle store stopped with id:"
                + this.shuffleId + "-" + this.mapTaskId);
        //recycle the shuffle resource
        //(1) retrieve the shuffle resource object from the thread specific storage
        //(2) return it to the shuffle resource tracker
        //ThreadLocalShuffleResourceHolder holder = new ThreadLocalShuffleResourceHolder();
        //ThreadLocalShuffleResourceHolder.ShuffleResource resource = holder.getResource();
        //if (resource != null) {
           //return it to the shuffle resource tracker
        //   this.shuffleStoreManager.getShuffleResourceTracker().recycleSerializationResource(resource);
        //}
        //else {
        //    LOG.error( "store id " + this.storeId + " map-side shared-memory based shuffle store stopped with id:"
	    //       + this.shuffleId + "-" + this.mapTaskId + " does not have recycle serialized resource");
        //
	    //}
        //then stop the native resources as well. 
        nstop(this.pointerToStore);
    }

    //to stop and reclaim the DRAM resource.
    private native void nstop (long ptrToStore);

    /**
     * to shutdown the session and reclaim the NVM resources required for shuffling this map task.
     */
    @Override
    public void shutdown() {
        LOG.info( "store id " + this.storeId + " map-side shared-memory based shuffle store shutdown with id:"
                    + this.shuffleId + "-" + this.mapTaskId);
        nshutdown(this.shuffleStoreManager.getPointer(), this.pointerToStore);
    }

    private native void nshutdown(long ptrToMgr, long ptrToStore);

    //for key = int, value = object 
    protected  void serializeVInt (int kvalue, Object vvalue, int partitionId, int indexPosition) {
        this.nkvalues[indexPosition] = kvalue;
        //serialize v into the bytebuffer. serializer has been initialized already.
        ByteBuffer currentVBuf = this.isUnsafeRow
	    ? this.serializer.originalBuffer
	    : this.serializer.getByteBuffer();
        if (this.isUnsafeRow) {
	    this.npartitions[indexPosition] = kvalue;

            this.unsafeRowSerializationStream
                .writeValue(vvalue, ClassTag$.MODULE$.Object());
            this.unsafeRowSerializationStream.flush();
        } else {
	    this.npartitions[indexPosition] = partitionId;

            this.serializer.writeObject(vvalue);
        }
        this.voffsets[indexPosition]= currentVBuf.position();
    }
    
    //for key = float, value = object 
    protected void serializeVFloat (float kvalue, Object vvalue, int partitionId, int indexPosition) {
    	
    	this.fkvalues[indexPosition] = kvalue;
        this.npartitions[indexPosition] = partitionId;
        //serialize v into the bytebuffer. serializer has been initialized already.
        this.serializer.writeObject(vvalue);
        ByteBuffer currentVBuf = this.serializer.getByteBuffer();
        this.voffsets[indexPosition]= currentVBuf.position();
    }
    
     //for key = long, value = object
     protected void serializeVLong (long kvalue, Object vvalue, int partitionId, int indexPosition) {
    	
    	this.lkvalues[indexPosition] = kvalue;
        this.npartitions[indexPosition] = partitionId;
        //serialize v into the bytebuffer. serializer has been initialized already.
        this.serializer.writeObject(vvalue);
        ByteBuffer currentVBuf = this.serializer.getByteBuffer();
        this.voffsets[indexPosition]= currentVBuf.position();
     }
    
     //for key = byte-array, value = object
     protected void serializeVByteArray (byte[] kvalue, Object vvalue, int partitionId, int indexPosition) {
     	
        this.npartitions[indexPosition] = partitionId;
        this.serializer.directCopy(kvalue);
        ByteBuffer currentKBuf = this.serializer.getByteBuffer();
        this.koffsets[indexPosition]= currentKBuf.position();
        //if (LOG.isDebugEnabled()) {
        //	LOG.debug("serialize KV pairs with byte-array " + i + "-th key has position: " + this.koffsets[i]);
        //}
        this.serializer.writeObject(vvalue);
        //the serialization buffer may be full and the new buffer is created. 
        ByteBuffer currentVBuf = this.serializer.getByteBuffer();
        this.voffsets[indexPosition]= currentVBuf.position();
     }
     
     //for key = double, value = object
     protected void serializeVDouble (double kvalue, Object vvalue, int partitionId, int indexPosition) {
       throw new RuntimeException ("serialize V for double value is not implemented");
     }
     
     //for key = string, value = object
     protected void serializeVString (String kvalue, Object vvalue, int partitionId, int indexPosition) {
         throw new RuntimeException ("serialize V for String value is not implemented");
         
     }
     
     //for key = object, value = object
     public void serializeVObject (Object kvalue, Object vvalue, int partitionId, int indexPosition) {
        this.okvalues[indexPosition] = kvalue;
        this.okhashes[indexPosition] = kvalue.hashCode();
        this.npartitions[indexPosition] = partitionId;
        this.serializer.writeObject(vvalue);
        ByteBuffer currentVBuf = this.serializer.getByteBuffer();
        this.voffsets[indexPosition]= currentVBuf.position();
     }

     @Override 
     public void serializeKVPair (Object kvalue, Object vvalue, int partitionId, int indexPosition, int scode) {
          //rely on Java to generate fast switch statement. 
    	  switch (scode) {
    	    case 0: 
		 serializeVInt (((Integer)kvalue).intValue(), vvalue, partitionId,  indexPosition); 
    	    	 break;
    	    case 1: 
		 serializeVLong (((Long)kvalue).longValue(), vvalue, partitionId,  indexPosition);
    	    	 break;
    	    case 2:
		 serializeVFloat (((Float)kvalue).floatValue(), vvalue,  partitionId,  indexPosition);
                 break;
    	    case 3: 
		 serializeVDouble (((Double)kvalue).doubleValue(), vvalue,  partitionId,  indexPosition); 
    	    	 break;
    	    case 4: 
    	    	 serializeVString ((String)kvalue, vvalue,  partitionId,  indexPosition);
    	    	 break;
    	    case 5:
    	    	 serializeVByteArray ((byte[])kvalue, vvalue,  partitionId,  indexPosition);
    	    	 break;
    	    case 6:
    	    	 serializeVObject (kvalue, vvalue,  partitionId,  indexPosition); 
    	    	 break;
    	    default: 
                 throw new RuntimeException ("no specialied key-value type expected");
    	    
    	  }
     }
    
     @Override
     public void storeKVPairs(int numberOfPairs, int scode) {
    	//rely on Java to generate fast switch statement. 
   	  switch (scode) {
   	    case 0: 
   	    	storeKVPairsWithIntKeys (numberOfPairs); 
   	    	 break;
   	    case 1: 
   	    	storeKVPairsWithLongKeys (numberOfPairs);
   	    	 break;
   	    case 2:
   	    	storeKVPairsWithFloatKeys(numberOfPairs);
                break;
   	    case 3: 
   	    	 throw new RuntimeException ("key type of double is not implemented");
   	    	 //break;
   	    case 4: 
   	    	 storeKVPairsWithStringKeys (numberOfPairs);
   	    	 break;
   	    case 5:
   	    	 storeKVPairsWithByteArrayKeys (numberOfPairs);
   	    	 break;
            case 6:
                // If JNI map/reduce disabled,
                // we store serialized records later.
                if (this.enableJniCallback) {
                    copyToNativeStore(numberOfPairs);
                }
                break;
   	    default: 
   	    	throw new RuntimeException ("unknown key type is encountered");
   	    
   	  }
     }
    

    /**
     * Copy arbitrary key-value pairs to the native map-side store.
     * @param numPairs The number of pairs to be transfer to the native store.
     */
    private void copyToNativeStore(int numPairs) {
        this.serializer.init();

        ByteBuffer holder = this.serializer.getByteBuffer();
        nCopyToNativeStore(this.pointerToStore, holder, this.voffsets,
                           this.okvalues, this.okhashes, this.npartitions, numPairs);

        this.serializer.init();
    }

    private native void nCopyToNativeStore (long ptrToStore, ByteBuffer holder, int[] voffsets,
                                            Object[] okvalues, int[] okhashes, int[] partitions, int numPairs);

    //Special case: to store the (K,V) pairs that have the K values to be with type of Integer
    public void storeKVPairsWithIntKeys (int numberOfPairs) {
        //NOTE: this byte buffer my be re-sized during serialization.
    	this.serializer.init();//to initialize the serializer;
        ByteBuffer holder = this.serializer.getByteBuffer();
     
        if (LOG.isDebugEnabled()) 
        {
            LOG.debug ( "store id " + this.storeId + " in method storeKVPairsWithIntKeys" + " numberOfPairs is: " + numberOfPairs); 
     	    //for direct buffer, array() throws unsupported operation exception. 
            //byte[] bytesHolder = holder.array();
            // parse the values and turn them into bytes.
            for (int i=0; i<numberOfPairs;i++) {

                LOG.debug ( "store id " + this.storeId + " " + i + "-th key's value: " + nkvalues[i]);

                int vStart=0;
                if (i>0) {
                    vStart = voffsets[i-1];
                }
                int vEnd=voffsets[i];
                int vLength = vEnd-vStart;
                
                LOG.debug ("store id " + this.storeId + " value: " + i +  " has length: " + vLength + " start: " + vStart + " end: " + vEnd);
                LOG.debug ("store id " + this.storeId + " [artition Id: " + i + " is: "  + npartitions[i]);
            }

        }


        nstoreKVPairsWithIntKeys(this.pointerToStore,
                                    holder, this.voffsets, this.nkvalues, npartitions, numberOfPairs);
        
        this.serializer.init();//to initialize the serializer;
    }

    private native void nstoreKVPairsWithIntKeys (long ptrToStore, ByteBuffer holder, int[] voffsets,
                                         int [] kvalues, int[] partitions, int numberOfPairs);
   
    //Special case: to store the (K,V) pairs that have the K values to be with type of float
    public void storeKVPairsWithFloatKeys (int numberOfPairs) {
    	this.serializer.init();//to initialize the serializer;
        ByteBuffer holder = this.serializer.getByteBuffer();
    
        if (LOG.isDebugEnabled()) {
            // parse the values and turn them into bytes.
       	    LOG.debug ( "store id " + this.storeId + " in method storeKVPairsWithFloatKeys" + " numberOfPairs is: " + numberOfPairs);
        	
            for (int i=0; i<numberOfPairs;i++) {

                LOG.debug( "store id " + this.storeId + " key: " + i  + "with value: " + fkvalues[i]);

                int vStart=0;
                if (i>0){
                    vStart = voffsets[i-1];
                }
                int vEnd=voffsets[i];
                int vLength = vEnd-vStart;
                
                LOG.debug("store id " + this.storeId + " value: " + i +  " has length: " + vLength + " start: " + vStart + " end: " + vEnd);
                LOG.debug("store id " + this.storeId + " partition Id: " + i + " is: "  + npartitions[i]);
            }

        }

        nstoreKVPairsWithFloatKeys(this.pointerToStore, holder, this.voffsets, this.fkvalues, this.npartitions, numberOfPairs);
        this.serializer.init();//to initialize the serializer;
    }

    private native void nstoreKVPairsWithFloatKeys (long ptrToStrore, ByteBuffer holder, int[] voffsets,
                                           float[] kvalues, int[] partitions, int numberOfPairs);

    //Special case: to store the (K,V) pairs that have the K values to be with type of Long
    public void storeKVPairsWithLongKeys (int numberOfPairs) {

    	this.serializer.init();//to initialize the serializer;
        ByteBuffer holder = this.serializer.getByteBuffer();
       
        if (LOG.isDebugEnabled()) {
        	
            LOG.debug ( "store id " + this.storeId + " in method storeKVPairsWithLongKeys" + " numberOfPairs is: " + numberOfPairs);
        	
            // parse the values and turn them into bytes.
            for (int i=0; i<numberOfPairs;i++) {

                LOG.debug( "store id " + this.storeId + " key: " + i  + "with value: " + lkvalues[i]);

                int vStart=0;
                if (i>0) {
                    vStart = voffsets[i-1];
                }
                int vEnd=voffsets[i];
                int vLength = vEnd-vStart;
               
                LOG.debug("store id " + this.storeId + " value: " + i +  " has length: " + vLength + " start: " + vStart + " end: " + vEnd);
                LOG.debug("store id " + this.storeId + " partition Id: " + i + " is: "  + npartitions[i]);
            }

        }


        nstoreKVPairsWithLongKeys (this.pointerToStore, holder, this.voffsets, this.lkvalues, this.npartitions, numberOfPairs);
        
        this.serializer.init();//to initialize the serializer;
    }

    private native void nstoreKVPairsWithLongKeys (long ptrToStore, ByteBuffer holder, int[] voffsets,
                                          long[] kvalues, int[] partitions, int numberOfPairs);

    //Special case: to store the (K,V) pairs that have the K values to be with type of String
    public void storeKVPairsWithStringKeys (int numberOfPairs){
    	this.serializer.init();//to initialize the serializer;
    	ByteBuffer holder = this.serializer.getByteBuffer();

        if (LOG.isDebugEnabled()) {
       	    LOG.debug ( "store id " + this.storeId + " in method storeKVPairsWithStringKeys" + " numberOfPairs is: " + numberOfPairs);
        	
            // parse the values and turn them into bytes.
            for (int i=0; i<numberOfPairs;i++) {

                LOG.debug( "store id " + this.storeId + " key: " + i  + "with value: " + skvalues[i]);

                int vStart=0;
                if (i>0){
                    vStart = voffsets[i-1];
                }
                int vEnd=voffsets[i];
                int vLength = vEnd-vStart;
                
                LOG.debug("store id " + this.storeId + " value: " + i +  " has length: " + vLength + " start: " + vStart + " end: " + vEnd);
                LOG.debug("store id " + this.storeId + " partition Id: " + i + " is: "  + npartitions[i]);
            }

        }

        //NOTE: this will need to do the fixing later. 
        for (int i=0; i<numberOfPairs; i++) {
        	this.slkvalues[i] = this.skvalues[i].length();  
        }

        nstoreKVPairsWithStringKeys (this.pointerToStore, holder, 
        		                     this.voffsets, this.skvalues, this.slkvalues, this.npartitions, numberOfPairs);
        this.serializer.init();//to initialize the serializer;

    }

    private native void nstoreKVPairsWithStringKeys (long ptrToStore, ByteBuffer holder, int[] voffsets,
                               String[] kvalues, int nkvalueLengths[], int partitions[], int numberOfPairs);

    
    //Special case: to store the (K,V) pairs that have the K values to be with type of byte-array
    public void storeKVPairsWithByteArrayKeys(int numberOfPairs) {
    	this.serializer.init();//to see whether this is the problem to not initialize it.
        ByteBuffer holder=this.serializer.getByteBuffer(); //this may be re-sized.

        if (LOG.isDebugEnabled()) {
            // parse the values and turn them into bytes.
            for (int i=0; i<numberOfPairs;i++) {
                int kLength = 0;
                int kStart=0;
                int kEnd=0;
                if (i==0) {
                    kLength=this.koffsets[i];
                    kStart=0;
                    kEnd=this.koffsets[i]; //exclusive;
                }
                else {
                    kStart=voffsets[i-1];
                    kEnd=koffsets[i]; //exclusive
                    kLength=kEnd-kStart;
                }
                byte[] kValue = new byte[kLength];
                holder.get(kValue); //then the position of the byte buffer advance kLength.
                LOG.debug("store id " + this.storeId +  " " 
                                  + i + "-th key" + "with length: " +  kLength + " value: " +  getHex(kValue));


                int vStart=koffsets[i];
                int vEnd=voffsets[i];
                int vLength = vEnd-vStart;
                byte[] vValue = new byte[vLength];
                holder.get(vValue);//then the position of the byte buffer advance vLength
                LOG.debug( "store id " + this.storeId + " value: " + i + " with value: " + getHex(vValue));

                LOG.debug( "store id " + this.storeId + " partition Id: " + i + " is: "  + npartitions[i]);
            }

        }

        nstoreKVPairsWithByteArrayKeys(this.pointerToStore,
                                             holder, this.koffsets, this.voffsets, this.npartitions,numberOfPairs);
        
        this.serializer.init();
    }

    private native void nstoreKVPairsWithByteArrayKeys(long ptrToStore, ByteBuffer holder,
                                      int [] koffsets, int [] voffsets, int[] partitions, int numberOfPairs);


    /**
     * to sort and store the sorted data into non-volatile memory that is ready for  the reduder
     * to fetch
     * @return status information that represents the map processing status
     */
    @Override
    public ShuffleDataModel.MapStatus sortAndStore() {
         ShuffleDataModel.MapStatus status = new ShuffleDataModel.MapStatus ();

         //the status details will be updated in JNI.
         nsortAndStore(this.pointerToStore, this.numberOfPartitions, status);
         
         if (LOG.isDebugEnabled()) { 
        	 //show the information 
             long retrieved_mapStauts[] = status.getMapStatus();
             long retrieved_shmRegionIdOfIndexChunk = status.getRegionIdOfIndexBucket();
             long retrieved_offsetToIndexBucket = status.getOffsetOfIndexBucket();
             
	         LOG.debug ("store id " + this.storeId + 
	        		 " in sortAndStore, total number of buckets is: " + retrieved_mapStauts.length);
	         for (int i=0; i<retrieved_mapStauts.length; i++) {
	         	LOG.debug ("store id" + this.storeId + " **in sortAndStore " + i + "-th bucket size: " + retrieved_mapStauts[i]);
	         }
	         LOG.debug ("store id " + this.storeId + 
	        		 " **in sortAndStore, retrieved shm region name: " + retrieved_shmRegionIdOfIndexChunk);
	         LOG.debug ("store id " + this.storeId +
	        		 " **in sortAndStore, retrieved offset to index chunk is:" + retrieved_offsetToIndexBucket);
         }
         
         return status;
    }

    /**
     * The map status only returns for each map's bucket, what each reducer will get what size of
     * byte-oriented content.
     *
     * @param ptrToStoreMgr: pointer to store manager.
     * @param mapStatus the array that will be populated by the underly C++ shuffle engine, before
     *                  it gets returned.
     * @return the Map status information on all of the buckets produced from the Map task.
     */
    private native void nsortAndStore (long ptrToStore, 
                                       int totalNumberOfPartitions,  ShuffleDataModel.MapStatus mapStatus);

    /**
     * Write partitioned and sorted records into the GlobalHeap.
     * The records must to be serialized and contained in DirectBuffer to use in JNI.
     **/
    public ShuffleDataModel.MapStatus writeToHeap(ByteBuffer buff, int[] sizes) {
        ShuffleDataModel.MapStatus status = new ShuffleDataModel.MapStatus();
        nwriteToHeap(this.pointerToStore, this.numberOfPartitions, sizes, buff, status);
        return status;
    }

    private native void nwriteToHeap(long ptrToStore,
                                     int totalNumberOfPartitions,
                                     int[] partitionLengths,
                                     ByteBuffer holder,
                                     ShuffleDataModel.MapStatus mapStatus);

    /**
     * to query what is the K value type used in this shuffle store
     * @return the K value type used for this shuffle operation.
     */
    @Override
    public ShuffleDataModel.KValueTypeId getKValueTypeId() {
    	return (this.ktypeId);
    }

    private ShuffleDataModel.KValueTypeId ktypeId = ShuffleDataModel.KValueTypeId.Object;

    /**
     * to set the K value type used in this shuffle store
     * @param ktypeId
     */
    @Override
    public void setKValueTypeId(ShuffleDataModel.KValueTypeId ktypeId) {
        this.ktypeId=ktypeId;
    }


    private byte[] vvalueType=null;
    private byte[] kvalueType=null;

    //to record whether the value type has been pushed down to the C++ shuffle engine or not. 
    private boolean vvalueTypeStored = false; 
    /**
     * based on a given particular V value, to store its value type. The corresponding key is either Java 
     * primitive types (int, long, float, double), or String. 
     * 
     * @param Vvalue
     */
    @Override
    public void storeVValueType(Object Vvalue) {
    	if (!vvalueTypeStored) {
	        this.serializer.init();
	        this.serializer.writeClass(Vvalue.getClass());
	
	        this.vvalueType = this.serializer.toBytes();
	     
	        //at the end, close it
	        this.serializer.close();
	
	        if (LOG.isDebugEnabled()) {
	        	LOG.debug("store id " + this.storeId + " value Type size: " + this.vvalueType.length
	        			 + " value: " + getHex(this.vvalueType) );
	        }
	        nstoreVValueType(this.pointerToStore, this.vvalueType, this.vvalueType.length);
	        //update the flag
	        vvalueTypeStored=true;
    	}
    }

    private native void nstoreVValueType(long ptrToStore, byte[] vvalueType, int vtypeLength);

    //to record whether the value type has been pushed down to the C++ shuffle engine or not. 
    private boolean keyvalueTypeStored = false; 
    
    /**
     * based on a given particular object based (K,V) pair, to store the corresponding types for
     * both Key and Value.
     * @param Kvalue
     * @param Vvalue
     */
    @Override
    public void storeKVTypes (Object Kvalue, Object Vvalue){
    	if (!keyvalueTypeStored) {
	        this.serializer.init();
	
	        //for Key value type
	        this.serializer.writeClass(Kvalue.getClass());
	        int kposition =  this.serializer.getByteBuffer().position();
	
	        int klength = this.serializer.getByteBuffer().position(); //to copy the byte array out.
	        this.kvalueType = new byte[klength];
	        this.serializer.getByteBuffer().put(this.vvalueType);
	
	        //for Value type
	         this.serializer.writeClass(Vvalue.getClass());
	         int vposition = this.serializer.getByteBuffer().position();
	
	         byte[] fullBuffer = new byte[vposition];
	         this.serializer.getByteBuffer().get(fullBuffer);
	
	         //copy only a portition of it to the vvalueType
	         this.vvalueType = Arrays.copyOfRange(fullBuffer, kposition, vposition);
	
	        //at the end, close it
	        this.serializer.close();
	
	        if (LOG.isDebugEnabled()) {
	            LOG.debug("store id " + this.storeId + " key Type:" + getHex(this.kvalueType));
	            LOG.debug("store id " + this.storeId + " value Type: " + getHex(this.vvalueType));
	        }
	
	        nstoreKVTypes(this.shuffleStoreManager.getPointer(), shuffleId, mapTaskId,
	        		            this.kvalueType, this.kvalueType.length,
	        		            this.vvalueType, this.vvalueType.length);
	        
	        keyvalueTypeStored=true;
    	}
    }

    private native void nstoreKVTypes(long ptrToStoreMgr,
                                      int shuffleId, int mapId, 
                                      byte[] kvalueType, int ktypeLength, byte[] vvalueType, int vtypeLength);

    /**
     * to support when K value type is an arbitrary object type. to retrieve the serialized
     * type information for K values that can be de-serialized by Java/Scala
     */
    @Override
    public byte[] getKValueType() {
        return this.kvalueType;
    }

    /**
     * to retrieve the serialized type information for the V values that can be
     * de-serialized by Java/Scala
     */
    @Override
    public  byte[] getVValueType() {
         return this.vvalueType;
    }

    
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
