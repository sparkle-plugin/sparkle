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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;

import com.esotericsoftware.kryo.Kryo;

import java.nio.ByteBuffer;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 

/**
 * Per executor process resource management (for example, allocate big chunk of global memory
 * dedicated for the map engines. Similar to object store manager
 */
public class ShuffleStoreManager {
    private static final Logger LOG = LoggerFactory.getLogger(ShuffleStoreManager.class.getName());

    private long pointerToShuffleManager =0;
    private ShuffleStoreTracker shuffleStoreTracker = null; 
    private ShuffleResourceTracker shuffleResourceTracker = null; 

    private boolean initialized =false;
    private boolean shutdown =false;
    
    private String globalHeapName;
    private int executorId; //as the region id.
    private int maximumNumberOfTaskThreads; 

    //the atomic counter to be assigned to each task thread as the logic thread identifier 
    private AtomicInteger logicalThreadCounter = new AtomicInteger(0);
    
    //this is a single object.
    public final static ShuffleStoreManager INSTANCE = new ShuffleStoreManager();

    private ShuffleStoreManager () {
        //only one library will be loaded.
        String libraryName = "jnishmshuffle";
        initNativeLibrary(libraryName);
    }

    /**
     * to load native shared libraries
     */
    private void initNativeLibrary(String libraryName) {
         try {
             System.loadLibrary(libraryName);
             LOG.info(libraryName + " shared library loaded via System.loadLibrary");
         }
         catch (UnsatisfiedLinkError ex){
             try {
                 NativeLibraryLoader.loadLibraryFromJar("/" + System.mapLibraryName(libraryName));
                 LOG.info(libraryName + " shared library loaded via loadLibraryFromJar");
             }
             catch (IOException exx) {
                 LOG.info("ERROR while trying to load shared library " + libraryName, exx);
                 throw new RuntimeException(exx);
             }
         }
    }

    /**
     * NOTE: this call is invoked via the construction of ShmShuffleManager in Spark, via SparkEnv's
     * create method. The first call is in the singl-threaded setting, and thus we want all of the singleton
     * objects in C++ shuffle engine, including map-shuffle store manager, reduce-shuffle store manager, and 
     * bytebuffer pools, and others, to be instantiated in this initialization call. In fact, all of these objects
     * are encapsulated in ShuffleStoreManager at this time. So construct the singleton object of ShuffleStoreManager,
     * we actually constructing all of these objects already.
     * @param executorId: the executor process where the shuffle store manager lives. 
     * @param globalHapeName: the global heap name for the Retail-Memory-Broker.
     * @param maximumNumberOfThreads: the maximum number of current threads in one single thread executor.  
     * 
     * @return the ShuffleStoreManager object itself. 
     */
    public synchronized ShuffleStoreManager initialize(String globalHeapName, int maximumNumberOfThreads, 
    		int executorId) {
    	this.globalHeapName = globalHeapName;
    	this.maximumNumberOfTaskThreads = maximumNumberOfThreads;
    	this.executorId = executorId; 
    	
        if (!initialized) {
            LOG.info("perform actual initialization for ShuffleStoreManager");

            this.shuffleStoreTracker = new ShuffleStoreTracker(maximumNumberOfThreads);
            this.shuffleResourceTracker = new ShuffleResourceTracker(); 
            this.pointerToShuffleManager = ninitialize(globalHeapName,  executorId);
            initialized = true;
        }

        return this;
    }

    public long getPointer() {
        return this.pointerToShuffleManager;
    }
    
    public ShuffleStoreTracker getShuffleStoreTracker() {
    	return this.shuffleStoreTracker;
    }
    
    public ShuffleResourceTracker getShuffleResourceTracker() {
    	return this.shuffleResourceTracker; 
    }
    
    /**
     * initialize the store manager, and get back the pointer of the manager.
     * @return
     */
    private native long ninitialize(String globalHeapName, int executorId);

    /**
     * TODO: who is going to call this method? likely the handle that deals with shutting down
     * the worker.
     * to shutdown per-executor shuffle store
     * @return
     */
    public synchronized void shutdown() {
	if (initialized) {
           if (!shutdown) {
              shutdown(this.pointerToShuffleManager);
            
              this.shuffleResourceTracker.shutdown();
              this.shuffleStoreTracker.shutdown();

              shutdown = true;

              LOG.info("ShuffleStoreManager shutdown....");
           }
	}
        //so that shuffle manager can re-start again if necessary (for testing purpose)
        initialized = false;
    }

    private native void shutdown(long ptrToShuffleManager);

    /**
     * NOTE: this method is to be called by the Shuffle Manager's unregisterHandle
     * to release resources occupied on this executor for a particular shuffle stage, with all of 
     * the map tasks launched in this stage.
     * 
     * only the map shuffle stores will need to be cleaned up for NVM resource management.
     * 
     * @param shuffleId shuffle id
     * @param number of the maps, with map id from 0 to numMaps-1.
     */
    public void cleanup(int shuffleId) {
    	ArrayList<MapSHMShuffleStore> retrievedStores = this.shuffleStoreTracker.getMapShuffleStores(shuffleId);
    	if (retrievedStores != null) {
    	   for(MapSHMShuffleStore store: retrievedStores) {
    		 store.shutdown();
    	   }
    	   //remove the entry for shuffle id
    	   this.shuffleStoreTracker.removeMapShuffelStores(shuffleId);
    	}
    }

    private native void ncleanup (long ptrToShuffleManager, int shuffleId, int numMaps);
    
    /**
     * to issue formating of the acquired shared-memory region for the process that the shuffle store manager 
     * is running on.
     * 
     */
    public void formatshm () {
        if (initialized && (!shutdown)) {
           LOG.info("to format shm region....");
           nformatshm(this.pointerToShuffleManager);
        }
        else {
	   LOG.warn("to format shm region with ShuffleStoreManager not initialized or already shutdown....");
        }

    }
    
    private native void nformatshm (long ptrToShuffleManager); 
    
    
    /**
     * for testing purpose, to create a new heap instance, every time a new test case is launched.
     */
    public void registershm () {
    	nregistershm(this.pointerToShuffleManager);
    }
    
    private native void nregistershm (long ptrToShuffleManager);
    
    /**
     * byteBuffer is used only at the Java side for data serialization, and then use the same buffer to pass to
     * C++ shuffle engine. 
     */
    public MapSHMShuffleStore createMapShuffleStore(Kryo kryo, ByteBuffer byteBuffer, int logicalThreadId,
                                                    int shuffleId, int mapId, int numberOfPartitions,
                                                    ShuffleDataModel.KValueTypeId keyType,
                                                    int sizeOfBatchSerialization,
                                                    boolean ordering) {
        MapSHMShuffleStore mapShuffleStore= new MapSHMShuffleStore(kryo, byteBuffer, this);
        mapShuffleStore.initialize(shuffleId, mapId, numberOfPartitions, keyType, sizeOfBatchSerialization, ordering);
        //Note: only map store needs to be tracked for NVM related ersource management
        this.shuffleStoreTracker.addMapShuffleStore(shuffleId, logicalThreadId, mapShuffleStore); 
        return mapShuffleStore;
    }


    /**
     *byteBuffer is used only at the Java side for data de-desrialization, the same buffer is passed to C++ shuffle
     *engine side to hold the data that is to be de-serialized at the Java side. 
     *
     */
    public ReduceSHMShuffleStore createReduceShuffleStore(Kryo kryo, ByteBuffer byteBuffer,
                                                    int shuffleId, int reduceId, int numberOfPartitions,
                                                    boolean ordering, boolean aggregation) {
        ReduceSHMShuffleStore reduceShuffleStore= new ReduceSHMShuffleStore(kryo, byteBuffer, this);
        reduceShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions, ordering, aggregation);
        //NOTE: reduce shuffle store does not need to be tracked. 
        return reduceShuffleStore;
    }
    
    public int getlogicalThreadCounter () {
    	int tid = this.logicalThreadCounter.getAndIncrement();
    	return tid;
    	
    }
}
