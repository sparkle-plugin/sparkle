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

package com.hp.hpl.firesteel.offheapstore;


import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.firesteel.shuffle.ShuffleStoreManager;


/**
 * test for access of unsafe memory 
 */
public class OffHeapStore {

    private static final Logger LOG = LoggerFactory.getLogger(OffHeapStore.class.getName());

    //this is a single object.
    public final static OffHeapStore INSTANCE = new OffHeapStore();
	private long ptrOffHeapStoreMgr=0;
	private String globalHeapName;
	private int executorId;
	private boolean initialized =false;
	private boolean shutdown =false;

    private OffHeapStore () {
        //only one library will be loaded.
        String libraryName = "jnishmoffheapstore";
        initNativeLibrary(libraryName);
    }

    /**
     * to load native shared libraries
     */
    private void initNativeLibrary(String libraryName) {
         try {
             System.loadLibrary(System.mapLibraryName(libraryName));
             LOG.info(libraryName + " shared library loaded via mapLibraryName");
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


    public boolean isInitialized() {
        return initialized;
    } 

    public synchronized OffHeapStore initialize(String globalHeapName, int executorId) {

	this.globalHeapName = globalHeapName;
	this.executorId = executorId;

		
        if (!initialized) {
            this.ptrOffHeapStoreMgr = ninitialize(globalHeapName,  executorId);
            initialized = true;
        }

        return this;

    }

    public long getPointerToOffHeapStoreManager() {
	return this.ptrOffHeapStoreMgr;
    }	

    private native long ninitialize (String globalHeapName, int executorId);


    public synchronized void shutdown() {
        if (!shutdown) {
            shutdown(this.ptrOffHeapStoreMgr);
            shutdown = true;
        }
    }

    private native void shutdown(long ptrOffHeapStoreMgr);

	/**
      * To create an attribute partition 
      * @param addr the local memory address where the unsafe partition (e.g., key value table, array, etc) is stored
      * @param size the memory size for the partition
      * @param shmAddr the shared memory address for the partition returned by OffHeapStoreManager
      */
    public void createAttributePartition (long addr, int size, ShmAddress shmAddr) {
		createAttributePartition(this.ptrOffHeapStoreMgr, addr, size, shmAddr);	
	}
    private native void createAttributePartition (long ptrOffHeapStoreMgr, long addr, int size, ShmAddress shmAddr);

	/**
      * To create an attribute partition and hash partition for the partition 
      * @param addr the local memory address where the unsafe partition (which should contain keys and values) is stored
      * @param size the memory size for the partition
      * @param valuesize the memory size for each value (Note: the key type is fixed as long)
      * @param shmAddr the shared memory addresses for partition and hash partition returned by OffHeapStoreManager
      */
    public void createAttributeHashPartition (long addr, int size, int valuesize, ShmAddress shmAddr) {

        if (!initialized) {
            this.ptrOffHeapStoreMgr = ninitialize(globalHeapName,  executorId);
            initialized = true;
        }
       
		createAttributeHashPartition(this.ptrOffHeapStoreMgr, addr, size, valuesize, shmAddr);	
		LOG.debug("createAttributeHashPartition JAVA:" + shmAddr.regionId + "," + shmAddr.offset_attrTable + "," + shmAddr.offset_hash1stTable + "," + shmAddr.offset_hash2ndTable);
		
	}
    private native void createAttributeHashPartition (long ptrOffHeapStoreMgr, long addr, int size, int valuesize, ShmAddress shmAddr); 

	/**
      * To get the local address for the shared memory address for the partition
      * @param shmAddr the shared memory address for the partition
      * @return the local address for the partition
      */
    public long getAttributePartition (ShmAddress shmAddr) {
		return getAttributePartition(this.ptrOffHeapStoreMgr, shmAddr);
	}
    private native long getAttributePartition (long ptrOffHeapStoreMgr, ShmAddress shmAddr);

	/**
      * To get the offset for the value for a given key in an attribute hash partition
      * @param shmAddr the shared memory address for the partition and hash partition
      * @param key key to be found 
      * @return the offset for the value for a given key
      */
    public long getAttributeOffsetInHashPartition (ShmAddress shmAddr, long key) {
		return getAttributeOffsetInHashPartition(this.ptrOffHeapStoreMgr, shmAddr, key);
	}
    private native long getAttributeOffsetInHashPartition (long ptrOffHeapStoreMgr, ShmAddress shmAddr, long key);

}

