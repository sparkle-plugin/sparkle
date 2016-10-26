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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Assert;
import org.junit.Ignore; 
import org.junit.Test; 
import junit.framework.TestCase;
import org.junit.runner.Result;

import java.nio.ByteBuffer;
import com.esotericsoftware.kryo.Kryo;
import com.hp.hpl.firesteel.shuffle.ShuffleDataModel.KValueTypeId;

import java.util.List;
import java.util.ArrayList; 

public class SortBasedMapSHMShuffleStoreWithIntKeysTest extends TestCase {

	 private static final Logger LOG = 
			          LoggerFactory.getLogger(SortBasedMapSHMShuffleStoreWithIntKeysTest.class.getName());
	 private static final int SIZE_OF_BATCH_SERIALIZATION = 100; 
	 
	 //the global heap name created via RMB. 
	 private static final String GLOBAL_HEAP_NAME = "/dev/shm/nvm/global0";
	 
	 public static class ApplicationTestClass {
	      private  int  pagerank;
	      private String pageurl;
	      private  int avgDuration;
	      
	      public ApplicationTestClass() {
	         pagerank = 0;
	         pageurl = null;
	         avgDuration = 0; 
	      }
	      
	      public ApplicationTestClass( int pr, String pu, int avg) {
	    	  this.pagerank = pr;
	    	  this.pageurl = pu;
	    	  this.avgDuration = avg; 
	      }
	  
	      
	      public int getPageRank() {
	    	  return this.pagerank;
	      }
	      
	      public String getPageUrl(){ 
	    	  return this.pageurl;
	      }
	      
	      public int getAvgDuration() {
	    	  return this.avgDuration;
	      }
	      
	      @Override 
              public boolean equals(Object other) {
	    	    boolean result = false;
	    	    if (other instanceof ApplicationTestClass) {
	    	        ApplicationTestClass that = (ApplicationTestClass) other;
	    	        result = (this.getPageRank() == that.getPageRank() && this.getPageUrl().equals (that.getPageUrl())
	    	        		            && this.getAvgDuration() == that.getAvgDuration());
	    	    }
	    	    return result;
	     }
	      
	 }
	 
	 @Override
	 protected void setUp() throws Exception{ 
		  
		 super.setUp();
		 
	 }
	 
	 
	 /**
	  *to test Shuffle Store Manager's load library, init and then shutdown for Int Keys based store.
	  */
	 @Test
	 public void testShutdownMapShuffleStoreManagerWithIntKeys() {
		 
		 LOG.info("this is the test for shutdownMapShuffleStoreManagerWithIntKeysTest");
		 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(
				 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
		 //to get a new heap instance for each new test case launched.
		 ShuffleStoreManager.INSTANCE.registershm();
		 
		 long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
		 LOG.info("native pointer of shuffle store manager retrieved is:"
		                + "0x"+ Long.toHexString(nativePointer));
		 
		 //then start the map shuffele store manager 
		 Kryo kryo=new Kryo();
		 kryo.register(ApplicationTestClass.class);
		 
		 //create a direct bytebuffer:
		 int bufferSize = 1*1024*1024; // 1M bytes
		 ByteBuffer byteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 int shuffleId = 0;
		 int mapId = 1; 
		 int numberOfPartitions = 100;
		 
		 KValueTypeId keyType = KValueTypeId.Int; 
		 
		 int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();
		 boolean ordering= true;
		 MapSHMShuffleStore mapSHMShuffleStore = 
				  ShuffleStoreManager.INSTANCE.createMapShuffleStore(kryo, byteBuffer, logicalThreadId,
				  shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
		 
		 mapSHMShuffleStore.stop();
		 
		 mapSHMShuffleStore.shutdown();
		 
		 ShuffleStoreManager.INSTANCE.shutdown();
	 }
	 
	 
	 /**
	  *to test Shuffle Store Manager's load library, init and then shutdown for String Keys based store.
	  */
	 @Test
	 public void testStartShutdownMapShuffleStoreManagerWithStringKeys() {
		 
		 LOG.info("this is the test for startShutdownMapShuffleStoreManagerWithStringKeysTest");
		 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(
				 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
		 //to get a new heap instance for each new test case launched.
		 ShuffleStoreManager.INSTANCE.registershm();
		 
		 long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
		 LOG.info("native pointer of shuffle store manager retrieved is:"
		                + "0x"+ Long.toHexString(nativePointer));
		 
		 //then start the map shuffele store manager 
		 Kryo kryo=new Kryo();
		 kryo.register(ApplicationTestClass.class);
		 
		 //create a direct bytebuffer:
		 int bufferSize = 1*1024*1024; // 1M bytes
		 ByteBuffer byteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 int shuffleId = 0;
		 int mapId = 1; 
		 int numberOfPartitions = 100;
		 
		 KValueTypeId keyType = KValueTypeId.String; 
		 

		 int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();
		 boolean ordering=true;
		 MapSHMShuffleStore mapSHMShuffleStore = 
				  ShuffleStoreManager.INSTANCE.createMapShuffleStore(kryo, byteBuffer, logicalThreadId,
				  shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
		 
		 mapSHMShuffleStore.stop();
		 
		 mapSHMShuffleStore.shutdown();
		 
		 ShuffleStoreManager.INSTANCE.shutdown();
	 }
	 

	 @Test
	 public void testShutdownMapShuffleStoreManagerSerializerAndStoreWithIntKeysTestWithoutClassRegistration() {
		 LOG.info("this is the test for shutdownMapShuffleStoreManagerSerializerAndStoreWithIntKeysTestWithoutClassRegistrationTest");
		 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(
				 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
		 //to get a new heap instance for each new test case launched.
		 ShuffleStoreManager.INSTANCE.registershm();
		 
		 long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
		 LOG.info("native pointer of shuffle store manager retrieved is:"
		                + "0x"+ Long.toHexString(nativePointer));
		 
		 //then start the map shuffele store manager 
		 Kryo kryo=new Kryo();
		 //create a direct bytebuffer:
		 int bufferSize = 1*1024*1024; // 1M bytes
		 ByteBuffer byteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 int shuffleId = 0;
		 int mapId = 1; 
		 int numberOfPartitions = 100;
		 
		 KValueTypeId keyType = KValueTypeId.Int; 
		 

		 int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter(); 
		 boolean ordering=true;
		 MapSHMShuffleStore mapSHMShuffleStore = 
				  ShuffleStoreManager.INSTANCE.createMapShuffleStore(kryo, byteBuffer, logicalThreadId, 
				  shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
		 
		 
		 int numberOfVs = 10; 
		 ArrayList<Object> testObjects = new ArrayList<Object> ();
		 ArrayList<Integer> partitions = new ArrayList<Integer> ();
		 ArrayList<Integer> kvalues = new ArrayList<Integer> ();
 
		 for (int i=0; i<numberOfVs; i++) {
			  ApplicationTestClass obj = new ApplicationTestClass (i, "hello" +i,  i+1); 
			  testObjects.add(obj);
 
			  partitions.add(i%2);
			  kvalues.add(i);
		 }
		 
		 for (int i=0; i<numberOfVs; i++) {
		     mapSHMShuffleStore.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
							i, KValueTypeId.Int.state);
		 }
		 
		 // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
                 //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
		 int numberOfPairs = numberOfVs; 
		 mapSHMShuffleStore.storeKVPairs(numberOfPairs, KValueTypeId.Int.state);
		
		 ShuffleDataModel.MapStatus mapStatusResult = mapSHMShuffleStore.sortAndStore();
		 
		 LOG.info("map status region id: " + mapStatusResult.getRegionIdOfIndexBucket());
		 LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult.getOffsetOfIndexBucket()));
		 long[] buckets = mapStatusResult.getMapStatus();
		 
		 if (buckets != null) {
			 for (int i=0; i<buckets.length; i++) {
				 LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
			 }
		 }
		 else {
			 LOG.info("map status buckets length is null.");
		 }
		 
		 
		 mapSHMShuffleStore.stop();
		 
		 mapSHMShuffleStore.shutdown();
		 
		 ShuffleStoreManager.INSTANCE.shutdown();
	 }
	 
	 @Test
	 public void testShutdownMapShuffleStoreManagerSerializerAndStoreWithIntKeysWithClassRegistration() {
		 LOG.info("this is the test for shutdownMapShuffleStoreManagerSerializerAndStoreWithIntKeysWithClassRegistrationTest");
		 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(
				 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
		 //to get a new heap instance for each new test case launched.
		 ShuffleStoreManager.INSTANCE.registershm();
		 
		 long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
		 LOG.info("native pointer of shuffle store manager retrieved is:"
		                + "0x"+ Long.toHexString(nativePointer));
		 
		 //then start the map shuffele store manager 
		 Kryo kryo=new Kryo();
		 kryo.register(ApplicationTestClass.class);
		 
		 //create a direct bytebuffer:
		 int bufferSize = 1*1024*1024; // 1M bytes
		 ByteBuffer byteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 int shuffleId = 0;
		 int mapId = 1; 
		 int numberOfPartitions = 100;
		 
		 KValueTypeId keyType = KValueTypeId.Int; 
		 
		 int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();
		 boolean ordering=true;
		 MapSHMShuffleStore mapSHMShuffleStore = 
				  ShuffleStoreManager.INSTANCE.createMapShuffleStore(kryo, byteBuffer, logicalThreadId,
				  shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
		 
		 
		 int numberOfVs = 10; 
		 ArrayList<Object> testObjects = new ArrayList<Object> ();
		 ArrayList<Integer> partitions = new ArrayList<Integer> ();
		 ArrayList<Integer> kvalues = new ArrayList<Integer> ();
 		 
		 for (int i=0; i<numberOfVs; i++) {
			  ApplicationTestClass obj = new ApplicationTestClass (i, "hello" +i,  i+1); 
			  testObjects.add(obj);
			  partitions.add(i%2);
			  kvalues.add(i);
		 }
		 

		 for (int i=0; i<numberOfVs; i++) {
		     mapSHMShuffleStore.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
							i, KValueTypeId.Int.state);
		 }
		 LOG.info("finish serialize the values");
		 
		 // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
                 //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
		 int numberOfPairs = numberOfVs; 
		 mapSHMShuffleStore.storeKVPairs(numberOfPairs, KValueTypeId.Int.state);
		 LOG.info("finish store the kv paris with int keys");
		
		 ShuffleDataModel.MapStatus mapStatusResult = mapSHMShuffleStore.sortAndStore();
		 LOG.info("finish sort and store the kv paris with int keys");
		 
		 LOG.info("map status region id: " + mapStatusResult.getRegionIdOfIndexBucket());
		 LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult.getOffsetOfIndexBucket()));
		 long[] buckets = mapStatusResult.getMapStatus();
		 
		 if (buckets != null) {
			 for (int i=0; i<buckets.length; i++) {
				 LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
			 }
		 }
		 else {
			 LOG.info("map status buckets length is null.");
		 }
		 
		 
		 mapSHMShuffleStore.stop();
		 
		 mapSHMShuffleStore.shutdown();
		 
		 ShuffleStoreManager.INSTANCE.shutdown();
	 }
	 
	 @Test
	 public void testStartShutdownMapShuffleStoreManagerSerializerAndStoreWithIntKeysWithRepeatedInvocation() {
		 LOG.info("this is the test for startShutdownMapShuffleStoreManagerSerializerAndStoreWithIntKeysWithRepeatedInvocationTest");
		 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(
				 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
		 //to get a new heap instance for each new test case launched.
		 ShuffleStoreManager.INSTANCE.registershm();
		 
		 long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
		 LOG.info("native pointer of shuffle store manager retrieved is:"
		                + "0x"+ Long.toHexString(nativePointer));
		 
		 //then start the map shuffele store manager 
		 Kryo kryo=new Kryo();
		 kryo.register(ApplicationTestClass.class);
		 
		 //create a direct bytebuffer:
		 int bufferSize = 1*1024*1024; // 1M bytes
		 ByteBuffer byteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 int shuffleId = 0;
		 int mapId = 1; 
		 int numberOfPartitions = 100;
		 
		 KValueTypeId keyType = KValueTypeId.Int; 
		 
		 
		 int repeatedNumber = 2; 
		 for (int k = 0; k<repeatedNumber; k++) {
	
			 LOG.info ("**************repeat number*********************" + k); 
			 
			 int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();
			 boolean ordering=true;
			 MapSHMShuffleStore mapSHMShuffleStore = 
					  ShuffleStoreManager.INSTANCE.createMapShuffleStore(kryo, byteBuffer, logicalThreadId,
					  shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
			 
			 int numberOfVs = 10; 
			 ArrayList<Object> testObjects = new ArrayList<Object> ();
			 ArrayList<Integer> partitions = new ArrayList<Integer> ();
			 ArrayList<Integer> kvalues = new ArrayList<Integer> ();
	 
			 for (int i=0; i<numberOfVs; i++) {
				  ApplicationTestClass obj = new ApplicationTestClass (i, "hello" +i,  i+1); 
				  testObjects.add(obj);
				  partitions.add(i%2);
				  kvalues.add(i);
			 }
			 
			 for (int i=0; i<numberOfVs; i++) {
			     mapSHMShuffleStore.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
								i, KValueTypeId.Int.state);
			 }
			  
			 // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
	                 //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
			 int numberOfPairs = numberOfVs; 
			 mapSHMShuffleStore.storeKVPairs(numberOfPairs, KValueTypeId.Int.state);
			
			 ShuffleDataModel.MapStatus mapStatusResult = mapSHMShuffleStore.sortAndStore();
			 
			 LOG.info("map status region id: " + mapStatusResult.getRegionIdOfIndexBucket());
			 LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult.getOffsetOfIndexBucket()));
			 long[] buckets = mapStatusResult.getMapStatus();
			 
			 if (buckets != null) {
				 for (int i=0; i<buckets.length; i++) {
					 LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
				 }
			 }
			 else {
				 LOG.info("map status buckets length is null.");
			 }
		 
			 mapSHMShuffleStore.stop();
			 
			 mapSHMShuffleStore.shutdown();
		 }
	 
		 ShuffleStoreManager.INSTANCE.shutdown();
	 }
	 
	 @Override
	 protected void tearDown() throws Exception{ 
		 //do something first
		 LOG.info("shm region:" + GLOBAL_HEAP_NAME + " to be formated");
		 ShuffleStoreManager.INSTANCE.formatshm(); 
		 
		 super.tearDown();
	 }
	 
	 public static void main(String[] args) {
		  //NOTE: for some reason the annotation does not work for @Test and @Ignore. Instead, the only
		  //thing work is the method name started with "test" to be the test methods. 
	      junit.textui.TestRunner.run(SortBasedMapSHMShuffleStoreWithIntKeysTest.class);
	 }
}



