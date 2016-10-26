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

public class  SortBasedReduceSHMShuffleStoreWithIntKeysTest extends TestCase {

	 private static final Logger LOG =
			 LoggerFactory.getLogger(SortBasedReduceSHMShuffleStoreWithIntKeysTest.class.getName());
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
	  * NOTE: this test also include the serialization and de-serialization of value class. With class registration. 
	  */
	 @Test
	 public void testStatusFromMapShuffleStoreWithIntKeysWithReduceId0WithClassRegistrationTest() {
		 
		 LOG.info("this is the test for statusFromMapShuffleStoreWithIntKeysWithReduceId0WithClassRegistrationTest");
		 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(
				 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
		 //to get a new heap instance for each new test case launched.
		 ShuffleStoreManager.INSTANCE.registershm();
		 
		 long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
		 LOG.info("native pointer of shuffle store manager retrieved is:"
		                + "0x"+ Long.toHexString(nativePointer));
		 
		 //then start the map shuffele store manager 
		 Kryo mapSideKryo=new Kryo();
		 mapSideKryo.register(ApplicationTestClass.class);
		 
		 //create a direct bytebuffer:
		 int bufferSize = 1*1024*1024; // 1M bytes
		 ByteBuffer mapSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 int shuffleId = 0;
		 int mapId = 1; 
		 int numberOfPartitions = 100;
		 
		 KValueTypeId keyType = KValueTypeId.Int; 
		 

		 boolean ordering=true;
		 int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();
		 MapSHMShuffleStore mapSHMShuffleStore = 
				  ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer, 
				  logicalThreadId,
				  shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
		 
		 Kryo reduceSideKryo=new Kryo();
		 reduceSideKryo.register(ApplicationTestClass.class);
		 ByteBuffer reduceSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 //choose reduce that is not 0, or 1, or 7 (arbitrary number that is smaller than number of partitions)
		 int reduceId = 0; 
		 ReduceSHMShuffleStore reduceSHMShuffleStore = 
				 ShuffleStoreManager.INSTANCE.createReduceShuffleStore(reduceSideKryo, reduceSideByteBuffer,
                 shuffleId, reduceId, numberOfPartitions, ordering, true); 
		 
		 ShuffleDataModel.MapStatus mapStatusResult = null; 
		 {
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
			
			 mapStatusResult = mapSHMShuffleStore.sortAndStore();
			 
			 LOG.info("map status region id: " + mapStatusResult.getRegionIdOfIndexBucket());
			 LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult.getOffsetOfIndexBucket()));
			 long[] buckets = mapStatusResult.getMapStatus();
			 
			 if (buckets != null) {
				 for (int i=0; i<buckets.length; i++) {
					 if (buckets[i] > 0) {
					    LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
					 }
				 }
			 }
			 else {
				 LOG.info("map status buckets length is null.");
			 }
			 
		 
		 }
		 
		 {
			 reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions, ordering, true); 
			 int mapIds[] = new int[1]; 
			 mapIds[0] = mapId;
			 long shmRegionIds[]= new long [1];
			 shmRegionIds[0] = mapStatusResult.getRegionIdOfIndexBucket();
			 long offsetToIndexChunks[] = new long[1];
			 offsetToIndexChunks[0] = mapStatusResult.getOffsetOfIndexBucket();
			 long sizes[] = new long[1];
			 sizes[0] = mapStatusResult.getMapStatus()[reduceId]; //pick the first bucket; 
			 
			 ShuffleDataModel.ReduceStatus statuses =
					 new ShuffleDataModel.ReduceStatus(mapIds, shmRegionIds, offsetToIndexChunks, sizes);
			 //NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet. 
			 reduceSHMShuffleStore.mergeSort(statuses);
			 
		 }
		 
		 //to actually pull the data out 
		 {
			 int knumbers = 6; 
			 ArrayList<Integer> kvalues = new ArrayList<Integer>();
			 ArrayList<ArrayList<Object>> vvalues = new ArrayList<ArrayList<Object>> (); 
			 for (int i=0; i<knumbers; i++) {
				 kvalues.add(0); //initialization to 0; 
				 vvalues.add(null); //initialization to null;
			 }
			 
			 int actualRetrievedKNumbers= reduceSHMShuffleStore.getKVPairsWithIntKeys (kvalues, vvalues, knumbers);
			 
			 LOG.info("actual number of the keys retrieved is: " + actualRetrievedKNumbers);
			 
			 for (int i=0; i<actualRetrievedKNumbers; i++) {
				 LOG.info("retrieved k value: " + kvalues.get(i));
				 ArrayList<Object> tvvalues = vvalues.get(i);
				 for (int m=0; m<tvvalues.size(); m++) {
					 Object x = tvvalues.get(m);
					 Assert.assertTrue(x instanceof ApplicationTestClass); 
					 if (x instanceof ApplicationTestClass) {
						 
					    ApplicationTestClass y = (ApplicationTestClass) x; 
					    LOG.info("**" + " object: " + " page rank:" + y.getPageRank() 
					    		      + " url: " + y.getPageUrl() 
					    		      + " avg duration: " + y.getAvgDuration());
					    
					    //based on how I constructed the test data  
					    Assert.assertEquals(y.getPageRank(), kvalues.get(i).intValue());
					    Assert.assertEquals(y.getPageUrl(), "hello"+ kvalues.get(i).intValue());
					    Assert.assertEquals(y.getAvgDuration(), kvalues.get(i).intValue() + 1);
 					 }
					 
					  
				 }
			 }
			 
			 Assert.assertEquals(actualRetrievedKNumbers, 5);
			 {
				int actuals[] = new int[actualRetrievedKNumbers];
				for (int i=0; i<actualRetrievedKNumbers; i++) {
					actuals[i] = kvalues.get(i); 
				}
				
				int expecteds[] = {0, 2, 4, 6, 8};
			    
			    Assert.assertArrayEquals(expecteds, actuals);
			 }
			 
		 }
		 
		 reduceSHMShuffleStore.stop();
		 LOG.info("reduce shuffle store stopped");
		 
		 reduceSHMShuffleStore.shutdown();
		 LOG.info("reduce shuffle store shutdown");
		 
		 mapSHMShuffleStore.stop();
		 LOG.info("map shuffle store stopped");
		 
		 mapSHMShuffleStore.shutdown();
		 LOG.info("map shuffle store shutdown");
		 
		 ShuffleStoreManager.INSTANCE.shutdown();
		 LOG.info("shuffle store manager shutdown");
	 }


	 /**
          * sort with no aggregation
	  * NOTE: this test also include the serialization and de-serialization of value class. With class registration. 
	  */
	 @Test
	 public void testStatusFromMapShuffleStoreWithIntKeysWithReduceId0WithClassRegistrationTestWithNoAggregation() {
		 
		 LOG.info("this is the test for statusFromMapShuffleStoreWithIntKeysWithReduceId0WithClassRegistrationTestWithNoAggregation");
		 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(
				 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
		 //to get a new heap instance for each new test case launched.
		 ShuffleStoreManager.INSTANCE.registershm();
		 
		 long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
		 LOG.info("native pointer of shuffle store manager retrieved is:"
		                + "0x"+ Long.toHexString(nativePointer));
		 
		 //then start the map shuffele store manager 
		 Kryo mapSideKryo=new Kryo();
		 mapSideKryo.register(ApplicationTestClass.class);
		 
		 //create a direct bytebuffer:
		 int bufferSize = 1*1024*1024; // 1M bytes
		 ByteBuffer mapSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 int shuffleId = 0;
		 int mapId = 1; 
		 int numberOfPartitions = 100;
		 
		 KValueTypeId keyType = KValueTypeId.Int; 
		 

		 boolean ordering=true;
		 int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();
		 MapSHMShuffleStore mapSHMShuffleStore = 
				  ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer, 
				  logicalThreadId,
				  shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
		 
		 Kryo reduceSideKryo=new Kryo();
		 reduceSideKryo.register(ApplicationTestClass.class);
		 ByteBuffer reduceSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 //choose reduce that is not 0, or 1, or 7 (arbitrary number that is smaller than number of partitions)
		 int reduceId = 0; 
                 boolean aggregation = false; 
		 ReduceSHMShuffleStore reduceSHMShuffleStore = 
				 ShuffleStoreManager.INSTANCE.createReduceShuffleStore(reduceSideKryo, reduceSideByteBuffer,
                 shuffleId, reduceId, numberOfPartitions, ordering, aggregation); 
		 
		 ShuffleDataModel.MapStatus mapStatusResult = null; 
		 {
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
			
			 mapStatusResult = mapSHMShuffleStore.sortAndStore();
			 
			 LOG.info("map status region id: " + mapStatusResult.getRegionIdOfIndexBucket());
			 LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult.getOffsetOfIndexBucket()));
			 long[] buckets = mapStatusResult.getMapStatus();
			 
			 if (buckets != null) {
				 for (int i=0; i<buckets.length; i++) {
					 if (buckets[i] > 0) {
					    LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
					 }
				 }
			 }
			 else {
				 LOG.info("map status buckets length is null.");
			 }
			 
		 
		 }
		 
		 {
			 reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions, ordering, aggregation); 
			 int mapIds[] = new int[1]; 
			 mapIds[0] = mapId;
			 long shmRegionIds[]= new long [1];
			 shmRegionIds[0] = mapStatusResult.getRegionIdOfIndexBucket();
			 long offsetToIndexChunks[] = new long[1];
			 offsetToIndexChunks[0] = mapStatusResult.getOffsetOfIndexBucket();
			 long sizes[] = new long[1];
			 sizes[0] = mapStatusResult.getMapStatus()[reduceId]; //pick the first bucket; 
			 
			 ShuffleDataModel.ReduceStatus statuses =
					 new ShuffleDataModel.ReduceStatus(mapIds, shmRegionIds, offsetToIndexChunks, sizes);
			 //NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet. 
			 reduceSHMShuffleStore.mergeSort(statuses);
			 
		 }
		 
		 //to actually pull the data out 
		 {
			 int knumbers = 6; 
			 ArrayList<Integer> kvalues = new ArrayList<Integer>();
			 ArrayList<Object> vvalues = new ArrayList<Object> (); 
			 for (int i=0; i<knumbers; i++) {
				 kvalues.add(0); //initialization to 0; 
				 vvalues.add(null); //initialization to null;
			 }
			 
			 int actualRetrievedKNumbers= reduceSHMShuffleStore.getSimpleKVPairsWithIntKeys (kvalues, vvalues, knumbers);
			 
			 LOG.info("actual number of the keys retrieved is: " + actualRetrievedKNumbers);
			 
			 for (int i=0; i<actualRetrievedKNumbers; i++) {
				 LOG.info("retrieved k value: " + kvalues.get(i));
				 Object tvvalue = vvalues.get(i);
				 {
				         Object x = tvvalue;
					 Assert.assertTrue(x instanceof ApplicationTestClass); 
					 if (x instanceof ApplicationTestClass) {
						 
					    ApplicationTestClass y = (ApplicationTestClass) x; 
					    LOG.info("**" + " object: " + " page rank:" + y.getPageRank() 
					    		      + " url: " + y.getPageUrl() 
					    		      + " avg duration: " + y.getAvgDuration());
					    
					    //based on how I constructed the test data  
					    Assert.assertEquals(y.getPageRank(), kvalues.get(i).intValue());
					    Assert.assertEquals(y.getPageUrl(), "hello"+ kvalues.get(i).intValue());
					    Assert.assertEquals(y.getAvgDuration(), kvalues.get(i).intValue() + 1);
 					 }
					 
					  
				 }
			 }
			 
			 Assert.assertEquals(actualRetrievedKNumbers, 5);
			 {
				int actuals[] = new int[actualRetrievedKNumbers];
				for (int i=0; i<actualRetrievedKNumbers; i++) {
					actuals[i] = kvalues.get(i); 
				}
				
				int expecteds[] = {0, 2, 4, 6, 8};
			    
			    Assert.assertArrayEquals(expecteds, actuals);
			 }
			 
		 }
		 
		 reduceSHMShuffleStore.stop();
		 LOG.info("reduce shuffle store stopped");
		 
		 reduceSHMShuffleStore.shutdown();
		 LOG.info("reduce shuffle store shutdown");
		 
		 mapSHMShuffleStore.stop();
		 LOG.info("map shuffle store stopped");
		 
		 mapSHMShuffleStore.shutdown();
		 LOG.info("map shuffle store shutdown");
		 
		 ShuffleStoreManager.INSTANCE.shutdown();
		 LOG.info("shuffle store manager shutdown");
	 }
	 
	 /**
          * sort and aggregation.
          *
	  * NOTE: this test also include the serialization and de-serialization of value class. With class registration. 
	  */
	 @Test
	 public void testStatusFromMapShuffleStoreWithIntKeysWithReduceId0WithoutClassRegistrationTest() {
		 
		 LOG.info("this is the test for statusFromMapShuffleStoreWithIntKeysWithReduceId0WithoutClassRegistrationTest");
		 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(
				 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
		 //to get a new heap instance for each new test case launched.
		 ShuffleStoreManager.INSTANCE.registershm();
		 
		 long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
		 LOG.info("native pointer of shuffle store manager retrieved is:"
		                + "0x"+ Long.toHexString(nativePointer));
		 
		 //then start the map shuffele store manager 
		 Kryo mapSideKryo=new Kryo();
		 
		 //create a direct bytebuffer:
		 int bufferSize = 1*1024*1024; // 1M bytes
		 ByteBuffer mapSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 int shuffleId = 0;
		 int mapId = 1; 
		 int numberOfPartitions = 100;
		 
		 KValueTypeId keyType = KValueTypeId.Int; 
		 
		 int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();
		 boolean ordering=true;
		 MapSHMShuffleStore mapSHMShuffleStore = 
				  ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer, 
				  logicalThreadId,
				  shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
		 
		 Kryo reduceSideKryo=new Kryo();
		 ByteBuffer reduceSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 //choose reduce that is not 0, or 1, or 7 (arbitrary number that is smaller than number of partitions)
		 int reduceId = 0; 
		 ReduceSHMShuffleStore reduceSHMShuffleStore = 
				 ShuffleStoreManager.INSTANCE.createReduceShuffleStore(reduceSideKryo, reduceSideByteBuffer,
                 shuffleId, reduceId, numberOfPartitions, ordering, true); 
		 
		 ShuffleDataModel.MapStatus mapStatusResult = null; 
		 {
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
			
			 mapStatusResult = mapSHMShuffleStore.sortAndStore();
			 
			 LOG.info("map status region id: " + mapStatusResult.getRegionIdOfIndexBucket());
			 LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult.getOffsetOfIndexBucket()));
			 long[] buckets = mapStatusResult.getMapStatus();
			 
			 if (buckets != null) {
				 for (int i=0; i<buckets.length; i++) {
					 if (buckets[i] > 0) {
					    LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
					 }
				 }
			 }
			 else {
				 LOG.info("map status buckets length is null.");
			 }
			 
		 
		 }
		 
		 {
			 reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions, ordering, true); 
			 int mapIds[] = new int[1]; 
			 mapIds[0] = mapId;
			 long shmRegionIds[]= new long [1];
			 shmRegionIds[0] = mapStatusResult.getRegionIdOfIndexBucket();
			 long offsetToIndexChunks[] = new long[1];
			 offsetToIndexChunks[0] = mapStatusResult.getOffsetOfIndexBucket();
			 long sizes[] = new long[1];
			 sizes[0] = mapStatusResult.getMapStatus()[reduceId]; //pick the first bucket; 
			 
			 ShuffleDataModel.ReduceStatus statuses =
					 new ShuffleDataModel.ReduceStatus(mapIds, shmRegionIds, offsetToIndexChunks, sizes);
			 //NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet. 
			 reduceSHMShuffleStore.mergeSort(statuses);
			 
		 }
		 
		 //to actually pull the data out 
		 {
			 int knumbers = 6; 
			 ArrayList<Integer> kvalues = new ArrayList<Integer>();
			 ArrayList<ArrayList<Object>> vvalues = new ArrayList<ArrayList<Object>> (); 
			 for (int i=0; i<knumbers; i++) {
				 kvalues.add(0); //initialization to 0; 
				 vvalues.add(null); //initialization to null;
			 }
			 
			 int actualRetrievedKNumbers= reduceSHMShuffleStore.getKVPairsWithIntKeys (kvalues, vvalues, knumbers);
			 
			 LOG.info("actual number of the keys retrieved is: " + actualRetrievedKNumbers);
			 
			 for (int i=0; i<actualRetrievedKNumbers; i++) {
				 LOG.info("retrieved k value: " + kvalues.get(i));
				 ArrayList<Object> tvvalues = vvalues.get(i);
				 for (int m=0; m<tvvalues.size(); m++) {
					 Object x = tvvalues.get(m);
					 Assert.assertTrue(x instanceof ApplicationTestClass); 
					 if (x instanceof ApplicationTestClass) {
						 
					    ApplicationTestClass y = (ApplicationTestClass) x; 
					    LOG.info("**" + " object: " + " page rank:" + y.getPageRank() 
					    		      + " url: " + y.getPageUrl() 
					    		      + " avg duration: " + y.getAvgDuration());
					    
					    //based on how I constructed the test data  
					    Assert.assertEquals(y.getPageRank(), kvalues.get(i).intValue());
					    Assert.assertEquals(y.getPageUrl(), "hello"+ kvalues.get(i).intValue());
					    Assert.assertEquals(y.getAvgDuration(), kvalues.get(i).intValue() + 1);
 					 }
					 
					  
				 }
			 }
			 
			 Assert.assertEquals(actualRetrievedKNumbers, 5);
			 {
				int actuals[] = new int[actualRetrievedKNumbers];
				for (int i=0; i<actualRetrievedKNumbers; i++) {
					actuals[i] = kvalues.get(i); 
				}
				
				int expecteds[] = {0, 2, 4, 6, 8};
			    
			    Assert.assertArrayEquals(expecteds, actuals);
			 }
			 
		 }
		 
		 
		 reduceSHMShuffleStore.stop();
		 LOG.info("reduce shuffle store stopped");
		 
		 reduceSHMShuffleStore.shutdown();
		 LOG.info("reduce shuffle store shutdown");
		 
		 mapSHMShuffleStore.stop();
		 LOG.info("map shuffle store stopped");
		 
		 mapSHMShuffleStore.shutdown();
		 LOG.info("map shuffle store shutdown");
		 
		 ShuffleStoreManager.INSTANCE.shutdown();
		 LOG.info("shuffle store manager shutdown");
	 }
	 
 
         /**
          * sort, but without aggregation 
          * NOTE: this test also include the serialization and de-serialization of value class. With class registration. 
          */
         @Test
         public void testStatusFromMapShuffleStoreWithIntKeysWithReduceId0WithoutClassRegistrationTestWithNoAggregation() {
	  
	    LOG.info("this is the test for statusFromMapShuffleStoreWithIntKeysWithReduceId0WithoutClassRegistrationTestWithNoAggregation");
	  
	    int executorId = 0; 
	    ShuffleStoreManager.INSTANCE.initialize(
			 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
	    //to get a new heap instance for each new test case launched.
	    ShuffleStoreManager.INSTANCE.registershm();
	  
	    long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
	    LOG.info("native pointer of shuffle store manager retrieved is:"
		  + "0x"+ Long.toHexString(nativePointer));
	  
	    //then start the map shuffele store manager 
	    Kryo mapSideKryo=new Kryo();
	  
	    //create a direct bytebuffer:
	    int bufferSize = 1*1024*1024; // 1M bytes
	    ByteBuffer mapSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
	  
	    int shuffleId = 0;
	    int mapId = 1; 
	    int numberOfPartitions = 100;
	  
	    KValueTypeId keyType = KValueTypeId.Int; 
	  
	    int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();
	    boolean ordering=true;
	    MapSHMShuffleStore mapSHMShuffleStore = 
	      ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer, 
								 logicalThreadId,
								 shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
	   
	    Kryo reduceSideKryo=new Kryo();
	    ByteBuffer reduceSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
	   
	    //choose reduce that is not 0, or 1, or 7 (arbitrary number that is smaller than number of partitions)
	    int reduceId = 0; 
	    boolean aggregation = false;
	    ReduceSHMShuffleStore reduceSHMShuffleStore = 
	       ShuffleStoreManager.INSTANCE.createReduceShuffleStore(reduceSideKryo, reduceSideByteBuffer,
								     shuffleId, reduceId, numberOfPartitions, ordering, aggregation); 
	    
	    ShuffleDataModel.MapStatus mapStatusResult = null; 
	    {
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
	       
	       mapStatusResult = mapSHMShuffleStore.sortAndStore();
	        
	       LOG.info("map status region id: " + mapStatusResult.getRegionIdOfIndexBucket());
	       LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult.getOffsetOfIndexBucket()));
	       long[] buckets = mapStatusResult.getMapStatus();
	        
	       if (buckets != null) {
		   for (int i=0; i<buckets.length; i++) {
		       if (buckets[i] > 0) {
			   LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
		       }
		   }
	       }
	       else {
		   LOG.info("map status buckets length is null.");
	       }
	        
	        
	    }
	    
	    {
	       reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions, ordering, aggregation); 
	       int mapIds[] = new int[1]; 
	       mapIds[0] = mapId;
	       long shmRegionIds[]= new long [1];
	       shmRegionIds[0] = mapStatusResult.getRegionIdOfIndexBucket();
	       long offsetToIndexChunks[] = new long[1];
	       offsetToIndexChunks[0] = mapStatusResult.getOffsetOfIndexBucket();
	       long sizes[] = new long[1];
	       sizes[0] = mapStatusResult.getMapStatus()[reduceId]; //pick the first bucket; 
	        
	        ShuffleDataModel.ReduceStatus statuses =
		    new ShuffleDataModel.ReduceStatus(mapIds, shmRegionIds, offsetToIndexChunks, sizes);
		//NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet. 
		reduceSHMShuffleStore.mergeSort(statuses);
		 
	    }
	    
	    //to actually pull the data out 
	    {
	       int knumbers = 6; 
	       ArrayList<Integer> kvalues = new ArrayList<Integer>();
	       ArrayList<Object> vvalues = new ArrayList<Object> (); 
	       for (int i=0; i<knumbers; i++) {
		   kvalues.add(0); //initialization to 0; 
		   vvalues.add(null); //initialization to null;
	       }
	        
	       int actualRetrievedKNumbers= reduceSHMShuffleStore.getSimpleKVPairsWithIntKeys (kvalues, vvalues, knumbers);
	        
	       LOG.info("actual number of the keys retrieved is: " + actualRetrievedKNumbers);
	        
	       for (int i=0; i<actualRetrievedKNumbers; i++) {
		   LOG.info("retrieved k value: " + kvalues.get(i));
		   Object tvvalue = vvalues.get(i);
		   {
		       Object x = tvvalue;
		       Assert.assertTrue(x instanceof ApplicationTestClass); 
		       if (x instanceof ApplicationTestClass) {
			    
			   ApplicationTestClass y = (ApplicationTestClass) x; 
			   LOG.info("**" + " object: " + " page rank:" + y.getPageRank() 
				    + " url: " + y.getPageUrl() 
				    + " avg duration: " + y.getAvgDuration());
			       
			   //based on how I constructed the test data  
			   Assert.assertEquals(y.getPageRank(), kvalues.get(i).intValue());
			   Assert.assertEquals(y.getPageUrl(), "hello"+ kvalues.get(i).intValue());
			   Assert.assertEquals(y.getAvgDuration(), kvalues.get(i).intValue() + 1);
		       }
		        
		         
		   }
	       }
	        
	       Assert.assertEquals(actualRetrievedKNumbers, 5);
	       {
		   int actuals[] = new int[actualRetrievedKNumbers];
		   for (int i=0; i<actualRetrievedKNumbers; i++) {
		       actuals[i] = kvalues.get(i); 
		   }
		   
		   int expecteds[] = {0, 2, 4, 6, 8};
		       
		   Assert.assertArrayEquals(expecteds, actuals);
	       }
	        
	   }
	    
	    
	   reduceSHMShuffleStore.stop();
	   LOG.info("reduce shuffle store stopped");
	    
	   reduceSHMShuffleStore.shutdown();
	   LOG.info("reduce shuffle store shutdown");
	    
	   mapSHMShuffleStore.stop();
	   LOG.info("map shuffle store stopped");
	    
	   mapSHMShuffleStore.shutdown();
	   LOG.info("map shuffle store shutdown");
	    
	   ShuffleStoreManager.INSTANCE.shutdown();
	   LOG.info("shuffle store manager shutdown");
         }
     
         //sort, with no aggregation 
         @Test
         public void testMapStatusFromMapShuffleStoreWithIntKeysWithReduceId1TestWithNoAggregation() {
	  
	   LOG.info("this is the test for mapStatusFromMapShuffleStoreWithIntKeysWithReduceId1TestWithNoAggregation");
	  
	   int executorId = 0; 
	   ShuffleStoreManager.INSTANCE.initialize(
			 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
	   //to get a new heap instance for each new test case launched.
	   ShuffleStoreManager.INSTANCE.registershm();
	  
	   long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
	   LOG.info("native pointer of shuffle store manager retrieved is:"
		  + "0x"+ Long.toHexString(nativePointer));
	  
	   //then start the map shuffele store manager 
	   Kryo mapSideKryo=new Kryo();
	   mapSideKryo.register(ApplicationTestClass.class);
	  
	   //create a direct bytebuffer:
	   int bufferSize = 1*1024*1024; // 1M bytes
	   ByteBuffer mapSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
	  
	   int shuffleId = 0;
	   int mapId = 1; 
	   int numberOfPartitions = 100;
	  
	   KValueTypeId keyType = KValueTypeId.Int; 
	  
	   int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();  
	   boolean ordering=true;
	   MapSHMShuffleStore mapSHMShuffleStore = 
	      ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer, 
		      logicalThreadId,
		      shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
	   
	   Kryo reduceSideKryo=new Kryo();
	   reduceSideKryo.register(ApplicationTestClass.class);
	   ByteBuffer reduceSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
	   
	   //choose reduce that is not 0, or 1, or 7 (arbitrary number that is smaller than number of partitions)
	   int reduceId = 1; 
	   boolean aggregation = false; 
	   ReduceSHMShuffleStore reduceSHMShuffleStore = 
	       ShuffleStoreManager.INSTANCE.createReduceShuffleStore(reduceSideKryo, reduceSideByteBuffer,
		     shuffleId, reduceId, numberOfPartitions, ordering,  aggregation); 
	    
	   ShuffleDataModel.MapStatus mapStatusResult = null; 
	   {
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
	       mapSHMShuffleStore.storeKVPairs(numberOfPairs,KValueTypeId.Int.state);
	       
	       mapStatusResult = mapSHMShuffleStore.sortAndStore();
	        
	       LOG.info("map status region id: " + mapStatusResult.getRegionIdOfIndexBucket());
	       LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult.getOffsetOfIndexBucket()));
	       long[] buckets = mapStatusResult.getMapStatus();
	        
	       if (buckets != null) {
		   for (int i=0; i<buckets.length; i++) {
		       if (buckets[i] > 0) {
			   LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
		       }
		   }
	       }
	       else {
		   LOG.info("map status buckets length is null.");
	       }
	        
	        
	   }
	    
	   {
	       reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions, ordering, aggregation); 
	       int mapIds[] = new int[1]; 
	       mapIds[0] = mapId;
	       long shmRegionIds[]= new long [1];
	       shmRegionIds[0] = mapStatusResult.getRegionIdOfIndexBucket();
	       long offsetToIndexChunks[] = new long[1];
	       offsetToIndexChunks[0] = mapStatusResult.getOffsetOfIndexBucket();
	       long sizes[] = new long[1];
	       sizes[0] = mapStatusResult.getMapStatus()[reduceId]; //pick the first bucket; 
	        
	        ShuffleDataModel.ReduceStatus statuses =
		    new ShuffleDataModel.ReduceStatus(mapIds, shmRegionIds, offsetToIndexChunks, sizes);
		//NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet. 
		reduceSHMShuffleStore.mergeSort(statuses);
		 
	   }
	    
	   //to actually pull the data out 
	   {
	       int knumbers = 6; 
	       ArrayList<Integer> kvalues = new ArrayList<Integer>();
	       ArrayList<Object> vvalues = new ArrayList<Object> (); 
	       for (int i=0; i<knumbers; i++) {
		   kvalues.add(0); //initialization to 0; 
		   vvalues.add(null); //initialization to null;
	       }
	        
	       int actualRetrievedKNumbers= reduceSHMShuffleStore.getSimpleKVPairsWithIntKeys (kvalues, vvalues, knumbers);
	        
	       LOG.info("actual number of the keys retrieved is: " + actualRetrievedKNumbers);
	        
	       for (int i=0; i<actualRetrievedKNumbers; i++) {
		   LOG.info("retrieved k value: " + kvalues.get(i));
		   Object tvvalue= vvalues.get(i);
		   {
		       Object x = tvvalue;
		       Assert.assertTrue(x instanceof ApplicationTestClass); 
		       if (x instanceof ApplicationTestClass) {
			    
			   ApplicationTestClass y = (ApplicationTestClass) x; 
			   LOG.info("**" + " object: " + " page rank:" + y.getPageRank() 
				    + " url: " + y.getPageUrl() 
				    + " avg duration: " + y.getAvgDuration());
			       
			   //based on how I constructed the test data  
			   Assert.assertEquals(y.getPageRank(), kvalues.get(i).intValue());
			   Assert.assertEquals(y.getPageUrl(), "hello"+ kvalues.get(i).intValue());
			   Assert.assertEquals(y.getAvgDuration(), kvalues.get(i).intValue() + 1);
		       }
		        
		         
		   }
	       }
	        
	       Assert.assertEquals(actualRetrievedKNumbers, 5);
	       {
		   int actuals[] = new int[actualRetrievedKNumbers];
		   for (int i=0; i<actualRetrievedKNumbers; i++) {
		       actuals[i] = kvalues.get(i); 
		   }
		   
		   int expecteds[] = {1, 3, 5, 7, 9};
		       
		   Assert.assertArrayEquals(expecteds, actuals);
	       }

	   }
	    
	   reduceSHMShuffleStore.stop();
	   LOG.info("reduce shuffle store stopped");
	    
	   reduceSHMShuffleStore.shutdown();
	   LOG.info("reduce shuffle store shutdown");
	    
	   mapSHMShuffleStore.stop();
	   LOG.info("map shuffle store stopped");
	    
	   mapSHMShuffleStore.shutdown();
	   LOG.info("map shuffle store shutdown");
	    
	   ShuffleStoreManager.INSTANCE.shutdown();
	   LOG.info("shuffle store manager shutdown");
         }

         //sort and aggregation
	 @Test
	 public void testMapStatusFromMapShuffleStoreWithIntKeysWithReduceId1Test() {
		 
		 LOG.info("this is the test for mapStatusFromMapShuffleStoreWithIntKeysWithReduceId1Test");
		 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(
				 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
		 //to get a new heap instance for each new test case launched.
		 ShuffleStoreManager.INSTANCE.registershm();
		 
		 long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
		 LOG.info("native pointer of shuffle store manager retrieved is:"
		                + "0x"+ Long.toHexString(nativePointer));
		 
		 //then start the map shuffele store manager 
		 Kryo mapSideKryo=new Kryo();
		 mapSideKryo.register(ApplicationTestClass.class);
		 
		 //create a direct bytebuffer:
		 int bufferSize = 1*1024*1024; // 1M bytes
		 ByteBuffer mapSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 int shuffleId = 0;
		 int mapId = 1; 
		 int numberOfPartitions = 100;
		 
		 KValueTypeId keyType = KValueTypeId.Int; 
		 
		 int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();  
		 boolean ordering=true;
		 MapSHMShuffleStore mapSHMShuffleStore = 
				  ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer, 
				  logicalThreadId,
				  shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
		 
		 Kryo reduceSideKryo=new Kryo();
		 reduceSideKryo.register(ApplicationTestClass.class);
		 ByteBuffer reduceSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 //choose reduce that is not 0, or 1, or 7 (arbitrary number that is smaller than number of partitions)
		 int reduceId = 1; 
		 ReduceSHMShuffleStore reduceSHMShuffleStore = 
				 ShuffleStoreManager.INSTANCE.createReduceShuffleStore(reduceSideKryo, reduceSideByteBuffer,
                 shuffleId, reduceId, numberOfPartitions, ordering, true); 
		 
		 ShuffleDataModel.MapStatus mapStatusResult = null; 
		 {
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
			
			 mapStatusResult = mapSHMShuffleStore.sortAndStore();
			 
			 LOG.info("map status region id: " + mapStatusResult.getRegionIdOfIndexBucket());
			 LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult.getOffsetOfIndexBucket()));
			 long[] buckets = mapStatusResult.getMapStatus();
			 
			 if (buckets != null) {
				 for (int i=0; i<buckets.length; i++) {
					 if (buckets[i] > 0) {
					    LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
					 }
				 }
			 }
			 else {
				 LOG.info("map status buckets length is null.");
			 }
			 
		 
		 }
		 
		 {
			 reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions, ordering, true); 
			 int mapIds[] = new int[1]; 
			 mapIds[0] = mapId;
			 long shmRegionIds[]= new long [1];
			 shmRegionIds[0] = mapStatusResult.getRegionIdOfIndexBucket();
			 long offsetToIndexChunks[] = new long[1];
			 offsetToIndexChunks[0] = mapStatusResult.getOffsetOfIndexBucket();
			 long sizes[] = new long[1];
			 sizes[0] = mapStatusResult.getMapStatus()[reduceId]; //pick the first bucket; 
			 
			 ShuffleDataModel.ReduceStatus statuses =
					 new ShuffleDataModel.ReduceStatus(mapIds, shmRegionIds, offsetToIndexChunks, sizes);
			 //NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet. 
			 reduceSHMShuffleStore.mergeSort(statuses);
			 
		 }
		 
		 //to actually pull the data out 
		 {
			 int knumbers = 6; 
			 ArrayList<Integer> kvalues = new ArrayList<Integer>();
			 ArrayList<ArrayList<Object>> vvalues = new ArrayList<ArrayList<Object>> (); 
			 for (int i=0; i<knumbers; i++) {
				 kvalues.add(0); //initialization to 0; 
				 vvalues.add(null); //initialization to null;
			 }
			 
			 int actualRetrievedKNumbers= reduceSHMShuffleStore.getKVPairsWithIntKeys (kvalues, vvalues, knumbers);
			 
			 LOG.info("actual number of the keys retrieved is: " + actualRetrievedKNumbers);
			 
			 for (int i=0; i<actualRetrievedKNumbers; i++) {
				 LOG.info("retrieved k value: " + kvalues.get(i));
				 ArrayList<Object> tvvalues = vvalues.get(i);
				 for (int m=0; m<tvvalues.size(); m++) {
					 Object x = tvvalues.get(m);
					 Assert.assertTrue(x instanceof ApplicationTestClass); 
					 if (x instanceof ApplicationTestClass) {
						 
					    ApplicationTestClass y = (ApplicationTestClass) x; 
					    LOG.info("**" + " object: " + " page rank:" + y.getPageRank() 
					    		      + " url: " + y.getPageUrl() 
					    		      + " avg duration: " + y.getAvgDuration());
					    
					    //based on how I constructed the test data  
					    Assert.assertEquals(y.getPageRank(), kvalues.get(i).intValue());
					    Assert.assertEquals(y.getPageUrl(), "hello"+ kvalues.get(i).intValue());
					    Assert.assertEquals(y.getAvgDuration(), kvalues.get(i).intValue() + 1);
 					 }
					 
					  
				 }
			 }
			 
			 Assert.assertEquals(actualRetrievedKNumbers, 5);
			 {
				int actuals[] = new int[actualRetrievedKNumbers];
				for (int i=0; i<actualRetrievedKNumbers; i++) {
					actuals[i] = kvalues.get(i); 
				}
				
				int expecteds[] = {1, 3, 5, 7, 9};
			    
			    Assert.assertArrayEquals(expecteds, actuals);
			 }
			 
		 }
		 
		 reduceSHMShuffleStore.stop();
		 LOG.info("reduce shuffle store stopped");
		 
		 reduceSHMShuffleStore.shutdown();
		 LOG.info("reduce shuffle store shutdown");
		 
		 mapSHMShuffleStore.stop();
		 LOG.info("map shuffle store stopped");
		 
		 mapSHMShuffleStore.shutdown();
		 LOG.info("map shuffle store shutdown");
		 
		 ShuffleStoreManager.INSTANCE.shutdown();
		 LOG.info("shuffle store manager shutdown");
	 }
	 
	 

	 @Test
	 public void testMapStatusFromMapShuffleStoreWithIntKeysWithReduceId96Test() {
		 
		 LOG.info("this is the test for mapStatusFromMapShuffleStoreWithIntKeysWithReduceId96Test");
		 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(
				 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
		 //to get a new heap instance for each new test case launched.
		 ShuffleStoreManager.INSTANCE.registershm();
		 
		 long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
		 LOG.info("native pointer of shuffle store manager retrieved is:"
		                + "0x"+ Long.toHexString(nativePointer));
		 
		 //then start the map shuffele store manager 
		 Kryo mapSideKryo=new Kryo();
		 mapSideKryo.register(ApplicationTestClass.class);
		 
		 //create a direct bytebuffer:
		 int bufferSize = 1*1024*1024; // 1M bytes
		 ByteBuffer mapSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 int shuffleId = 0;
		 int mapId = 1; 
		 int numberOfPartitions = 100;
		 
		 KValueTypeId keyType = KValueTypeId.Int; 
		 
		 int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();  
		 boolean ordering=true;
		 MapSHMShuffleStore mapSHMShuffleStore = 
				  ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer,
				  logicalThreadId,
				  shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
		 
		 Kryo reduceSideKryo=new Kryo();
		 reduceSideKryo.register(ApplicationTestClass.class);
		 ByteBuffer reduceSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 //choose reduce that is not 0, or 1, or 7 (arbitrary number that is smaller than number of partitions)
		 int reduceId = 97; 
		 ReduceSHMShuffleStore reduceSHMShuffleStore = 
				 ShuffleStoreManager.INSTANCE.createReduceShuffleStore(reduceSideKryo, reduceSideByteBuffer,
                 shuffleId, reduceId, numberOfPartitions, ordering, true); 
		 
		 ShuffleDataModel.MapStatus mapStatusResult = null; 
		 {
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
			
			 mapStatusResult = mapSHMShuffleStore.sortAndStore();
			 
			 LOG.info("map status region name: " + mapStatusResult.getRegionIdOfIndexBucket());
			 LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult.getOffsetOfIndexBucket()));
			 long[] buckets = mapStatusResult.getMapStatus();
			 
			 if (buckets != null) {
				 for (int i=0; i<buckets.length; i++) {
					 if (buckets[i] > 0) {
					    LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
					 }
				 }
			 }
			 else {
				 LOG.info("map status buckets length is null.");
			 }
			 
		 
		 }
		 
		 {
			 reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions, ordering, true); 
			 int mapIds[] = new int[1]; 
			 mapIds[0] = mapId;
			 long shmRegionIds[]= new long [1];
			 shmRegionIds[0] = mapStatusResult.getRegionIdOfIndexBucket();
			 long offsetToIndexChunks[] = new long[1];
			 offsetToIndexChunks[0] = mapStatusResult.getOffsetOfIndexBucket();
			 long sizes[] = new long[1];
			 sizes[0] = mapStatusResult.getMapStatus()[reduceId]; //pick the first bucket; 
			 
			 ShuffleDataModel.ReduceStatus statuses =
					 new ShuffleDataModel.ReduceStatus(mapIds, shmRegionIds, offsetToIndexChunks, sizes);
			 //NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet. 
			 reduceSHMShuffleStore.mergeSort(statuses);
			 
		 }
		 
		 //to actually pull the data out 
		 {
			 int knumbers = 6; 
			 ArrayList<Integer> kvalues = new ArrayList<Integer>();
			 ArrayList<ArrayList<Object>> vvalues = new ArrayList<ArrayList<Object>> (); 
			 for (int i=0; i<knumbers; i++) {
				 kvalues.add(0); //initialization to 0; 
				 vvalues.add(null); //initialization to null;
			 }
			 
			 int actualRetrievedKNumbers= reduceSHMShuffleStore.getKVPairsWithIntKeys (kvalues, vvalues, knumbers);
			 
			 LOG.info("actual number of the keys retrieved is: " + actualRetrievedKNumbers);
			 
			 for (int i=0; i<actualRetrievedKNumbers; i++) {
				 LOG.info("retrieved k value: " + kvalues.get(i));
				 ArrayList<Object> tvvalues = vvalues.get(i);
				 for (int m=0; m<tvvalues.size(); m++) {
					 Object x = tvvalues.get(m);
					 Assert.assertTrue(x instanceof ApplicationTestClass); 
					 if (x instanceof ApplicationTestClass) {
						 
					    ApplicationTestClass y = (ApplicationTestClass) x; 
					    LOG.info("**" + " object: " + " page rank:" + y.getPageRank() 
					    		      + " url: " + y.getPageUrl() 
					    		      + " avg duration: " + y.getAvgDuration());
					    
					    //based on how I constructed the test data  
					    Assert.assertEquals(y.getPageRank(), kvalues.get(i).intValue());
					    Assert.assertEquals(y.getPageUrl(), "hello"+ kvalues.get(i).intValue());
					    Assert.assertEquals(y.getAvgDuration(), kvalues.get(i).intValue() + 1);
 					 }
					 
					  
				 }
			 }
			 
			 Assert.assertEquals(actualRetrievedKNumbers, 0);
			 
		 }
		 
		 reduceSHMShuffleStore.stop();
		 LOG.info("reduce shuffle store stopped");
		 
		 reduceSHMShuffleStore.shutdown();
		 LOG.info("reduce shuffle store shutdown");
		 
		 mapSHMShuffleStore.stop();
		 LOG.info("map shuffle store stopped");
		 
		 mapSHMShuffleStore.shutdown();
		 LOG.info("map shuffle store shutdown");
		 
		 ShuffleStoreManager.INSTANCE.shutdown();
		 LOG.info("shuffle store manager shutdown");
	 }
	 
         //sort, without aggregation. 
         @Test
         public void testMapStatusFromMapShuffleStoreWithIntKeysWithReduceId96TestWithNoAggregation() {
	  
	   LOG.info("this is the test for mapStatusFromMapShuffleStoreWithIntKeysWithReduceId96TestWithNoAggregation");
	  
	   int executorId = 0; 
	   ShuffleStoreManager.INSTANCE.initialize(
						 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
	   //to get a new heap instance for each new test case launched.
	   ShuffleStoreManager.INSTANCE.registershm();
	  
	   long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
	   LOG.info("native pointer of shuffle store manager retrieved is:"
		  + "0x"+ Long.toHexString(nativePointer));
	  
	   //then start the map shuffele store manager 
	   Kryo mapSideKryo=new Kryo();
	   mapSideKryo.register(ApplicationTestClass.class);
	  
	   //create a direct bytebuffer:
	   int bufferSize = 1*1024*1024; // 1M bytes
	   ByteBuffer mapSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
	  
	   int shuffleId = 0;
	   int mapId = 1; 
	   int numberOfPartitions = 100;
	  
	   KValueTypeId keyType = KValueTypeId.Int; 
	  
	   int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();  
	   boolean ordering=true;
	   MapSHMShuffleStore mapSHMShuffleStore = 
	      ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer,
	         logicalThreadId,
		 shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
	   
	   Kryo reduceSideKryo=new Kryo();
	   reduceSideKryo.register(ApplicationTestClass.class);
	   ByteBuffer reduceSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
	   
	   //choose reduce that is not 0, or 1, or 7 (arbitrary number that is smaller than number of partitions)
	   int reduceId = 97; 
	   boolean aggregation = false; 
	   ReduceSHMShuffleStore reduceSHMShuffleStore = 
	       ShuffleStoreManager.INSTANCE.createReduceShuffleStore(reduceSideKryo, reduceSideByteBuffer,
								     shuffleId, reduceId, numberOfPartitions, ordering, aggregation); 
	    
	   ShuffleDataModel.MapStatus mapStatusResult = null; 
	   {
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
	       
	       mapStatusResult = mapSHMShuffleStore.sortAndStore();
	        
	       LOG.info("map status region name: " + mapStatusResult.getRegionIdOfIndexBucket());
	       LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult.getOffsetOfIndexBucket()));
	       long[] buckets = mapStatusResult.getMapStatus();
	        
	       if (buckets != null) {
		   for (int i=0; i<buckets.length; i++) {
		       if (buckets[i] > 0) {
			   LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
		       }
		   }
	       }
	       else {
		   LOG.info("map status buckets length is null.");
	       }
	        
	        
	   }
	    
	   {
	       reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions, ordering, aggregation); 
	       int mapIds[] = new int[1]; 
	       mapIds[0] = mapId;
	       long shmRegionIds[]= new long [1];
	       shmRegionIds[0] = mapStatusResult.getRegionIdOfIndexBucket();
	       long offsetToIndexChunks[] = new long[1];
	       offsetToIndexChunks[0] = mapStatusResult.getOffsetOfIndexBucket();
	       long sizes[] = new long[1];
	       sizes[0] = mapStatusResult.getMapStatus()[reduceId]; //pick the first bucket; 
	        
	        ShuffleDataModel.ReduceStatus statuses =
		    new ShuffleDataModel.ReduceStatus(mapIds, shmRegionIds, offsetToIndexChunks, sizes);
		//NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet. 
		reduceSHMShuffleStore.mergeSort(statuses);
		 
	   }
	    
	   //to actually pull the data out 
	   {
	       int knumbers = 6; 
	       ArrayList<Integer> kvalues = new ArrayList<Integer>();
	       ArrayList<Object> vvalues = new ArrayList<Object> (); 
	       for (int i=0; i<knumbers; i++) {
		   kvalues.add(0); //initialization to 0; 
		   vvalues.add(null); //initialization to null;
	       }
	        
	       int actualRetrievedKNumbers= reduceSHMShuffleStore.getSimpleKVPairsWithIntKeys (kvalues, vvalues, knumbers);

	        
	       LOG.info("actual number of the keys retrieved is: " + actualRetrievedKNumbers);
	        
	       for (int i=0; i<actualRetrievedKNumbers; i++) {
		   LOG.info("retrieved k value: " + kvalues.get(i));
		   Object tvvalue = vvalues.get(i);
		   {
		       Object x = tvvalue;
		       Assert.assertTrue(x instanceof ApplicationTestClass); 
		       if (x instanceof ApplicationTestClass) {
			    
			   ApplicationTestClass y = (ApplicationTestClass) x; 
			   LOG.info("**" + " object: " + " page rank:" + y.getPageRank() 
				    + " url: " + y.getPageUrl() 
				    + " avg duration: " + y.getAvgDuration());
			       
			   //based on how I constructed the test data  
			   Assert.assertEquals(y.getPageRank(), kvalues.get(i).intValue());
			   Assert.assertEquals(y.getPageUrl(), "hello"+ kvalues.get(i).intValue());
			   Assert.assertEquals(y.getAvgDuration(), kvalues.get(i).intValue() + 1);
		       }
		        
		         
		   }
	       }
	        
	       Assert.assertEquals(actualRetrievedKNumbers, 0);
	   }
	    
	   reduceSHMShuffleStore.stop();
	   LOG.info("reduce shuffle store stopped");
	    
	   reduceSHMShuffleStore.shutdown();
	   LOG.info("reduce shuffle store shutdown");
	    
	   mapSHMShuffleStore.stop();
	   LOG.info("map shuffle store stopped");
	    
	   mapSHMShuffleStore.shutdown();
	   LOG.info("map shuffle store shutdown");
	    
	   ShuffleStoreManager.INSTANCE.shutdown();
	   LOG.info("shuffle store manager shutdown");
         }

	 @Test
	 public void testMapStatusFromMapShuffleStoreWithIntKeysWithReduceIdTwoMergedTest() {
		 
		 LOG.info("this is the test for mapStatusFromMapShuffleStoreWithIntKeysWithReduceIdTwoMergedTest");
 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(
				 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
		 //to get a new heap instance for each new test case launched.
		 ShuffleStoreManager.INSTANCE.registershm();
		 
		 long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
		 LOG.info("native pointer of shuffle store manager retrieved is:"
		                + "0x"+ Long.toHexString(nativePointer));
		 
		 //then start the map shuffele store manager 
		 Kryo mapSideKryo=new Kryo();
		 mapSideKryo.register(ApplicationTestClass.class);
		 
		 //create a direct bytebuffer:
		 int bufferSize = 1*1024*1024; // 1M bytes
		 ByteBuffer mapSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 int shuffleId = 0;
		 int mapId1 = 1; 
		 int numberOfPartitions = 100;  
		 
		 KValueTypeId keyType = KValueTypeId.Int; 
		 
		 int logicalThreadId1 =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();
		 boolean ordering= true;
		 MapSHMShuffleStore mapSHMShuffleStore1 = 
		      ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer,
			  logicalThreadId1,
			  shuffleId, mapId1, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
		 
		 int mapId2 = 4; 
		 int logicalThreadId2 =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter(); 
		 MapSHMShuffleStore mapSHMShuffleStore2 = 
			  ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer, 
			  logicalThreadId2,
			  shuffleId, mapId2, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
		 
		 Kryo reduceSideKryo=new Kryo();
		 reduceSideKryo.register(ApplicationTestClass.class);
		 ByteBuffer reduceSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 //choose reduce that is not 0, or 1, or 7 (arbitrary number that is smaller than number of partitions)
		 int reduceId = 0; 
		 ReduceSHMShuffleStore reduceSHMShuffleStore = 
				 ShuffleStoreManager.INSTANCE.createReduceShuffleStore(reduceSideKryo, reduceSideByteBuffer,
                 shuffleId, reduceId, numberOfPartitions, ordering, true); 
		 
		 ShuffleDataModel.MapStatus mapStatusResult1 = null; 
		 {
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
			     mapSHMShuffleStore1.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
								 i, KValueTypeId.Int.state);
			 }
			 
			 // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
	                 //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
			 int numberOfPairs = numberOfVs; 
			 mapSHMShuffleStore1.storeKVPairs(numberOfPairs, KValueTypeId.Int.state);
			
			 mapStatusResult1 = mapSHMShuffleStore1.sortAndStore();
			 
			 LOG.info("map status region id: " + mapStatusResult1.getRegionIdOfIndexBucket());
			 LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult1.getOffsetOfIndexBucket()));
			 long[] buckets = mapStatusResult1.getMapStatus();
			 
			 if (buckets != null) {
				 for (int i=0; i<buckets.length; i++) {
					 if (buckets[i] > 0) {
					    LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
					 }
				 }
			 }
			 else {
				 LOG.info("map status buckets length is null.");
			 }
			 
		 
		 }
		 
		 ShuffleDataModel.MapStatus mapStatusResult2 = null;
		 {
			 int numberOfVs = 10; 
			 ArrayList<Object> testObjects = new ArrayList<Object> ();
			 ArrayList<Integer> partitions = new ArrayList<Integer> ();
			 ArrayList<Integer> kvalues = new ArrayList<Integer> ();
	 		 ArrayList<Integer> voffsets = new ArrayList<Integer> ();
			 for (int i=0; i<numberOfVs; i++) {
				  ApplicationTestClass obj = new ApplicationTestClass (i, "hello" +i,  i+1); 
				  testObjects.add(obj);
				  voffsets.add(0); //initialized it.
				  partitions.add(i%2);
				  kvalues.add(i);
			 }
			 
			 for (int i=0; i<numberOfVs; i++) {
			     mapSHMShuffleStore2.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
								 i, KValueTypeId.Int.state);
			 }
			  
			 // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
	                 //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
			 int numberOfPairs = numberOfVs; 
			 mapSHMShuffleStore2.storeKVPairs(numberOfPairs, KValueTypeId.Int.state);
			
			 mapStatusResult2 = mapSHMShuffleStore2.sortAndStore();
			 
			 LOG.info("map status region id: " + mapStatusResult2.getRegionIdOfIndexBucket());
			 LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult2.getOffsetOfIndexBucket()));
			 long[] buckets = mapStatusResult2.getMapStatus();
			 
			 if (buckets != null) {
				 for (int i=0; i<buckets.length; i++) {
					 if (buckets[i] > 0) {
					    LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
					 }
				 }
			 }
			 else {
				 LOG.info("map status buckets length is null.");
			 }
			 
		 
		 }
		 
		 {
			 reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions, ordering, true); 
			 int mapIds[] = new int[2]; 
			 mapIds[0] = mapId1;
			 mapIds[1] = mapId2;
			 
			 long shmRegionIds[]= new long [2];
			 shmRegionIds[0] = mapStatusResult1.getRegionIdOfIndexBucket();
			 shmRegionIds[1] = mapStatusResult2.getRegionIdOfIndexBucket();
			 
			 long offsetToIndexChunks[] = new long[2];
			 offsetToIndexChunks[0] = mapStatusResult1.getOffsetOfIndexBucket();
			 offsetToIndexChunks[1] = mapStatusResult2.getOffsetOfIndexBucket();
			 
			 long sizes[] = new long[2];
			 sizes[0] = mapStatusResult1.getMapStatus()[reduceId]; //pick the first bucket; 
			 sizes[1] = mapStatusResult2.getMapStatus()[reduceId]; //pick the first bucket; 
			 
			 ShuffleDataModel.ReduceStatus statuses =
					 new ShuffleDataModel.ReduceStatus(mapIds, shmRegionIds, offsetToIndexChunks, sizes);
			 //NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet. 
			 reduceSHMShuffleStore.mergeSort(statuses);
			 
		 }
		 
		 //to actually pull the data out 
		 {
			 int knumbers = 6; 
			 ArrayList<Integer> kvalues = new ArrayList<Integer>();
			 ArrayList<ArrayList<Object>> vvalues = new ArrayList<ArrayList<Object>> (); 
			 for (int i=0; i<knumbers; i++) {
				 kvalues.add(0); //initialization to 0; 
				 vvalues.add(null); //initialization to null;
			 }
			 
			 int actualRetrievedKNumbers= reduceSHMShuffleStore.getKVPairsWithIntKeys (kvalues, vvalues, knumbers);
			 
			 LOG.info("actual number of the keys retrieved is: " + actualRetrievedKNumbers);
			 
			 for (int i=0; i<actualRetrievedKNumbers; i++) {
				 LOG.info("retrieved k value: " + kvalues.get(i));
				 ArrayList<Object> tvvalues = vvalues.get(i);
				 Assert.assertEquals(tvvalues.size(), 2); //each k now has two values to be associated. 
				 
				 for (int m=0; m<tvvalues.size(); m++) {
					 Object x = tvvalues.get(m);
					 Assert.assertTrue(x instanceof ApplicationTestClass); 
					 if (x instanceof ApplicationTestClass) {
						 
					    ApplicationTestClass y = (ApplicationTestClass) x; 
					    LOG.info("**" + " object: " + " page rank:" + y.getPageRank() 
					    		      + " url: " + y.getPageUrl() 
					    		      + " avg duration: " + y.getAvgDuration());
					    
					    //based on how I constructed the test data  
					    Assert.assertEquals(y.getPageRank(), kvalues.get(i).intValue());
					    Assert.assertEquals(y.getPageUrl(), "hello"+ kvalues.get(i).intValue());
					    Assert.assertEquals(y.getAvgDuration(), kvalues.get(i).intValue() + 1);
 					 }
					 
					  
				 }
			 }
			 
			 Assert.assertEquals(actualRetrievedKNumbers, 5);
			 {
				int actuals[] = new int[actualRetrievedKNumbers];
				for (int i=0; i<actualRetrievedKNumbers; i++) {
					actuals[i] = kvalues.get(i); 
				}
				
				int expecteds[] = {0, 2, 4, 6, 8};
			    
			    Assert.assertArrayEquals(expecteds, actuals);
			 }
			 
		 }
		 
		 reduceSHMShuffleStore.stop();
		 LOG.info("reduce shuffle store stopped");
		 
		 reduceSHMShuffleStore.shutdown();
		 LOG.info("reduce shuffle store shutdown");
		 
		 mapSHMShuffleStore1.stop();
		 LOG.info("map shuffle1 store stopped");
		 
		 mapSHMShuffleStore1.shutdown();
		 LOG.info("map shuffle store shutdown");
		 
		 mapSHMShuffleStore2.stop();
		 LOG.info("map shuffle1 store stopped");
		 
		 mapSHMShuffleStore2.shutdown();
		 LOG.info("map shuffle store shutdown");
		 
		 ShuffleStoreManager.INSTANCE.shutdown();
		 LOG.info("shuffle store manager shutdown");
	 }
	 
         //sort, without aggregation 
         @Test
         public void testMapStatusFromMapShuffleStoreWithIntKeysWithReduceIdTwoMergedTestWithNoAggregation() {
	  
	    LOG.info("this is the test for mapStatusFromMapShuffleStoreWithIntKeysWithReduceIdTwoMergedTestWithNoAggregation");
 
	    int executorId = 0; 
	    ShuffleStoreManager.INSTANCE.initialize(
		 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
	    //to get a new heap instance for each new test case launched.
	    ShuffleStoreManager.INSTANCE.registershm();
	  
	    long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
	    LOG.info("native pointer of shuffle store manager retrieved is:"
		  + "0x"+ Long.toHexString(nativePointer));
	  
	    //then start the map shuffele store manager 
	    Kryo mapSideKryo=new Kryo();
	    mapSideKryo.register(ApplicationTestClass.class);
	  
	    //create a direct bytebuffer:
	    int bufferSize = 1*1024*1024; // 1M bytes
	    ByteBuffer mapSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
	  
	    int shuffleId = 0;
	    int mapId1 = 1; 
	    int numberOfPartitions = 100;  
	  
	    KValueTypeId keyType = KValueTypeId.Int; 
	  
	    int logicalThreadId1 =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();
	    boolean ordering= true;
	    MapSHMShuffleStore mapSHMShuffleStore1 = 
	      ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer,
		 logicalThreadId1,
         	 shuffleId, mapId1, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
	   
	    int mapId2 = 4; 
	    int logicalThreadId2 =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter(); 
	    MapSHMShuffleStore mapSHMShuffleStore2 = 
	       ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer, 
	          logicalThreadId2,
		  shuffleId, mapId2, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
	    
	    Kryo reduceSideKryo=new Kryo();
	    reduceSideKryo.register(ApplicationTestClass.class);
	    ByteBuffer reduceSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
	    
	    //choose reduce that is not 0, or 1, or 7 (arbitrary number that is smaller than number of partitions)
	    int reduceId = 0; 
	    boolean aggregation = false; 
	    ReduceSHMShuffleStore reduceSHMShuffleStore = 
		ShuffleStoreManager.INSTANCE.createReduceShuffleStore(reduceSideKryo, reduceSideByteBuffer,
			      shuffleId, reduceId, numberOfPartitions, ordering, aggregation); 
	     
	    ShuffleDataModel.MapStatus mapStatusResult1 = null; 
	    {
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
		    mapSHMShuffleStore1.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
							i, KValueTypeId.Int.state);
		}

		 
		// storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
		//ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
		int numberOfPairs = numberOfVs; 
		mapSHMShuffleStore1.storeKVPairs(numberOfPairs, KValueTypeId.Int.state);
		
		mapStatusResult1 = mapSHMShuffleStore1.sortAndStore();
		 
		LOG.info("map status region id: " + mapStatusResult1.getRegionIdOfIndexBucket());
		LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult1.getOffsetOfIndexBucket()));
		long[] buckets = mapStatusResult1.getMapStatus();
		 
		if (buckets != null) {
		    for (int i=0; i<buckets.length; i++) {
			if (buckets[i] > 0) {
			    LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
			}
		    }
		}
		else {
		    LOG.info("map status buckets length is null.");
		}
		 
		 
	    }
	     
	    ShuffleDataModel.MapStatus mapStatusResult2 = null;
	    {
		int numberOfVs = 10; 
		ArrayList<Object> testObjects = new ArrayList<Object> ();
		ArrayList<Integer> partitions = new ArrayList<Integer> ();
		ArrayList<Integer> kvalues = new ArrayList<Integer> ();
		ArrayList<Integer> voffsets = new ArrayList<Integer> ();
		for (int i=0; i<numberOfVs; i++) {
		    ApplicationTestClass obj = new ApplicationTestClass (i, "hello" +i,  i+1); 
		    testObjects.add(obj);
		    voffsets.add(0); //initialized it.
		    partitions.add(i%2);
		    kvalues.add(i);
		}
		 
		for (int i=0; i<numberOfVs; i++) {
		    mapSHMShuffleStore2.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
							i, KValueTypeId.Int.state);
		}
		 
		// storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
		//ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
		int numberOfPairs = numberOfVs; 
		mapSHMShuffleStore2.storeKVPairs(numberOfPairs, KValueTypeId.Int.state);
		
		mapStatusResult2 = mapSHMShuffleStore2.sortAndStore();
		 
		LOG.info("map status region id: " + mapStatusResult2.getRegionIdOfIndexBucket());
		LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult2.getOffsetOfIndexBucket()));
		long[] buckets = mapStatusResult2.getMapStatus();
		 
		if (buckets != null) {
		    for (int i=0; i<buckets.length; i++) {
			if (buckets[i] > 0) {
			    LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
			}
		    }
		}
		else {
		    LOG.info("map status buckets length is null.");
		}
		 
		 
	    }
	     
	    {
		reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions, ordering, aggregation); 
		int mapIds[] = new int[2]; 
		mapIds[0] = mapId1;
		mapIds[1] = mapId2;
		 
		long shmRegionIds[]= new long [2];
		shmRegionIds[0] = mapStatusResult1.getRegionIdOfIndexBucket();
		shmRegionIds[1] = mapStatusResult2.getRegionIdOfIndexBucket();
		 
		long offsetToIndexChunks[] = new long[2];
		offsetToIndexChunks[0] = mapStatusResult1.getOffsetOfIndexBucket();
		offsetToIndexChunks[1] = mapStatusResult2.getOffsetOfIndexBucket();
		 
		long sizes[] = new long[2];
		sizes[0] = mapStatusResult1.getMapStatus()[reduceId]; //pick the first bucket; 
		sizes[1] = mapStatusResult2.getMapStatus()[reduceId]; //pick the first bucket; 
		 
		 ShuffleDataModel.ReduceStatus statuses =
		     new ShuffleDataModel.ReduceStatus(mapIds, shmRegionIds, offsetToIndexChunks, sizes);
		 //NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet. 
		 reduceSHMShuffleStore.mergeSort(statuses);
		  
	    }
	     
	    //to actually pull the data out 
	    {
                //now the actual number is doubled, compared to sort-aggregation test case. we would expect to get 2*5=10.
		int knumbers = 14; //needs to be larger than 2*5=10.
		ArrayList<Integer> kvalues = new ArrayList<Integer>();
		ArrayList<Object> vvalues = new ArrayList<Object> (); 
		for (int i=0; i<knumbers; i++) {
		    kvalues.add(0); //initialization to 0; 
		    vvalues.add(null); //initialization to null;
		}
		 
		int actualRetrievedKNumbers= reduceSHMShuffleStore.getSimpleKVPairsWithIntKeys (kvalues, vvalues, knumbers);

		LOG.info("actual number of the keys retrieved is: " + actualRetrievedKNumbers);
		 
		for (int i=0; i<actualRetrievedKNumbers; i++) {
		    LOG.info("retrieved k value: " + kvalues.get(i));
		    Object tvvalue = vvalues.get(i);
		     
		    {
			Object x = tvvalue;
			Assert.assertTrue(x instanceof ApplicationTestClass); 
			if (x instanceof ApplicationTestClass) {
			     
			    ApplicationTestClass y = (ApplicationTestClass) x; 
			    LOG.info("**" + " object: " + " page rank:" + y.getPageRank() 
				     + " url: " + y.getPageUrl() 
				     + " avg duration: " + y.getAvgDuration());
			        
			    //based on how I constructed the test data  
			    Assert.assertEquals(y.getPageRank(), kvalues.get(i).intValue());
			    Assert.assertEquals(y.getPageUrl(), "hello"+ kvalues.get(i).intValue());
			    Assert.assertEquals(y.getAvgDuration(), kvalues.get(i).intValue() + 1);
			}
			 
			  
		    }
		}
		 
		//Assert.assertEquals(actualRetrievedKNumbers, 5);
		Assert.assertEquals(actualRetrievedKNumbers, 10); //double, because we produce single <key, value> pairs. 
		{
		    int actuals[] = new int[actualRetrievedKNumbers];
		    for (int i=0; i<actualRetrievedKNumbers; i++) {
			actuals[i] = kvalues.get(i); 
		    }
		    
		    int expecteds[] = {0, 0, 2, 2, 4, 4, 6, 6, 8, 8};
		        
		    Assert.assertArrayEquals(expecteds, actuals);
		}

	    }
	     
	    reduceSHMShuffleStore.stop();
	    LOG.info("reduce shuffle store stopped");
	     
	    reduceSHMShuffleStore.shutdown();
	    LOG.info("reduce shuffle store shutdown");
	     
	    mapSHMShuffleStore1.stop();
	    LOG.info("map shuffle1 store stopped");
	     
	    mapSHMShuffleStore1.shutdown();
	    LOG.info("map shuffle store shutdown");
	     
	    mapSHMShuffleStore2.stop();
	    LOG.info("map shuffle1 store stopped");
	     
	    mapSHMShuffleStore2.shutdown();
	    LOG.info("map shuffle store shutdown");
	     
	    ShuffleStoreManager.INSTANCE.shutdown();
	    LOG.info("shuffle store manager shutdown");
         }
     
         //sort, without aggregation 
         @Test
         public void testMapStatusFromMapShuffleStoreWithIntKeysWithReduceId0RetrievedMultipleTimesWithNoAggregation() {
	  
	    LOG.info("this is the test for mapStatusFromMapShuffleStoreWithIntKeysWithReduceId0RetrievedMultipleTimesTestWithNoAggregation");
	 
	    int executorId = 0; 
	    ShuffleStoreManager.INSTANCE.initialize(
						 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
	    //to get a new heap instance for each new test case launched.
	    ShuffleStoreManager.INSTANCE.registershm();
 
	    long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
	    LOG.info("native pointer of shuffle store manager retrieved is:"
		  + "0x"+ Long.toHexString(nativePointer));
	  
	    //then start the map shuffle store manager 
	    Kryo mapSideKryo=new Kryo();
	    mapSideKryo.register(ApplicationTestClass.class);
	  
	    //create a direct bytebuffer:
	    int bufferSize = 1*1024*1024; // 1M bytes
	    ByteBuffer mapSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
	  
	    int shuffleId = 0;
	    int mapId = 1; 
	    int numberOfPartitions = 100;
	  
	    KValueTypeId keyType = KValueTypeId.Int; 
	  

	    int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter(); 
	    boolean ordering=true;
	    MapSHMShuffleStore mapSHMShuffleStore = 
	      ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer,
	             logicalThreadId,
	             shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
	   
	    Kryo reduceSideKryo=new Kryo();
	    reduceSideKryo.register(ApplicationTestClass.class);
	    ByteBuffer reduceSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
	   
	    //choose reduce that is not 0, or 1, or 7 (arbitrary number that is smaller than number of partitions)
	    int reduceId = 0; 
	    boolean aggregation = false; 
	    ReduceSHMShuffleStore reduceSHMShuffleStore = 
	       ShuffleStoreManager.INSTANCE.createReduceShuffleStore(reduceSideKryo, reduceSideByteBuffer,
		     shuffleId, reduceId, numberOfPartitions, ordering, aggregation); 
	    
	   ShuffleDataModel.MapStatus mapStatusResult = null; 
	   {
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
	       
	       mapStatusResult = mapSHMShuffleStore.sortAndStore();
	        
	       LOG.info("map status region id: " + mapStatusResult.getRegionIdOfIndexBucket());
	       LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult.getOffsetOfIndexBucket()));
	       long[] buckets = mapStatusResult.getMapStatus();
	        
	       if (buckets != null) {
		   for (int i=0; i<buckets.length; i++) {
		       if (buckets[i] > 0) {
			   LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
		       }
		   }
	       }
	       else {
		   LOG.info("map status buckets length is null.");
	       }
	        
	        
	   }
	    
	   {
	       reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions, ordering, aggregation); 
	       int mapIds[] = new int[1]; 
	       mapIds[0] = mapId;
	       long shmRegionIds[]= new long [1];
	       shmRegionIds[0] = mapStatusResult.getRegionIdOfIndexBucket();
	       long offsetToIndexChunks[] = new long[1];
	       offsetToIndexChunks[0] = mapStatusResult.getOffsetOfIndexBucket();
	       long sizes[] = new long[1];
	       sizes[0] = mapStatusResult.getMapStatus()[reduceId]; //pick the first bucket; 
	        
	        ShuffleDataModel.ReduceStatus statuses =
		    new ShuffleDataModel.ReduceStatus(mapIds, shmRegionIds, offsetToIndexChunks, sizes);
		//NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet. 
		reduceSHMShuffleStore.mergeSort(statuses);
		 
	   }
	    
	   //to actually pull the data out 
	   int actualRetrievedKNumbers =0; 
	   int accumulatedRetrievedNumbers = 0; 
	   ArrayList<Integer> accumualtedKeys = new ArrayList<Integer> ();
	   do 
		{
		    int knumbers = 2; 
		    ArrayList<Integer> kvalues = new ArrayList<Integer>();
		    ArrayList<ArrayList<Object>> vvalues = new ArrayList<ArrayList<Object>> (); 
		    for (int i=0; i<knumbers; i++) {
			kvalues.add(0); //initialization to 0; 
			vvalues.add(null); //initialization to null;
		    }
		     
		    actualRetrievedKNumbers= reduceSHMShuffleStore.getKVPairsWithIntKeys (kvalues, vvalues, knumbers);
		    accumulatedRetrievedNumbers +=actualRetrievedKNumbers;
		     
		    LOG.info("==================actual number of the keys retrieved is: " + actualRetrievedKNumbers + "=======================");
		     
		    for (int i=0; i<actualRetrievedKNumbers; i++) {
			LOG.info("retrieved k value: " + kvalues.get(i));
			accumualtedKeys.add(kvalues.get(i));
			 
			ArrayList<Object> tvvalues = vvalues.get(i);
			for (int m=0; m<tvvalues.size(); m++) {
			    Object x = tvvalues.get(m);
			    Assert.assertTrue(x instanceof ApplicationTestClass); 
			    if (x instanceof ApplicationTestClass) {
				 
				ApplicationTestClass y = (ApplicationTestClass) x; 
				LOG.info("**" + " object: " + " page rank:" + y.getPageRank() 
					 + " url: " + y.getPageUrl() 
					 + " avg duration: " + y.getAvgDuration());
				   
				    
			    }
			     
			      
			}
		    }
		     
	    }
	    while (actualRetrievedKNumbers > 0);
	     
	
	    Assert.assertEquals(accumulatedRetrievedNumbers, 5);
		     
	    {
		int actuals[] = new int[accumulatedRetrievedNumbers];
		for (int i=0; i<accumulatedRetrievedNumbers; i++) {
		    actuals[i] = accumualtedKeys.get(i); 
		}
		
		int expecteds[] = {0, 2, 4, 6, 8};
		    
		Assert.assertArrayEquals(expecteds, actuals);
	    }
	     
	    reduceSHMShuffleStore.stop();
	    LOG.info("reduce shuffle store stopped");
	     
	    reduceSHMShuffleStore.shutdown();
	    LOG.info("reduce shuffle store shutdown");
	     
	    mapSHMShuffleStore.stop();
	    LOG.info("map shuffle store stopped");
	     
	    mapSHMShuffleStore.shutdown();
	    LOG.info("map shuffle store shutdown");
	     
	    ShuffleStoreManager.INSTANCE.shutdown();
	    LOG.info("shuffle store manager shutdown");
         }
 
	 @Test
	 public void testMapStatusFromMapShuffleStoreWithIntKeysWithReduceId0RetrievedMultipleTimes() {
		 
		 LOG.info("this is the test for mapStatusFromMapShuffleStoreWithIntKeysWithReduceId0RetrievedMultipleTimesTest");
	
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(
				 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
		 //to get a new heap instance for each new test case launched.
		 ShuffleStoreManager.INSTANCE.registershm();
 
		 long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
		 LOG.info("native pointer of shuffle store manager retrieved is:"
		                + "0x"+ Long.toHexString(nativePointer));
		 
		 //then start the map shuffle store manager 
		 Kryo mapSideKryo=new Kryo();
		 mapSideKryo.register(ApplicationTestClass.class);
		 
		 //create a direct bytebuffer:
		 int bufferSize = 1*1024*1024; // 1M bytes
		 ByteBuffer mapSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 int shuffleId = 0;
		 int mapId = 1; 
		 int numberOfPartitions = 100;
		 
		 KValueTypeId keyType = KValueTypeId.Int; 
		 

		 int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter(); 
		 boolean ordering=true;
		 MapSHMShuffleStore mapSHMShuffleStore = 
				  ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer,
				  logicalThreadId,
				  shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
		 
		 Kryo reduceSideKryo=new Kryo();
		 reduceSideKryo.register(ApplicationTestClass.class);
		 ByteBuffer reduceSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 //choose reduce that is not 0, or 1, or 7 (arbitrary number that is smaller than number of partitions)
		 int reduceId = 0; 
		 ReduceSHMShuffleStore reduceSHMShuffleStore = 
				 ShuffleStoreManager.INSTANCE.createReduceShuffleStore(reduceSideKryo, reduceSideByteBuffer,
                 shuffleId, reduceId, numberOfPartitions, ordering, true); 
		 
		 ShuffleDataModel.MapStatus mapStatusResult = null; 
		 {
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
			
			 mapStatusResult = mapSHMShuffleStore.sortAndStore();
			 
			 LOG.info("map status region id: " + mapStatusResult.getRegionIdOfIndexBucket());
			 LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult.getOffsetOfIndexBucket()));
			 long[] buckets = mapStatusResult.getMapStatus();
			 
			 if (buckets != null) {
				 for (int i=0; i<buckets.length; i++) {
					 if (buckets[i] > 0) {
					    LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
					 }
				 }
			 }
			 else {
				 LOG.info("map status buckets length is null.");
			 }
			 
		 
		 }
		 
		 {
			 reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions, ordering, true); 
			 int mapIds[] = new int[1]; 
			 mapIds[0] = mapId;
			 long shmRegionIds[]= new long [1];
			 shmRegionIds[0] = mapStatusResult.getRegionIdOfIndexBucket();
			 long offsetToIndexChunks[] = new long[1];
			 offsetToIndexChunks[0] = mapStatusResult.getOffsetOfIndexBucket();
			 long sizes[] = new long[1];
			 sizes[0] = mapStatusResult.getMapStatus()[reduceId]; //pick the first bucket; 
			 
			 ShuffleDataModel.ReduceStatus statuses =
					 new ShuffleDataModel.ReduceStatus(mapIds, shmRegionIds, offsetToIndexChunks, sizes);
			 //NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet. 
			 reduceSHMShuffleStore.mergeSort(statuses);
			 
		 }
		 
		 //to actually pull the data out 
		 int actualRetrievedKNumbers =0; 
		 int accumulatedRetrievedNumbers = 0; 
		 ArrayList<Integer> accumualtedKeys = new ArrayList<Integer> ();
		 do 
		 {
			 int knumbers = 2; 
			 ArrayList<Integer> kvalues = new ArrayList<Integer>();
			 ArrayList<ArrayList<Object>> vvalues = new ArrayList<ArrayList<Object>> (); 
			 for (int i=0; i<knumbers; i++) {
				 kvalues.add(0); //initialization to 0; 
				 vvalues.add(null); //initialization to null;
			 }
			 
			 actualRetrievedKNumbers= reduceSHMShuffleStore.getKVPairsWithIntKeys (kvalues, vvalues, knumbers);
			 accumulatedRetrievedNumbers +=actualRetrievedKNumbers;
			 
			 LOG.info("==================actual number of the keys retrieved is: " + actualRetrievedKNumbers + "=======================");
			 
			 for (int i=0; i<actualRetrievedKNumbers; i++) {
				 LOG.info("retrieved k value: " + kvalues.get(i));
				 accumualtedKeys.add(kvalues.get(i));
				 
				 ArrayList<Object> tvvalues = vvalues.get(i);
				 for (int m=0; m<tvvalues.size(); m++) {
					 Object x = tvvalues.get(m);
					 Assert.assertTrue(x instanceof ApplicationTestClass); 
					 if (x instanceof ApplicationTestClass) {
						 
					    ApplicationTestClass y = (ApplicationTestClass) x; 
					    LOG.info("**" + " object: " + " page rank:" + y.getPageRank() 
					    		      + " url: " + y.getPageUrl() 
					    		      + " avg duration: " + y.getAvgDuration());
					   
					    
 					 }
					 
					  
				 }
			 }
			 
		 }
		 while (actualRetrievedKNumbers > 0);
		 
		 
		 Assert.assertEquals(accumulatedRetrievedNumbers, 5);
		 {
			int actuals[] = new int[accumulatedRetrievedNumbers];
			for (int i=0; i<accumulatedRetrievedNumbers; i++) {
				actuals[i] = accumualtedKeys.get(i); 
			}
			
			int expecteds[] = {0, 2, 4, 6, 8};
		    
		    Assert.assertArrayEquals(expecteds, actuals);
		 }
		 
		 reduceSHMShuffleStore.stop();
		 LOG.info("reduce shuffle store stopped");
		 
		 reduceSHMShuffleStore.shutdown();
		 LOG.info("reduce shuffle store shutdown");
		 
		 mapSHMShuffleStore.stop();
		 LOG.info("map shuffle store stopped");
		 
		 mapSHMShuffleStore.shutdown();
		 LOG.info("map shuffle store shutdown");
		 
		 ShuffleStoreManager.INSTANCE.shutdown();
		 LOG.info("shuffle store manager shutdown");
	 }

         //sort, without aggregation. 
         @Test
         public void testMapShuffleStoreWithIntKeysWithReduceIdTwoMergedArrayWithNoAggregation() {
	  
	     LOG.info("this is the test for mapShuffleStoreWithIntKeysWithReduceIdTwoMergedArrayTestWithNoAggregation");
	  
	     int executorId = 0; 
             ShuffleStoreManager.INSTANCE.initialize(
		 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
	     //to get a new heap instance for each new test case launched.
	     ShuffleStoreManager.INSTANCE.registershm();

	     long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
	     LOG.info("native pointer of shuffle store manager retrieved is:"
		  + "0x"+ Long.toHexString(nativePointer));
	  
	     //then start the map shuffele store manager 
	     Kryo mapSideKryo=new Kryo();
	     mapSideKryo.register(ApplicationTestClass.class);
	  
	     //create a direct bytebuffer:
	     int bufferSize = 1*1024*1024; // 1M bytes
	     ByteBuffer mapSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
	  
	     int shuffleId = 0;
	     int mapId1 = 0; 
	     int numberOfPartitions = 4;
	  
	     KValueTypeId keyType = KValueTypeId.Int; 
	  
	     int logicalThreadId1 =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();  
	     boolean ordering=true;
	     MapSHMShuffleStore mapSHMShuffleStore1 = 
	      ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer,
		 logicalThreadId1,
		 shuffleId, mapId1, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
	   
	     int mapId2 = 1; 
	     int logicalThreadId2 =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();
	     MapSHMShuffleStore mapSHMShuffleStore2 = 
	       ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer, 
			  logicalThreadId2,
			  shuffleId, mapId2, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
	    
	     Kryo reduceSideKryo=new Kryo();
	     reduceSideKryo.register(ApplicationTestClass.class);
	     ByteBuffer reduceSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
	    
	     //choose reduce that is not 0, or 1, or 7 (arbitrary number that is smaller than number of partitions)
	     int reduceId = 1; 
	     boolean aggregation = false; 
	     ReduceSHMShuffleStore reduceSHMShuffleStore = 
		ShuffleStoreManager.INSTANCE.createReduceShuffleStore(reduceSideKryo, reduceSideByteBuffer,
			      shuffleId, reduceId, numberOfPartitions, ordering, aggregation); 
	     
	     ShuffleDataModel.MapStatus mapStatusResult1 = null; 
	     {
		int numberOfVs = 2; 
		ArrayList<Object> testObjects = new ArrayList<Object> ();
		ArrayList<Integer> partitions = new ArrayList<Integer> ();
		ArrayList<Integer> kvalues = new ArrayList<Integer> ();
		 
		
		testObjects.add(1); 
		testObjects.add(2);
		kvalues.add(1);
		kvalues.add(1);
		partitions.add(1);
		partitions.add(1);
		 

		for (int i=0; i<numberOfVs; i++) {
		    mapSHMShuffleStore1.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
							i, KValueTypeId.Int.state);
		}
		 
		 
		// storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
		//ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
		int numberOfPairs = numberOfVs; 
		mapSHMShuffleStore1.storeKVPairs(numberOfPairs, KValueTypeId.Int.state);
		
		mapStatusResult1 = mapSHMShuffleStore1.sortAndStore();
		 
		LOG.info("map status region id: " + mapStatusResult1.getRegionIdOfIndexBucket());
		LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult1.getOffsetOfIndexBucket()));
		long[] buckets = mapStatusResult1.getMapStatus();
		 
		if (buckets != null) {
		    for (int i=0; i<buckets.length; i++) {
			if (buckets[i] > 0) {
			    LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
			}
		    }
		}
		else {
		    LOG.info("map status buckets length is null.");
		}
		 
		 
	    }
	     
	    ShuffleDataModel.MapStatus mapStatusResult2 = null;
	    {
		int numberOfVs = 2; 
		ArrayList<Object> testObjects = new ArrayList<Object> ();
		ArrayList<Integer> partitions = new ArrayList<Integer> ();
		ArrayList<Integer> kvalues = new ArrayList<Integer> ();
 
		 
		testObjects.add(3); 
		testObjects.add(1);
		kvalues.add(1);
		kvalues.add(2);
		partitions.add(1);
		partitions.add(0);
 
		for (int i=0; i<numberOfVs; i++) {
		    mapSHMShuffleStore2.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
							i, KValueTypeId.Int.state);
		}
		 
		// storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
		//ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
		int numberOfPairs = numberOfVs; 
		mapSHMShuffleStore2.storeKVPairs(numberOfPairs, KValueTypeId.Int.state);
		
		mapStatusResult2 = mapSHMShuffleStore2.sortAndStore();
		 
		LOG.info("map status region id: " + mapStatusResult2.getRegionIdOfIndexBucket());
		LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult2.getOffsetOfIndexBucket()));
		long[] buckets = mapStatusResult2.getMapStatus();
		 
		if (buckets != null) {
		    for (int i=0; i<buckets.length; i++) {
			if (buckets[i] > 0) {
			    LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
			}
		    }
		}
		else {
		    LOG.info("map status buckets length is null.");
		}
		 
		 
	    }
	     
	    {
		reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions, ordering, aggregation); 
		int mapIds[] = new int[2]; 
		mapIds[0] = mapId1;
		mapIds[1] = mapId2;
		 
		long shmRegionIds[]= new long [2];
		shmRegionIds[0] = mapStatusResult1.getRegionIdOfIndexBucket();
		shmRegionIds[1] = mapStatusResult2.getRegionIdOfIndexBucket();
		 
		long offsetToIndexChunks[] = new long[2];
		offsetToIndexChunks[0] = mapStatusResult1.getOffsetOfIndexBucket();
		offsetToIndexChunks[1] = mapStatusResult2.getOffsetOfIndexBucket();
		 
		long sizes[] = new long[2];
		sizes[0] = mapStatusResult1.getMapStatus()[reduceId]; //pick the first bucket; 
		sizes[1] = mapStatusResult2.getMapStatus()[reduceId]; //pick the first bucket; 
		 
		 ShuffleDataModel.ReduceStatus statuses =
		     new ShuffleDataModel.ReduceStatus(mapIds, shmRegionIds, offsetToIndexChunks, sizes);
		 //NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet. 
		 reduceSHMShuffleStore.mergeSort(statuses);
		  
	    }
	     
	    //to actually pull the data out 
	    {
		int knumbers = 14; 
		ArrayList<Integer> kvalues = new ArrayList<Integer>();
		ArrayList<Object> vvalues = new ArrayList<Object> (); 
		for (int i=0; i<knumbers; i++) {
		    kvalues.add(0); //initialization to 0; 
		    vvalues.add(null); //initialization to null;
		}
		 
		int actualRetrievedKNumbers= reduceSHMShuffleStore.getSimpleKVPairsWithIntKeys (kvalues, vvalues, knumbers);
		 
		LOG.info("actual number of the keys retrieved is: " + actualRetrievedKNumbers);
		 
		for (int i=0; i<actualRetrievedKNumbers; i++) {
		    LOG.info("retrieved k value: " + kvalues.get(i));
		    Object tvvalue = vvalues.get(i);
		     
		    {
			Object x = tvvalue;
			Assert.assertTrue(x instanceof Integer); 
			if (x instanceof Integer) {
			    LOG.info("**" + " retrieved object is: " + x);
			}
		    }
		}
		 
		 
	    }
	     
	    reduceSHMShuffleStore.stop();
	    LOG.info("reduce shuffle store stopped");
	     
	    reduceSHMShuffleStore.shutdown();
	    LOG.info("reduce shuffle store shutdown");
	     
	    mapSHMShuffleStore1.stop();
	    LOG.info("map shuffle1 store stopped");
	     
	    mapSHMShuffleStore1.shutdown();
	    LOG.info("map shuffle store shutdown");
	     
	    mapSHMShuffleStore2.stop();
	    LOG.info("map shuffle1 store stopped");
	     
	    mapSHMShuffleStore2.shutdown();
	    LOG.info("map shuffle store shutdown");
	     
	    ShuffleStoreManager.INSTANCE.shutdown();
	    LOG.info("shuffle store manager shutdown");
         }
     
         //sort, with aggregation.
	 @Test
	 public void testMapShuffleStoreWithIntKeysWithReduceIdTwoMergedArray() {
		 
		 LOG.info("this is the test for mapShuffleStoreWithIntKeysWithReduceIdTwoMergedArrayTest");
		 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(
				 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
		 //to get a new heap instance for each new test case launched.
		 ShuffleStoreManager.INSTANCE.registershm();

		 long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
		 LOG.info("native pointer of shuffle store manager retrieved is:"
		                + "0x"+ Long.toHexString(nativePointer));
		 
		 //then start the map shuffele store manager 
		 Kryo mapSideKryo=new Kryo();
		 mapSideKryo.register(ApplicationTestClass.class);
		 
		 //create a direct bytebuffer:
		 int bufferSize = 1*1024*1024; // 1M bytes
		 ByteBuffer mapSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 int shuffleId = 0;
		 int mapId1 = 0; 
		 int numberOfPartitions = 4;
		 
		 KValueTypeId keyType = KValueTypeId.Int; 
		 
		 int logicalThreadId1 =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();  
		 boolean ordering=true;
		 MapSHMShuffleStore mapSHMShuffleStore1 = 
				  ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer,
				  logicalThreadId1,
				  shuffleId, mapId1, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
		 
		 int mapId2 = 1; 
		 int logicalThreadId2 =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();
		 MapSHMShuffleStore mapSHMShuffleStore2 = 
				  ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer, 
				  logicalThreadId2,
				  shuffleId, mapId2, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
		 
		 Kryo reduceSideKryo=new Kryo();
		 reduceSideKryo.register(ApplicationTestClass.class);
		 ByteBuffer reduceSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 //choose reduce that is not 0, or 1, or 7 (arbitrary number that is smaller than number of partitions)
		 int reduceId = 1; 
		 ReduceSHMShuffleStore reduceSHMShuffleStore = 
				 ShuffleStoreManager.INSTANCE.createReduceShuffleStore(reduceSideKryo, reduceSideByteBuffer,
                 shuffleId, reduceId, numberOfPartitions, ordering, true); 
		 
		 ShuffleDataModel.MapStatus mapStatusResult1 = null; 
		 {
			 int numberOfVs = 2; 
			 ArrayList<Object> testObjects = new ArrayList<Object> ();
			 ArrayList<Integer> partitions = new ArrayList<Integer> ();
			 ArrayList<Integer> kvalues = new ArrayList<Integer> ();
	 
			
	 		 testObjects.add(1); 
	 		 testObjects.add(2);
	 		 kvalues.add(1);
	 		 kvalues.add(1);
	 		 partitions.add(1);
	 		 partitions.add(1);
	 		 
  
			 for (int i=0; i<numberOfVs; i++) {
			     mapSHMShuffleStore1.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
								 i, KValueTypeId.Int.state);
			 }
			 
			 // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
	                 //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
			 int numberOfPairs = numberOfVs; 
			 mapSHMShuffleStore1.storeKVPairs(numberOfPairs, KValueTypeId.Int.state);
			
			 mapStatusResult1 = mapSHMShuffleStore1.sortAndStore();
			 
			 LOG.info("map status region id: " + mapStatusResult1.getRegionIdOfIndexBucket());
			 LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult1.getOffsetOfIndexBucket()));
			 long[] buckets = mapStatusResult1.getMapStatus();
			 
			 if (buckets != null) {
				 for (int i=0; i<buckets.length; i++) {
					 if (buckets[i] > 0) {
					    LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
					 }
				 }
			 }
			 else {
				 LOG.info("map status buckets length is null.");
			 }
			 
		 
		 }
		 
		 ShuffleDataModel.MapStatus mapStatusResult2 = null;
		 {
			 int numberOfVs = 2; 
			 ArrayList<Object> testObjects = new ArrayList<Object> ();
			 ArrayList<Integer> partitions = new ArrayList<Integer> ();
			 ArrayList<Integer> kvalues = new ArrayList<Integer> ();
 
	 		 
	 		 testObjects.add(3); 
	 		 testObjects.add(1);
	 		 kvalues.add(1);
	 		 kvalues.add(2);
	 		 partitions.add(1);
	 		 partitions.add(0);
 
			 for (int i=0; i<numberOfVs; i++) {
			     mapSHMShuffleStore2.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
								 i, KValueTypeId.Int.state);
			 }
			 
			 // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
	                 //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
			 int numberOfPairs = numberOfVs; 
			 mapSHMShuffleStore2.storeKVPairs(numberOfPairs, KValueTypeId.Int.state);
			
			 mapStatusResult2 = mapSHMShuffleStore2.sortAndStore();
			 
			 LOG.info("map status region id: " + mapStatusResult2.getRegionIdOfIndexBucket());
			 LOG.info ("map status offset to index chunk: 0x " + Long.toHexString(mapStatusResult2.getOffsetOfIndexBucket()));
			 long[] buckets = mapStatusResult2.getMapStatus();
			 
			 if (buckets != null) {
				 for (int i=0; i<buckets.length; i++) {
					 if (buckets[i] > 0) {
					    LOG.info("map status, bucket: " + i + " has size: " + buckets[i]);
					 }
				 }
			 }
			 else {
				 LOG.info("map status buckets length is null.");
			 }
			 
		 
		 }
		 
		 {
			 reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions, ordering, true); 
			 int mapIds[] = new int[2]; 
			 mapIds[0] = mapId1;
			 mapIds[1] = mapId2;
			 
			 long shmRegionIds[]= new long [2];
			 shmRegionIds[0] = mapStatusResult1.getRegionIdOfIndexBucket();
			 shmRegionIds[1] = mapStatusResult2.getRegionIdOfIndexBucket();
			 
			 long offsetToIndexChunks[] = new long[2];
			 offsetToIndexChunks[0] = mapStatusResult1.getOffsetOfIndexBucket();
			 offsetToIndexChunks[1] = mapStatusResult2.getOffsetOfIndexBucket();
			 
			 long sizes[] = new long[2];
			 sizes[0] = mapStatusResult1.getMapStatus()[reduceId]; //pick the first bucket; 
			 sizes[1] = mapStatusResult2.getMapStatus()[reduceId]; //pick the first bucket; 
			 
			 ShuffleDataModel.ReduceStatus statuses =
					 new ShuffleDataModel.ReduceStatus(mapIds, shmRegionIds, offsetToIndexChunks, sizes);
			 //NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet. 
			 reduceSHMShuffleStore.mergeSort(statuses);
			 
		 }
		 
		 //to actually pull the data out 
		 {
			 int knumbers = 6; 
			 ArrayList<Integer> kvalues = new ArrayList<Integer>();
			 ArrayList<ArrayList<Object>> vvalues = new ArrayList<ArrayList<Object>> (); 
			 for (int i=0; i<knumbers; i++) {
				 kvalues.add(0); //initialization to 0; 
				 vvalues.add(null); //initialization to null;
			 }
			 
			 int actualRetrievedKNumbers= reduceSHMShuffleStore.getKVPairsWithIntKeys (kvalues, vvalues, knumbers);
			 
			 LOG.info("actual number of the keys retrieved is: " + actualRetrievedKNumbers);
			 
			 for (int i=0; i<actualRetrievedKNumbers; i++) {
				 LOG.info("retrieved k value: " + kvalues.get(i));
				 ArrayList<Object> tvvalues = vvalues.get(i);
				 Assert.assertEquals(tvvalues.size(), 3); //each k now has two values to be associated. 
				 
				 for (int m=0; m<tvvalues.size(); m++) {
					 Object x = tvvalues.get(m);
					 Assert.assertTrue(x instanceof Integer); 
					 if (x instanceof Integer) {
					    LOG.info("**" + " retrieved object is: " + x);
					 }
				 }
			 }
			 
			 
		 }
		 
		 reduceSHMShuffleStore.stop();
		 LOG.info("reduce shuffle store stopped");
		 
		 reduceSHMShuffleStore.shutdown();
		 LOG.info("reduce shuffle store shutdown");
		 
		 mapSHMShuffleStore1.stop();
		 LOG.info("map shuffle1 store stopped");
		 
		 mapSHMShuffleStore1.shutdown();
		 LOG.info("map shuffle store shutdown");
		 
		 mapSHMShuffleStore2.stop();
		 LOG.info("map shuffle1 store stopped");
		 
		 mapSHMShuffleStore2.shutdown();
		 LOG.info("map shuffle store shutdown");
		 
		 ShuffleStoreManager.INSTANCE.shutdown();
		 LOG.info("shuffle store manager shutdown");
	 }
	 
	 
	 //NOTE: to test how the buffer gets re-used in different batches.
	 
	 @Override
	 protected void tearDown() throws Exception{ 
		 //do something first;
		 LOG.info("shm region:" + GLOBAL_HEAP_NAME + " to be formated");
		 ShuffleStoreManager.INSTANCE.formatshm(); 
		 
		 super.tearDown();
	 }
	 
	 public static void main(String[] args) {
		  //NOTE: for some reason the annotation does not work for @Test and @Ignore. Instead, the only
		  //thing work is the method name started with "test" to be the test methods. 
	      junit.textui.TestRunner.run(SortBasedReduceSHMShuffleStoreWithIntKeysTest.class);
	 }
}



