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

import java.nio.ByteBuffer;
import com.esotericsoftware.kryo.Kryo;
import com.hp.hpl.firesteel.shuffle.ShuffleDataModel.KValueTypeId;

import java.util.ArrayList; 

public class  SortBasedReduceSHMShuffleStoreWithByteArrayKeysTest extends TestCase {

	 private static final Logger LOG =
			 LoggerFactory.getLogger(SortBasedReduceSHMShuffleStoreWithByteArrayKeysTest.class.getName());
	 private static final int SIZE_OF_BATCH_SERIALIZATION = 100; 
	 
	 //the global heap name created via RMB. 
	 private static final String GLOBAL_HEAP_NAME = "/dev/shm/nvm/global0";
	
	 
	 public static class EncapsulatedByteArrayClass{
	      byte[] content;
	 }
	 
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
	  * NOTE: this test is to show how string and bytes can be converted back and forth. 
	  */
	 @Test
	 public void testStringByteConversion() {
		 LOG.info("this is the testStringByteConversion");
		 
		 String svalue = new String ("good1");
		 
		 byte[] bytes = svalue.getBytes();
		 
		 LOG.info ("the length of the string in bytes is:" + bytes.length);
		 LOG.info ("the string in bytes is:" + MapSHMShuffleStore.getHex(bytes));
		 
		 String backValue = new String (bytes);
		 
		 LOG.info("the converted back string is: " + backValue);
	 }
	 
	 /**
	  * NOTE: this test is to test how big each value object should be after the kryo-serialization. 
	  */
	 @Test
	 public void testStatusFromMapShuffleStoreWithByteArrayKeysWithReduceId0WithClassRegistrationOnlyShowValueSerialization(){
		 LOG.info("this is the test for testStatusFromMapShuffleStoreWithByteArrayKeysWithReduceId0WithClassRegistrationOnlyShowValueSerialization");
		 
		 int maxNumberOfTaskThreads = 30; 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(GLOBAL_HEAP_NAME , maxNumberOfTaskThreads, executorId);
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
		 
		 KValueTypeId keyType = KValueTypeId.ByteArray; 
		 

		 boolean ordering=true;
		 int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();
		 MapSHMShuffleStore mapSHMShuffleStore = 
				  ShuffleStoreManager.INSTANCE.createMapShuffleStore(mapSideKryo, mapSideByteBuffer, 
				  logicalThreadId,
				  shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
		 
		 Kryo reduceSideKryo=new Kryo();
		 reduceSideKryo.register(ApplicationTestClass.class);
		 
		 {
			 int numberOfVs = 10; 
			 ArrayList<Object> testObjects = new ArrayList<Object> ();
			 ArrayList<Integer> partitions = new ArrayList<Integer> ();
			 ArrayList<byte[]> kvalues = new ArrayList<byte[]> ();
	 		 
			 for (int i=0; i<numberOfVs; i++) {
				  String  v = new String ("good" + i);
				  kvalues.add(v.getBytes());
				  ApplicationTestClass obj = new ApplicationTestClass (i, "hello" +i,  i+1); 
				  testObjects.add(obj);
			 
				  partitions.add(i%2);
			 }
			 
			 for (int i=0; i<numberOfVs; i++) {
			     mapSHMShuffleStore.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
								i, KValueTypeId.ByteArray.state);
			 }
		 }
	 }
	 
	 /**
	  * NOTE: this test also include the serialization and de-serialization of value class. With class registration. 
	  */
	 @Test
	 public void testStatusFromMapShuffleStoreWithByteArrayKeysWithReduceId0WithClassRegistration() {
		 
		 LOG.info("this is the test for testStatusFromMapShuffleStoreWithByteArrayKeysWithReduceId0WithClassRegistration");
		 
		 int maxNumberOfTaskThreads = 30; 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(GLOBAL_HEAP_NAME , maxNumberOfTaskThreads, executorId);
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
		 
		 KValueTypeId keyType = KValueTypeId.ByteArray; 
		 

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
			 ArrayList<byte[]> kvalues = new ArrayList<byte[]> ();
	 		 
			 for (int i=0; i<numberOfVs; i++) {
				  String  v = new String ("good" + i);
				  kvalues.add(v.getBytes());
				  ApplicationTestClass obj = new ApplicationTestClass (i, "hello" +i,  i+1); 
				  testObjects.add(obj);
			 
				  partitions.add(i%2);
			 }
			 
			 for (int i=0; i<numberOfVs; i++) {
			     mapSHMShuffleStore.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
								i, KValueTypeId.ByteArray.state);
			 }

			 // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
	                 //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
			 int numberOfPairs = numberOfVs; 
			 mapSHMShuffleStore.storeKVPairs(numberOfPairs, KValueTypeId.ByteArray.state);
			
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
			 ArrayList<byte[]> kvalues = new ArrayList<byte[]>();
			 ArrayList<ArrayList<Object>> vvalues = new ArrayList<ArrayList<Object>> (); 
			 for (int i=0; i<knumbers; i++) {
				 kvalues.add(null); //initialization to 0; 
				 vvalues.add(null); //initialization to null;
			 }
			 
			 //ArrayList<byte[]> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers
			 int actualRetrievedKNumbers= reduceSHMShuffleStore.getKVPairsWithByteArrayKeys (kvalues, vvalues, knumbers);
			 
			 LOG.info("actual number of the keys retrieved is: " + actualRetrievedKNumbers);
			 
			 for (int i=0; i<actualRetrievedKNumbers; i++) {
				 LOG.info("retrieved k value " +  " with length of: " + kvalues.get(i).length 
						          + " and value: " +  MapSHMShuffleStore.getHex(kvalues.get(i)));
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
					    //Assert.assertEquals(y.getPageRank(), kvalues.get(i).intValue());
					    //Assert.assertEquals(y.getPageUrl(), new String(kvalues.get(i)));
					   // Assert.assertEquals(y.getAvgDuration(), kvalues.get(i).intValue() + 1);
 					 }
					 
					  
				 }
			 }
			 
			 Assert.assertEquals(actualRetrievedKNumbers, 5);
			 {
				EncapsulatedByteArrayClass actuals[] = new EncapsulatedByteArrayClass[actualRetrievedKNumbers];
				for (int i=0; i<actualRetrievedKNumbers; i++) {
					actuals[i] = new EncapsulatedByteArrayClass();
					actuals[i].content = kvalues.get(i); 
				}
				
				String expecteds[] = {"good0", "good2", "good4", "good6", "good8"};
			    
				for (int i=0; i<actualRetrievedKNumbers; i++) {
			       Assert.assertEquals(expecteds[i], new String(actuals[i].content));
				}
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
	  * NOTE: this test also include the serialization and de-serialization of value class. With class registration. 
	  */
	 @Test
	 public void testStatusFromMapShuffleStoreWithByteArrayKeysWithReduceId0WithoutClassRegistration() {
		 
		 LOG.info("this is the test for testStatusFromMapShuffleStoreWithByteArrayKeysWithReduceId0WithoutClassRegistration");
		 
		 int maxNumberOfTaskThreads = 30; 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(GLOBAL_HEAP_NAME , maxNumberOfTaskThreads, executorId);
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
		 
		 KValueTypeId keyType = KValueTypeId.ByteArray; 
		 
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
			 ArrayList<byte[]> kvalues = new ArrayList<byte[]> ();
 
			 for (int i=0; i<numberOfVs; i++) {
				 String  v = new String ("hello" + i);
				  kvalues.add(v.getBytes());
				  ApplicationTestClass obj = new ApplicationTestClass (i, "hello" +i,  i+1); 
				  testObjects.add(obj);
				  partitions.add(i%2);
				 
			 }

			 for (int i=0; i<numberOfVs; i++) {
			     mapSHMShuffleStore.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
								i, KValueTypeId.ByteArray.state);
			 }
			 
			 
			 // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
	                 //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
			 int numberOfPairs = numberOfVs; 
			 mapSHMShuffleStore.storeKVPairs(numberOfPairs, KValueTypeId.ByteArray.state);
			
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
			 ArrayList<byte[]> kvalues = new ArrayList<byte[]>();
			 ArrayList<ArrayList<Object>> vvalues = new ArrayList<ArrayList<Object>> (); 
			 for (int i=0; i<knumbers; i++) {
				 kvalues.add(null); //initialization to 0; 
				 vvalues.add(null); //initialization to null;
			 }
			 
			 int actualRetrievedKNumbers= reduceSHMShuffleStore.getKVPairsWithByteArrayKeys (kvalues, vvalues, knumbers);
			 
			 LOG.info("actual number of the keys retrieved is: " + actualRetrievedKNumbers);
			 
			 for (int i=0; i<actualRetrievedKNumbers; i++) {
				 LOG.info("retrieved k value (converted to String): " + new String(kvalues.get(i)));
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
					    //Assert.assertEquals(y.getPageRank(), kvalues.get(i).intValue());
					    Assert.assertEquals(y.getPageUrl(), new String(kvalues.get(i)));
					    //Assert.assertEquals(y.getAvgDuration(), kvalues.get(i).intValue() + 1);
 					 }
					 
					  
				 }
			 }
			 
			 Assert.assertEquals(actualRetrievedKNumbers, 5);
			 {
				EncapsulatedByteArrayClass actuals[] = new EncapsulatedByteArrayClass[actualRetrievedKNumbers];
				for (int i=0; i<actualRetrievedKNumbers; i++) {
					actuals[i] = new EncapsulatedByteArrayClass();
					actuals[i].content = kvalues.get(i); 
				}
				
				String expecteds[] = {"hello0", "hello2", "hello4", "hello6", "hello8"};
			    
				for (int i=0; i<actualRetrievedKNumbers; i++) {
				       Assert.assertEquals(expecteds[i], new String(actuals[i].content));
			    }
			 
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
	 public void testMapStatusFromMapShuffleStoreWithByteArrayKeysWithReduceId1() {
		 
		 LOG.info("this is the test for testMapStatusFromMapShuffleStoreWithByteArrayKeysWithReduceId1");
		 
		 int maxNumberOfTaskThreads = 30; 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(GLOBAL_HEAP_NAME, maxNumberOfTaskThreads, executorId);
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
		 
		 KValueTypeId keyType = KValueTypeId.ByteArray; 
		 
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
			 ArrayList<byte[]> kvalues = new ArrayList<byte[]> ();
	 
			 for (int i=0; i<numberOfVs; i++) {
				  
				  String  v = new String ("hello" + i);
				  kvalues.add(v.getBytes());
				  ApplicationTestClass obj = new ApplicationTestClass (i, "hello" +i,  i+1); 
				  testObjects.add(obj);
			 
				  partitions.add(i%2);
			 }
			 
			 for (int i=0; i<numberOfVs; i++) {
			     mapSHMShuffleStore.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
								i, KValueTypeId.ByteArray.state);
			 }

			 // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
	                 //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
			 int numberOfPairs = numberOfVs; 
			 mapSHMShuffleStore.storeKVPairs(numberOfPairs, KValueTypeId.ByteArray.state);
			
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
			 ArrayList<byte[]> kvalues = new ArrayList<byte[]>();
			 ArrayList<ArrayList<Object>> vvalues = new ArrayList<ArrayList<Object>> (); 
			 for (int i=0; i<knumbers; i++) {
				 kvalues.add(null); //initialization to 0; 
				 vvalues.add(null); //initialization to null;
			 }
			 
			 int actualRetrievedKNumbers= reduceSHMShuffleStore.getKVPairsWithByteArrayKeys(kvalues, vvalues, knumbers);
			 
			 LOG.info("actual number of the keys retrieved is: " + actualRetrievedKNumbers);
			 
			 for (int i=0; i<actualRetrievedKNumbers; i++) {
				 LOG.info("retrieved k value (converted to String): " + new String(kvalues.get(i)));
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
					    //Assert.assertEquals(y.getPageRank(), kvalues.get(i).intValue());
					    Assert.assertEquals(y.getPageUrl(), new String(kvalues.get(i)));
					    //Assert.assertEquals(y.getAvgDuration(), kvalues.get(i).intValue() + 1);
 					 }
					 
					  
				 }
			 }
			 
			 Assert.assertEquals(actualRetrievedKNumbers, 5);
			 {
				EncapsulatedByteArrayClass actuals[] = new EncapsulatedByteArrayClass[actualRetrievedKNumbers];
				for (int i=0; i<actualRetrievedKNumbers; i++) {
					actuals[i] = new EncapsulatedByteArrayClass();
					actuals[i].content = kvalues.get(i);
				}
				
				String expecteds[] = {"hello1", "hello3", "hello5", "hello7", "hello9"};
			    
				for (int i=0; i<actualRetrievedKNumbers; i++) {
			                Assert.assertEquals(expecteds[i], new String(actuals[i].content)); 
				}
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
	 public void testMapStatusFromMapShuffleStoreWithByteArrayKeysWithReduceId96() {
		 
		 LOG.info("this is the test for testMapStatusFromMapShuffleStoreWithByteArrayKeysWithReduceId96");
		 
		 int maxNumberOfTaskThreads = 30; 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(GLOBAL_HEAP_NAME, maxNumberOfTaskThreads, executorId);
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
		 
		 KValueTypeId keyType = KValueTypeId.ByteArray; 
		 
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
			 ArrayList<byte[]> kvalues = new ArrayList<byte[]> ();
	 
			 for (int i=0; i<numberOfVs; i++) {
				  String  v = new String ("good" + i);
				  kvalues.add(v.getBytes());
				  
				  ApplicationTestClass obj = new ApplicationTestClass (i, "hello" +i,  i+1); 
				  testObjects.add(obj);
				  partitions.add(i%2);
				  
			 }
			 
			 for (int i=0; i<numberOfVs; i++) {
			     mapSHMShuffleStore.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
								i, KValueTypeId.ByteArray.state);
			 }
			 
			 // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
	                 //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
			 int numberOfPairs = numberOfVs; 
			 mapSHMShuffleStore.storeKVPairs(numberOfPairs, KValueTypeId.ByteArray.state);
			
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
			 ArrayList<byte[]> kvalues = new ArrayList<byte[]>();
			 ArrayList<ArrayList<Object>> vvalues = new ArrayList<ArrayList<Object>> (); 
			 for (int i=0; i<knumbers; i++) {
				 kvalues.add(null); //initialization to 0; 
				 vvalues.add(null); //initialization to null;
			 }
			 
			 int actualRetrievedKNumbers= reduceSHMShuffleStore.getKVPairsWithByteArrayKeys (kvalues, vvalues, knumbers);
			 
			 LOG.info("actual number of the keys retrieved is: " + actualRetrievedKNumbers);
			 
			 for (int i=0; i<actualRetrievedKNumbers; i++) {
				 LOG.info("retrieved k value (converted to String): " + new String(kvalues.get(i)));
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
					    //Assert.assertEquals(y.getPageRank(), kvalues.get(i).intValue());
					    Assert.assertEquals(y.getPageUrl(), new String(kvalues.get(i)));
					    //Assert.assertEquals(y.getAvgDuration(), kvalues.get(i).intValue() + 1);
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
	 public void testMapStatusFromMapShuffleStoreWithByteArrayKeysWithReduceIdTwoMerged() {
		 
		 LOG.info("this is the test for testMapStatusFromMapShuffleStoreWithByteArrayKeysWithReduceIdTwoMerged");
		 
		 int maxNumberOfTaskThreads = 30; 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(GLOBAL_HEAP_NAME, maxNumberOfTaskThreads, executorId);
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
		 
		 KValueTypeId keyType = KValueTypeId.ByteArray; 
		 
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
			 ArrayList<byte[]> kvalues = new ArrayList<byte[]> ();
	 	 
			 for (int i=0; i<numberOfVs; i++) {
				  String  v = new String ("hello" + i);
				  kvalues.add(v.getBytes());
				  
				  ApplicationTestClass obj = new ApplicationTestClass (i, "hello" +i,  i+1); 
				  testObjects.add(obj);
		 
				  partitions.add(i%2);
				 
			 }
			 
			 for (int i=0; i<numberOfVs; i++) {
			     mapSHMShuffleStore1.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
								 i, KValueTypeId.ByteArray.state);
			 }
			  
			 
			 // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
	                 //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
			 int numberOfPairs = numberOfVs; 
			 mapSHMShuffleStore1.storeKVPairs(numberOfPairs, KValueTypeId.ByteArray.state);
			
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
			 ArrayList<byte[]> kvalues = new ArrayList<byte[]> ();
	 		 ArrayList<Integer> voffsets = new ArrayList<Integer> ();
			 for (int i=0; i<numberOfVs; i++) {
				  String  v = new String ("hello" + i);
				  kvalues.add(v.getBytes());
				  
				  ApplicationTestClass obj = new ApplicationTestClass (i, "hello" +i,  i+1); 
				  testObjects.add(obj);
				  partitions.add(i%2);
			 }
			 
			 for (int i=0; i<numberOfVs; i++) {
			     mapSHMShuffleStore2.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
								 i, KValueTypeId.ByteArray.state);
			 }
			 
			 // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
	                 //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
			 int numberOfPairs = numberOfVs; 
			 mapSHMShuffleStore2.storeKVPairs(numberOfPairs, KValueTypeId.ByteArray.state);
			
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
			 ArrayList<byte[]> kvalues = new ArrayList<byte[]>();
			 ArrayList<ArrayList<Object>> vvalues = new ArrayList<ArrayList<Object>> (); 
			 for (int i=0; i<knumbers; i++) {
				 kvalues.add(null); //initialization to 0; 
				 vvalues.add(null); //initialization to null;
			 }
			 
			 int actualRetrievedKNumbers= reduceSHMShuffleStore.getKVPairsWithByteArrayKeys (kvalues, vvalues, knumbers);
			 
			 LOG.info("actual number of the keys retrieved is: " + actualRetrievedKNumbers);
			 
			 for (int i=0; i<actualRetrievedKNumbers; i++) {
				 LOG.info("retrieved k value (converted to String): " + new String(kvalues.get(i)));
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
					    //Assert.assertEquals(y.getPageRank(), kvalues.get(i).intValue());
					    Assert.assertEquals(y.getPageUrl(), new String(kvalues.get(i)));
					    //Assert.assertEquals(y.getAvgDuration(), kvalues.get(i).intValue() + 1);
 					 }
					 
					  
				 }
			 }
			 
			 Assert.assertEquals(actualRetrievedKNumbers, 5);
			 {
				EncapsulatedByteArrayClass actuals[] = new EncapsulatedByteArrayClass[actualRetrievedKNumbers];
			 
				for (int i=0; i<actualRetrievedKNumbers; i++) {
					actuals[i] = new EncapsulatedByteArrayClass();
					actuals[i].content = kvalues.get(i);  
				}
				
				String expecteds[] = {"hello0", "hello2", "hello4", "hello6", "hello8"};
				for (int i=0; i<actualRetrievedKNumbers; i++) {
			        Assert.assertEquals(expecteds[i], new String(actuals[i].content));
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
	 
 
	 @Test
	 public void testMapStatusFromMapShuffleStoreWithByteArrayKeysWithReduceId0RetrievedMultipleTimes() {
		 
		 LOG.info("this is the test for testMapStatusFromMapShuffleStoreWithByteArrayKeysWithReduceId0RetrievedMultipleTimes");
		 
		 int maxNumberOfTaskThreads = 30; 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(GLOBAL_HEAP_NAME, maxNumberOfTaskThreads, executorId);
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
		 
		 KValueTypeId keyType = KValueTypeId.ByteArray; 
		 

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
			 ArrayList<byte[]> kvalues = new ArrayList<byte[]> ();
	 	 
			 for (int i=0; i<numberOfVs; i++) {
				  String  v = new String ("hello" + i);
				  kvalues.add(v.getBytes());
				  ApplicationTestClass obj = new ApplicationTestClass (i, "hello" +i,  i+1); 
				  testObjects.add(obj);
 
				  partitions.add(i%2);
			 }
			 
			 for (int i=0; i<numberOfVs; i++) {
			     mapSHMShuffleStore.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
								i, KValueTypeId.ByteArray.state);
			 }
			 
			 // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
	                 //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
			 int numberOfPairs = numberOfVs; 
			 mapSHMShuffleStore.storeKVPairs(numberOfPairs, KValueTypeId.ByteArray.state);
			
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
		 ArrayList<byte[]> accumualtedKeys = new ArrayList<byte[]> ();
		 do 
		 {
			 int knumbers = 2; 
			 ArrayList<byte[]> kvalues = new ArrayList<byte[]>();
			 ArrayList<ArrayList<Object>> vvalues = new ArrayList<ArrayList<Object>> (); 
			 for (int i=0; i<knumbers; i++) {
				 kvalues.add(null); //initialization to 0; 
				 vvalues.add(null); //initialization to null;
			 }
			 
			 actualRetrievedKNumbers= reduceSHMShuffleStore.getKVPairsWithByteArrayKeys (kvalues, vvalues, knumbers);
			 accumulatedRetrievedNumbers +=actualRetrievedKNumbers;
			 
			 LOG.info("==================actual number of the keys retrieved is: " + actualRetrievedKNumbers + "=======================");
			 
			 for (int i=0; i<actualRetrievedKNumbers; i++) {
				 LOG.info("retrieved k value (converted to String): " + new String(kvalues.get(i)));
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
			 EncapsulatedByteArrayClass actuals[] = new EncapsulatedByteArrayClass[accumulatedRetrievedNumbers];
			for (int i=0; i<accumulatedRetrievedNumbers; i++) {
				actuals[i] = new EncapsulatedByteArrayClass();
				actuals[i].content = accumualtedKeys.get(i); 
			}
			
			String expecteds[] = {"hello0", "hello2", "hello4", "hello6", "hello8"};
		    
			for (int i=0; i<accumulatedRetrievedNumbers; i++) {
		      Assert.assertEquals(expecteds[i], new String(actuals[i].content));
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
	 public void testMapShuffleStoreWithByteArrayKeysWithReduceIdTwoMergedArray() {
		 
		 LOG.info("this is the test for testMapShuffleStoreWithByteArrayKeysWithReduceIdTwoMergedArray");
		 
		 int maxNumberOfTaskThreads = 30; 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(GLOBAL_HEAP_NAME, maxNumberOfTaskThreads, executorId);
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
		 
		 KValueTypeId keyType = KValueTypeId.ByteArray; 
		 
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
			 ArrayList<byte[]> kvalues = new ArrayList<byte[]> ();
	 
			
	 		 testObjects.add(1); 
	 		 testObjects.add(2);
	 		 kvalues.add(new String("hello1").getBytes());
	 		 kvalues.add(new String("hello1").getBytes());
	 		 partitions.add(1);
	 		 partitions.add(1);
	 		 
  
			 for (int i=0; i<numberOfVs; i++) {
			     mapSHMShuffleStore1.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
								 i, KValueTypeId.ByteArray.state);
			 }

			 // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
	                 //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
			 int numberOfPairs = numberOfVs; 
			 mapSHMShuffleStore1.storeKVPairs(numberOfPairs, KValueTypeId.ByteArray.state); 
			
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
			 ArrayList<byte[]> kvalues = new ArrayList<byte[]> ();
 
	 		 
	 		 testObjects.add(3); 
	 		 testObjects.add(1);
	 		 kvalues.add(new String("hello1").getBytes());
	 		 kvalues.add(new String("hello2").getBytes());
	 		 partitions.add(1);
	 		 partitions.add(0);
 
			 for (int i=0; i<numberOfVs; i++) {
			     mapSHMShuffleStore2.serializeKVPair(kvalues.get(i), testObjects.get(i), partitions.get(i),
								 i, KValueTypeId.ByteArray.state);
			 }
			 
			 // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
	                 //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
			 int numberOfPairs = numberOfVs; 
			 mapSHMShuffleStore2.storeKVPairs(numberOfPairs, KValueTypeId.ByteArray.state);
			
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
			 ArrayList<byte[]> kvalues = new ArrayList<byte[]>();
			 ArrayList<ArrayList<Object>> vvalues = new ArrayList<ArrayList<Object>> (); 
			 for (int i=0; i<knumbers; i++) {
				 kvalues.add(null); //initialization to 0; 
				 vvalues.add(null); //initialization to null;
			 }
			 
			 int actualRetrievedKNumbers= reduceSHMShuffleStore.getKVPairsWithByteArrayKeys (kvalues, vvalues, knumbers);
			 
			 LOG.info("actual number of the keys retrieved is: " + actualRetrievedKNumbers);
			 
			 for (int i=0; i<actualRetrievedKNumbers; i++) {
				 LOG.info("retrieved k value (converted to String): " + new String(kvalues.get(i)));
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
	      junit.textui.TestRunner.run(SortBasedReduceSHMShuffleStoreWithByteArrayKeysTest.class);
	 }
}



