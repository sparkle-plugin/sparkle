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


import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Assert;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.hp.hpl.firesteel.shuffle.SortBasedMapSHMShuffleStoreWithIntKeysTest.ApplicationTestClass;
import com.hp.hpl.firesteel.shuffle.ShuffleDataModel.KValueTypeId;
import com.hp.hpl.firesteel.shuffle.ThreadLocalShuffleResourceHolder.ShuffleResource;
import com.hp.hpl.firesteel.shuffle.ThreadLocalShuffleResourceHolder.ReusableSerializationResource;
import junit.framework.TestCase;
 

public class ShuffleStoreTrackerTest extends TestCase {

	 private static final int SIZE_OF_BATCH_SERIALIZATION = 100; 
	 //the global heap name created via RMB. 
	 private static final String GLOBAL_HEAP_NAME = "/dev/shm/nvm/global0";
	 
	 
	 private static final Logger LOG = LoggerFactory.getLogger(ShuffleStoreTrackerTest.class.getName());

	 @Override
	 protected void setUp() throws Exception{ 
		 super.setUp();
		 
	 }
	 
	 
	 /**
	  *to test how shuffle resource tracker behaves, for a single map-shuffle-store creation and then shutdown. 
	  */
	 @Test
	 public void testOneSingleMapStoreStartShutdownShuffleStoreTracker() {
		 LOG.info("this is the test for oneSingleMapStoreStartShutdownShuffleStoreTrackerTest");
		 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(
				 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
		 //to get a new heap instance for each new test case launched.
		 ShuffleStoreManager.INSTANCE.registershm();
		 //Can we make format immediately after the initialization?
		 //NOTE: shm region can only be formated after the region gets closed, 
		 //that happens when the shuffle store manager gets shutdown. 
		 //ShuffleStoreManager.INSTANCE.formatshm(); 
		 
		 LOG.info("after shm formating in oneSingleMapStoreStartShutdownShuffleStoreTrackerTest");
		 
		 int shuffleId = 0;
		 MapSHMShuffleStore mapSHMShuffleStore  = null;
		 
		 {
		         ThreadLocalShuffleResourceHolder resourceHolder= new ThreadLocalShuffleResourceHolder();
                         ShuffleResource shuffleResource =resourceHolder.getResource();
                         if (shuffleResource == null) {

			    //then start the map shuffle store manager. 
                            //NOTE: in Java, we will have to de-couple its dependency to Spark core in Scala. 
			    Kryo kryo=new Kryo();
			    kryo.register(ApplicationTestClass.class);
			 
			    //create a direct bytebuffer:
			    int bufferSize = 1*1024*1024; // 1M bytes
			    ByteBuffer byteBuffer =  ByteBuffer.allocateDirect(bufferSize);
			    int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();			 

			    shuffleResource = new ShuffleResource(
						 new ReusableSerializationResource (kryo, byteBuffer),
						 logicalThreadId);
			    //push to the thread specific storage for future retrieval in the same task execution.
			    resourceHolder.initialize (shuffleResource);

			 }

			 int mapId = 1; 
			 int numberOfPartitions = 100;
			 
			 KValueTypeId keyType = KValueTypeId.Int; 
			 

			 boolean ordering =true;
			 mapSHMShuffleStore = 
					  ShuffleStoreManager.INSTANCE.createMapShuffleStore(
					     shuffleResource.getSerializationResource().getKryoInstance(), 
                                             shuffleResource.getSerializationResource().getByteBuffer(),
                                             shuffleResource.getLogicalThreadId(),
					     shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
			 
			 mapSHMShuffleStore.stop();
			 
			 //NOTE: is that the same for MapShuffleStore, cleanup and shutdown are identical.
			 mapSHMShuffleStore.shutdown();
		 
		 }
		 
		 //now if we retrieve the shuffle store tracker, we should have one entry.
		 ShuffleStoreTracker tracker = ShuffleStoreManager.INSTANCE.getShuffleStoreTracker();
		 ArrayList<MapSHMShuffleStore> mapShuffleStores =  tracker.getMapShuffleStores(shuffleId);
		 
		 Assert.assertEquals (mapShuffleStores.size(), 1);
		 Assert.assertEquals (mapShuffleStores.get(0).getStoreId(), mapSHMShuffleStore.getStoreId()); 
		 
			 //then issue clean up
		 ShuffleStoreManager.INSTANCE.cleanup(shuffleId);
		 mapShuffleStores = tracker.getMapShuffleStores(shuffleId);
		 Assert.assertEquals (mapShuffleStores, null);
		 
		 
		 ShuffleStoreManager.INSTANCE.shutdown();
	 }
	 
	 

	 /**
	  * to test how shuffle resource tracker behaves, for ten map-shuffle-stores' creation and then shutdown. 
	  */
	 @Test
	 public void testTenSingleMapStoreStartShutdownShuffleStoreTracker() {
		 LOG.info("this is the test for tenSingleMapStoreStartShutdownShuffleStoreTrackerTest");
	 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(
				 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
		 //to get a new heap instance for each new test case launched.
		 ShuffleStoreManager.INSTANCE.registershm();
		 
		 int shuffleId = 0;
		 MapSHMShuffleStore mapSHMShuffleStore  = null;
		 int counter= 10;
		 
		 for (int i=0; i<counter;i++) {

		         ThreadLocalShuffleResourceHolder resourceHolder= new ThreadLocalShuffleResourceHolder();
                         ShuffleResource shuffleResource = resourceHolder.getResource();

                         if (shuffleResource == null) {

			    //then start the map shuffle store manager. 
                            //NOTE: in Java, we will have to de-couple its dependency to Spark core in Scala. 
			    Kryo kryo=new Kryo();
			    kryo.register(ApplicationTestClass.class);
			 
			    //create a direct bytebuffer:
			    int bufferSize = 1*1024*1024; // 1M bytes
			    ByteBuffer byteBuffer =  ByteBuffer.allocateDirect(bufferSize);
			    int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();			 

			    shuffleResource = new ShuffleResource(
						 new ReusableSerializationResource (kryo, byteBuffer),
						 logicalThreadId);
			    //push to the thread specific storage for future retrieval in the same task execution.
			    resourceHolder.initialize (shuffleResource);

			 }
			 
			 int mapId = 1; 
			 int numberOfPartitions = 100;
			 
			 KValueTypeId keyType = KValueTypeId.Int; 
			 
			 boolean ordering =true;
			 mapSHMShuffleStore = 
					  ShuffleStoreManager.INSTANCE.createMapShuffleStore(
                                            shuffleResource.getSerializationResource().getKryoInstance(), 
                                            shuffleResource.getSerializationResource().getByteBuffer(), 
                                            shuffleResource.getLogicalThreadId(),
					    shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
			 
			 mapSHMShuffleStore.stop();
			
			 //NOTE: is that the same for MapShuffleStore, cleanup and shutdown are identical. 
			 mapSHMShuffleStore.shutdown();
		 
		 }
		 
		 //now if we retrieve the shuffle store tracker, we should have one entry.
		 ShuffleStoreTracker tracker = ShuffleStoreManager.INSTANCE.getShuffleStoreTracker();
		 ArrayList<MapSHMShuffleStore> mapShuffleStores =  tracker.getMapShuffleStores(shuffleId);
		 
		 Assert.assertEquals (mapShuffleStores.size(), counter);
		 
		 //then issue clean up. for a store
		 ShuffleStoreManager.INSTANCE.cleanup(shuffleId);
		 mapShuffleStores = tracker.getMapShuffleStores(shuffleId);
		 Assert.assertEquals (mapShuffleStores, null);
		 
		 
		 ShuffleStoreManager.INSTANCE.shutdown();
	 }
	 
	 
	 /**
	  * to test how shuffle resource tracker behaves, for a real map->reduce shuffle, and then to see how internal C++ shuffle engine's
	  * NVM resource cleanup goes.  
	  */
	 @Test
	 public void testMapReduceShuffleDataInvolvedForShuffleStoreTracker() {
		 LOG.info("this is the test for testMapReduceShuffleDataInvolvedForShuffleStoreTracker");
		 
		 int executorId = 0; 
		 ShuffleStoreManager.INSTANCE.initialize(
				 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, executorId);
		 //to get a new heap instance for each new test case launched.
		 ShuffleStoreManager.INSTANCE.registershm();
		 
 
		 long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
		 LOG.info("native pointer of shuffle store manager retrieved is:"
		                + "0x"+ Long.toHexString(nativePointer));

		 MapSHMShuffleStore mapSHMShuffleStore = null; 
		 
		 ThreadLocalShuffleResourceHolder resourceHolder= new ThreadLocalShuffleResourceHolder();
                 ShuffleResource shuffleResource = resourceHolder.getResource();

                 if (shuffleResource == null) {
		       //then start the map shuffle store manager. 
                       //NOTE: in Java, we will have to de-couple its dependency to Spark core in Scala. 
		       Kryo kryo=new Kryo();
		       kryo.register(ApplicationTestClass.class);
			 
		       //create a direct bytebuffer:
		       int bufferSize = 1*1024*1024; // 1M bytes
		       ByteBuffer byteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		       int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();			 
  		    
                       shuffleResource = new ShuffleResource(
						 new ReusableSerializationResource (kryo, byteBuffer),
						 logicalThreadId);

		       //push to the thread specific storage for future retrieval in the same task execution.
		       resourceHolder.initialize (shuffleResource);
		  }

		  int shuffleId = 0;
		  int mapId = 1; 
		  int numberOfPartitions = 100;
		 
		  KValueTypeId keyType = KValueTypeId.Int; 
		 

		  boolean ordering =true;
		  mapSHMShuffleStore = 
				  ShuffleStoreManager.INSTANCE.createMapShuffleStore(
				     shuffleResource.getSerializationResource().getKryoInstance(), 
                                     shuffleResource.getSerializationResource().getByteBuffer(),
				     shuffleResource.getLogicalThreadId(),
				     shuffleId, mapId, numberOfPartitions, keyType, SIZE_OF_BATCH_SERIALIZATION, ordering);
		 
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

		 mapSHMShuffleStore.stop();
		 LOG.info("map shuffle store stopped");

                 //then the reduce store starts
                 //we should re-use the one created from the map side
		 //Kryo reduceSideKryo=new Kryo();
		 //reduceSideKryo.register(ApplicationTestClass.class);
		 //ByteBuffer reduceSideByteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		 
		 //choose reduce that is not 0, or 1, or 7 (arbitrary number that is smaller than number of partitions)
		 int reduceId = 0; 
                 //since this is single thread, it is feasible to reuse the map-side serialization resource: kryo serializer
                 //and serialization buffer.
		 ReduceSHMShuffleStore reduceSHMShuffleStore = null;

		 {

		         ThreadLocalShuffleResourceHolder resourceHolderReducerSide= new ThreadLocalShuffleResourceHolder();
                         ShuffleResource shuffleResourceReducerSide =resourceHolderReducerSide.getResource();

                         if (shuffleResourceReducerSide == null) {

			    LOG.info("retrieved shuffle resource is null. so we will need to create a new one.");
			    //then start the map shuffle store manager. 
                            //NOTE: in Java, we will have to de-couple its dependency to Spark core in Scala. 
			    Kryo kryo=new Kryo();
			    kryo.register(ApplicationTestClass.class);
			 
			    //create a direct bytebuffer:
			    int bufferSize = 1*1024*1024; // 1M bytes
			    ByteBuffer byteBuffer =  ByteBuffer.allocateDirect(bufferSize);
			    int logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter();			 

			    shuffleResourceReducerSide = new ShuffleResource(
						 new ReusableSerializationResource (kryo, byteBuffer),
						 logicalThreadId);

			    //push to the thread specific storage for future retrieval in the same task execution.
			    resourceHolderReducerSide.initialize (shuffleResourceReducerSide);

			 }

                        LOG.info("retrieved shuffle resource is not null. so we will re-use the existing  one.");
			reduceSHMShuffleStore=	 ShuffleStoreManager.INSTANCE.createReduceShuffleStore(
                                   shuffleResourceReducerSide.getSerializationResource().getKryoInstance(), 
                                   shuffleResourceReducerSide.getSerializationResource().getByteBuffer(),
				   shuffleId, reduceId, numberOfPartitions, ordering, true); 
		 }

		 
		 {
			 boolean ordering2= true;
			 reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions, ordering2, true); 
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
		 
    		 //NOTE: for map-shuffle-store, are cleanup(.) and shutdown(.) identical? 
                 //Note: we defer map store shutdown until the shuffle store manager's cleanup shuffle id phase.
		 // mapSHMShuffleStore.shutdown();
		 //LOG.info("map shuffle store shutdown");
		 
		 //now if we retrieve the shuffle store tracker, we should have one entry.
		 ShuffleStoreTracker tracker = ShuffleStoreManager.INSTANCE.getShuffleStoreTracker();
		 ArrayList<MapSHMShuffleStore> mapShuffleStores =  tracker.getMapShuffleStores(shuffleId);
		 
		 Assert.assertEquals (mapShuffleStores.size(), 1);
		 
		 //then issue clean up. for a store
		 ShuffleStoreManager.INSTANCE.cleanup(shuffleId);
		 mapShuffleStores = tracker.getMapShuffleStores(shuffleId);
		 Assert.assertEquals (mapShuffleStores, null);
		 
		 
		 
		 ShuffleStoreManager.INSTANCE.shutdown();
		 LOG.info("shuffle store manager shutdown");
	 }
	 
	 @Override
	 protected void tearDown() throws Exception{ 
		 
		 LOG.info("shm region:" + GLOBAL_HEAP_NAME + " to be formated");
		 ShuffleStoreManager.INSTANCE.formatshm(); 
		 
		 super.tearDown();
	 }
	 
	 public static void main(String[] args) {
		  
	      junit.textui.TestRunner.run(ShuffleStoreTrackerTest.class);
	 }
}


