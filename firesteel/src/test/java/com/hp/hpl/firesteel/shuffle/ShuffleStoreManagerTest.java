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
import org.junit.Test;
import junit.framework.TestCase;
 

public class ShuffleStoreManagerTest extends TestCase {

	 private static final Logger LOG = LoggerFactory.getLogger(ShuffleStoreManagerTest.class.getName());
	 //the global heap name created via RMB. 
	 private static final String GLOBAL_HEAP_NAME = "/dev/shm/nvm/global0";
	 
	 @Override
	 protected void setUp() throws Exception{ 
		  
		 super.setUp();
		 
	 }
	 
	 
	 /**
	  *to test Shuffle Store Manager's load library, init and then shutdown 
	  */
	 @Test
	 public void testStartShutdownShuffleStoreManager() {
		 LOG.info("this is the test for testStartShutdownShuffleStoreManager");
		 
		 ShuffleStoreManager.INSTANCE.initialize(
				 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, 0);
		 long nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
		 LOG.info("native pointer of shuffle store manager retrieved is:"
		                + "0x"+ Long.toHexString(nativePointer));
		 ShuffleStoreManager.INSTANCE.shutdown();
	 }
	 
	 /**
	  *to test Shuffle Store Manager's formating of the shared-memory library. 
	  */
	 @Test
	 public void testFormatingOfSharedMemory() {
		 LOG.info("this is the test for testFormatingOfSharedMemory");
		 
		 ShuffleStoreManager.INSTANCE.initialize(
				 GLOBAL_HEAP_NAME, TestRelatedConstants.maxNumberOfTaskThreads, 0);
		 LOG.info("shm region:" + GLOBAL_HEAP_NAME + " to be formated");
		 ShuffleStoreManager.INSTANCE.formatshm(); 
		 ShuffleStoreManager.INSTANCE.shutdown();
	 }
	 
	 @Override
	 protected void tearDown() throws Exception{ 
		 //do something first;
		 super.tearDown();
	 }
	 
	 public static void main(String[] args) {
		  
	      junit.textui.TestRunner.run(ShuffleStoreManagerTest.class);
	 }
}


