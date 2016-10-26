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

import junit.framework.Test;
import junit.framework.TestSuite;


public class AllTests {

	public static void main(String[] args) {
		junit.textui.TestRunner.run(suite());
	}
	
	public static Test suite() { 
		
		
		//we only pick the following two test cases related classes.
		TestSuite suite= new TestSuite("package com.hp.hpl.firesteel.shuffle tests");
		
		suite.addTestSuite(ShuffleStoreManagerTest.class);
		suite.addTestSuite(ShuffleStoreTrackerTest.class);
		suite.addTestSuite(KryoserializerTest.class);
		
		suite.addTestSuite(SortBasedMapSHMShuffleStoreWithIntKeysTest.class);
		suite.addTestSuite(SortBasedReduceSHMShuffleStoreWithIntKeysTest.class);
                suite.addTestSuite(SortBasedMapSHMShuffleStoreWithByteArrayKeysTest.class);
                suite.addTestSuite(SortBasedReduceSHMShuffleStoreWithByteArrayKeysTest.class);
		
		suite.addTestSuite(HashBasedMapSHMShuffleStoreWithIntKeysTest.class);
		suite.addTestSuite(HashBasedMapSHMShuffleStoreWithLongKeysTest.class);
		suite.addTestSuite(HashBasedReduceSHMShuffleStoreWithIntKeysTest.class);
		suite.addTestSuite(HashBasedReduceSHMShuffleStoreWithLongKeysTest.class);
		 
		suite.addTestSuite(PassThroughReduceSHMShuffleStoreWithIntKeysTest.class);
		suite.addTestSuite(PassThroughReduceSHMShuffleStoreWithLongKeysTest.class);
		 
		return suite;
	}
}
