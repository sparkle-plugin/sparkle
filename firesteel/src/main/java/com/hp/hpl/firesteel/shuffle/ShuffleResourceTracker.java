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

import java.util.concurrent.ConcurrentLinkedQueue;  
import java.util.Iterator; 
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * To allow the reusable resource: kryo instance, bytebuffer for serialization and the logical thread id
 * to be used within the same Spark executor, independent of whether it is at the Map phase or Reduce phase.
 *
 */
public class ShuffleResourceTracker {
       private static final Logger LOG = LoggerFactory.getLogger(ShuffleResourceTracker.class.getName());
	   //to be conservative, instead of ArrayBlockingQueue that has the limited capacity, ConcurrentLinkedQueue
	   //does not need to specify the capacity limit. 
	   //to track all of the resources that have been created so far.
       private ConcurrentLinkedQueue<ThreadLocalShuffleResourceHolder.ShuffleResource> fullTrackingQueue;
       //to track all of the resources that are currently idle and ready to be picked up.
       private ConcurrentLinkedQueue <ThreadLocalShuffleResourceHolder.ShuffleResource> idleTrackingQueue; 
       
       
       public  ShuffleResourceTracker  (){
    	   
    	   this.fullTrackingQueue =
    			   new ConcurrentLinkedQueue <ThreadLocalShuffleResourceHolder.ShuffleResource>();
    	   this.idleTrackingQueue = 
    			   new ConcurrentLinkedQueue <ThreadLocalShuffleResourceHolder.ShuffleResource>();
       }
       
       public ThreadLocalShuffleResourceHolder.ShuffleResource getIdleResource() {
    	   ThreadLocalShuffleResourceHolder.ShuffleResource resource = this.idleTrackingQueue.poll();
           if (resource != null) {
               LOG.info("get re-usable serialized resource with id: " + resource.hashCode());
	   }
           else {
	       LOG.info("idle serialized resources is empty!!");
	   }

    	   return resource;
       }
       
       /**
        * add the new resource to the tracking, and simply return the passed-in resource back. 
        * @param resource: because the resource element: Kryo instance is from Spark, to decouple the dependency
        * to Spark, we will have to rely on the Scala side to provide the Kryo instance. 
        * @return
        */
       public ThreadLocalShuffleResourceHolder.ShuffleResource addNewResource(
    		   ThreadLocalShuffleResourceHolder.ShuffleResource resource ) {
           LOG.info("Thread: " +  Thread.currentThread().getId() 
                        + "add new serialized resource with id: " + resource.hashCode());
    	   this.fullTrackingQueue.add(resource);
    	   return resource; 
       }
       
       /**
        * so that the resource can be re-used later by other Map or Reduce tasks. 
        * @param resource the resource that gets finished by the Map or Reduce task, and it is returned to the queue. 
        */
       public void recycleSerializationResource(ThreadLocalShuffleResourceHolder.ShuffleResource resource) {
           LOG.info("Thread: " +  Thread.currentThread().getId() 
                        + "recycle serialized resource with id: " + resource.hashCode());
    	   this.idleTrackingQueue.add(resource);
       }
       
       
       public int getIdleTrackingQueueSize() {
    	   return this.idleTrackingQueue.size();
       }
       
       public int getFullTrackingQueueSize() {
    	   return this.fullTrackingQueue.size(); 
       }
       
       
       /**
        * to clean up the resources held. 
        */
       public void shutdown () {
            Iterator <ThreadLocalShuffleResourceHolder.ShuffleResource> resources = this.fullTrackingQueue.iterator();
    	    while (resources.hasNext()) {
    	    	ThreadLocalShuffleResourceHolder.ShuffleResource resource = resources.next();
    	    	resource.freeResource(); 
    	    }
    	    
    	    this.fullTrackingQueue.clear();
    	    this.idleTrackingQueue.clear();
       }
}
