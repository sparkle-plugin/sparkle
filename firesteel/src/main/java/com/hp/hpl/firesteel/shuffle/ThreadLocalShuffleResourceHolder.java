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
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import com.esotericsoftware.kryo.Kryo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To hold the shuffle resources on each runtime Spark task, at the map side and also the receiver side.
 * The current resources include Kryo instance that has all of the registered classes, and the ByteBuffer 
 * allocated from the native memory.
 * 
 * The same thread in an executor will be used as a Map thread or a Reduce thread. The same logical thread
 * id can be re-used in either a Map thread or a Reduce Thread, launched from a thread pool. As we found that
 * the total number of different OS threads can go beyond the number of the cores (task threads) specified by 
 * Spark configuration file. 
 * 
 * More importantly, the same logical thread gets tiled to a pair of <KryoInstance and ByteBuffer>. Both the logical
 * threads and KryoInstance/ByteBuffer are all re-usable resources. 
 *
 */
public class ThreadLocalShuffleResourceHolder {
	
	private static final Logger LOG = 
            LoggerFactory.getLogger(ThreadLocalShuffleResourceHolder.class.getName());
	
	/**
	 * by combining these two resources, we will have it to be freed out when the Map or reduce Task is done.
	 *
	 */
    public static class ReusableSerializationResource {
        private Kryo kryo;
        private ByteBuffer buffer; 
    
        public ReusableSerializationResource  (Kryo kryoInstance, ByteBuffer bufferInstance) {
            this.kryo = kryoInstance;
            this.buffer = bufferInstance; 
        }
    
        public Kryo  getKryoInstance() {
            return this.kryo;
        }
    
        public ByteBuffer getByteBuffer() {
            return this.buffer; 
        }

        public void freeResource() {
            try {
                if ( (buffer != null) && (buffer.isDirect())) {
                    Field cleanerField = buffer.getClass().getDeclaredField("cleaner");
                    cleanerField.setAccessible(true);
                    Object cleaner = cleanerField.get(buffer);
                    Method cleanerMethod = cleaner.getClass().getMethod("clean");
                    cleanerMethod.invoke(cleaner, null);
                }
            } catch (Exception ex) {
                LOG.error("fails to free shuffle resource.", ex);
            }
        }
    }

	public static class ShuffleResource {
		private ReusableSerializationResource serializationResource; 
		
		//to add to the logical thread, which we use to keep track of shuffle store memory resource.
		private int logicalThreadId; 
		
		/**
		 * Two kinds of resources: kryo instance and byte buffer instance, and also the logical thread id. 
		 * @param resourceInstance: that holds the kryo instance and bytebuffer instance. 
		 * @param logical thread id, the logical thread id that is meaningful only in the 
		 */
		public ShuffleResource (ReusableSerializationResource resourceInstance, int logicalThreadId) {
			this.serializationResource = resourceInstance; 
			this.logicalThreadId = logicalThreadId; 
		}
		
		public ReusableSerializationResource getSerializationResource() {
			return this.serializationResource; 
		}
		
		public int getLogicalThreadId() {
			return this.logicalThreadId; 
		}
		
		public void freeResource() {
			if (this.serializationResource != null) {
				this.serializationResource.freeResource();
			}
		}
	}
	
	
	private static final ThreadLocal<ShuffleResource> holder = new ThreadLocal<ShuffleResource>();
	
	/**
	 * The two resources will have to be created outside this class. 
	 * NOTE: logical thread id will be called from Shuffle Store Manager to get the unique atomic counter. 
	 */
	public void initialize (ShuffleResource resource) {
		holder.set(resource);
	}
	
	/**
	 * retrieve the locally stored resource in the thread. If the resource is null, then
	 * we will need to initialize the resource. 
	 */
	public ShuffleResource getResource() {
		return holder.get();
	}
	
}
