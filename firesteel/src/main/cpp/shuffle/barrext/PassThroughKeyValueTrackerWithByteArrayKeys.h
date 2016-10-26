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

#ifndef PASSTHROUGH_KEY_TRACKER_WITH_BYTEARRAY_KEYS_H_
#define PASSTHROUGH_KEY_TRACKER_WITH_BYTEARRAY_KEYS_H_

#include <glog/logging.h>
#include "ShuffleConstants.h"
#include <stdlib.h>
#include <string.h>

//to define the structure for byte array key with variable length
namespace ByteArrayKeyWithVariableLength {

  //this is designed for the JNI to pick up the pass-through key/value pairs.
  //the actual data is already stored in the buffer manager passed from the Java. 
  struct PassThroughKeyTracker {
    int start_keyoffset; //cached key in the passed-in buffer manager.
    int end_keyoffset; //cached key in the passed-in buffer manager.

    int start_valueoffset; //cached data in the passed-in buffer manager (same for the keys)
    int end_valueoffset;   //cached data in the passed-in buffer manager (same for the keys)
  };

  /*
   * this is designed for direct pass-through. This is used when no ordering is required,
   * and no aggregation is required.
   */
  struct PassThroughMapBuckets {
    int reducerId; 
    //the curent allocated number of elements to hold the key tracker.
    //therefore, total malloc will need keyTrackerCapacity*sizeof(PassThroughKeyTracker)
    size_t keyValueOffsetTracker;
    size_t currentKeyValueOffsetTrackerCapacity; 
    
    PassThroughKeyTracker *keyAndValueOffsets;
   
    //the holder passed through from the Java via byte-buffer.
    unsigned char *passThroughBuffer;
    unsigned char *passThroughBufferCursor;
    size_t buffer_capacity;

    size_t bufferPositionTracker;
    
    //to keep track of whether keys are ordered.
    bool orderingRequired; 
    //to keep track of whetehr values are aggregated
    bool aggregationRequired;

    //to record whether it is activated;
    bool activated; 

    PassThroughMapBuckets(int rId, bool ordering, bool aggregation,
                         unsigned char *buffer, size_t buf_capacity):
        reducerId (rId),
	passThroughBuffer (buffer),
	passThroughBufferCursor(buffer),
	buffer_capacity(buf_capacity),
        orderingRequired(ordering),
	aggregationRequired(aggregation),
	activated(false) {
          //with the configuration, to control whether we need to do the malloc nor not.
          if ( !(orderingRequired || aggregationRequired)) {
            currentKeyValueOffsetTrackerCapacity =
                        SHMShuffleGlobalConstants::PASSTHROUGH_KEYVALUE_TRACKER_SIZE;
            keyAndValueOffsets = 
              (PassThroughKeyTracker*)malloc(currentKeyValueOffsetTrackerCapacity * sizeof (PassThroughKeyTracker));
            activated = true; 
	  }
          else { 
	    currentKeyValueOffsetTrackerCapacity = 0;
            keyAndValueOffsets = nullptr;
	  }
          
          //start with 0 
          keyValueOffsetTracker=0;
          bufferPositionTracker=0;
    }

    //we need to invoke this check, before doing actual add key/value operation.
    bool isActivated () {
      return activated; 
    }

    //copy both the key and the value to the buffer.
    //right now, if the buffer exceeds the total size, we will raise the FATAL error. Later, we will 
    //need to see how to grow the size of the pass-in buffer from Java.
    size_t addKeyValue(unsigned char *kvalue, int ksize, unsigned char *vvalue, int vsize){
        if (keyValueOffsetTracker == currentKeyValueOffsetTrackerCapacity) {
            LOG(INFO) << "key and value offsets " << (void*) keyAndValueOffsets
               << " with keyvalueoffset tracker: " << keyValueOffsetTracker
	       << " reaches tracker capacity: " << currentKeyValueOffsetTrackerCapacity;
            //re-allocate then.
            currentKeyValueOffsetTrackerCapacity += SHMShuffleGlobalConstants::PASSTHROUGH_KEYVALUE_TRACKER_SIZE;
            keyAndValueOffsets  = (PassThroughKeyTracker*)realloc(keyAndValueOffsets,
						  currentKeyValueOffsetTrackerCapacity*sizeof(PassThroughKeyTracker));
        }

        //to make sure that we have sufficient buffer memory left
        size_t expectedSize = bufferPositionTracker + ksize + vsize;
        if (expectedSize < buffer_capacity) {
	   //OK, we can safely copy it to the output buffer.
           //(1) copy the key, and then advance the buffer pointer
	   keyAndValueOffsets[keyValueOffsetTracker].start_keyoffset = bufferPositionTracker;
           memcpy(passThroughBufferCursor, kvalue, ksize);
           passThroughBufferCursor+=ksize; //pointer.  
           bufferPositionTracker += ksize; //offset
  	   keyAndValueOffsets[keyValueOffsetTracker].end_keyoffset = bufferPositionTracker;


  	   keyAndValueOffsets[keyValueOffsetTracker].start_valueoffset = bufferPositionTracker;
           memcpy(passThroughBufferCursor, vvalue, vsize);
           passThroughBufferCursor+=vsize;  
           bufferPositionTracker += vsize;
  	   keyAndValueOffsets[keyValueOffsetTracker].end_valueoffset = bufferPositionTracker;

           size_t currentIndex = keyValueOffsetTracker;
           keyValueOffsetTracker ++;

           return currentIndex;

	}
        else {
          //we will need to later have the way to extend the buffer that gets passed from Java. 
          //the runtime assertion will terminate the process.
	  CHECK (expectedSize < buffer_capacity) << " buffer capacity: " << buffer_capacity <<" gets exceeded";
          return -1; 
	}

    }

    void reset() {
        //start with 0 
        keyValueOffsetTracker=0;
        bufferPositionTracker =0;
        passThroughBufferCursor=passThroughBuffer;
    }

    //to free the held resources, and return them to the pool.
    void release() {
        //start with 0 
        keyValueOffsetTracker = 0;
        bufferPositionTracker = 0;
        passThroughBufferCursor=passThroughBuffer;

        //free the resources. later, we will put them to the pooled resources.
        if (keyAndValueOffsets != nullptr ) {
           free((void*)keyAndValueOffsets);
           keyAndValueOffsets = nullptr;
	}

    }

  };
};

#endif /*PASSTHROUGH_KEY_TRACKER_WITH_BYTEARRAY_KEYS_H_*/



