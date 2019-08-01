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

package org.apache.spark.shuffle.shm


import com.hp.hpl.firesteel.shuffle.ShuffleDataModel.ReduceStatus
import org.apache.spark.TaskContext
import com.hp.hpl.firesteel.shuffle.ReduceSHMShuffleStore
import com.hp.hpl.firesteel.shuffle.ShuffleDataModel
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}

import scala.collection.mutable.ArrayBuffer

//to pass collection between Java and Scala
import scala.language.existentials
import scala.collection.JavaConversions._
import java.util.List
import java.util.ArrayList
import java.lang.{Float => JFloat}
import java.lang.{Integer => JInteger}
import java.lang.{Long => JLong}
import java.lang.{String => JString}

/**
 * to create iterator that wrapps the de-serialized data pulled from the C++ shuffle engine,
 * with returned iterator to be (key, value)
 *
 */
private[spark] class ShmShuffleFetcherKeyValueIterator
(contenxt: TaskContext,
 statuses: Seq[(BlockId, Long, Long, Long, Boolean)],
 reduceShuffleStore: ReduceSHMShuffleStore)
  extends Iterator[(Any, Any)] with Logging {

  //this parameter will be set as a configuration parameter later.
  private val SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE =  2000

  //when the actual key/value pairs returned has number smaller than the specified size,
  //we know that that is the last batch.
  private var endOfFetch =false

  //private val kvBuffer = new Queue[(Object, Object)]
  //use array instead of queue, to make it faster.
  private val kvBuffer = new Array[(Object, Object)](SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE)
  private var count = 0
  private var actualPairs = 0

  //this  value type remained unknown after mergeSort call.
  private var kvalueTypeId  = ShuffleDataModel.KValueTypeId.Unknown
  //the round of fetch so far
  private var fetchRound = 0
  //value holder alwasy is with object type.
  private  val vvalues = new ArrayList[Object]()
  //to pre-create the holder for the key-value retrieved from the shuffle engine.
  private val ikvalues = new ArrayList[JInteger]()
  private val fkvalues = new ArrayList[JFloat]()
  private val lkvalues = new ArrayList[JLong]()
  private val skvalues = new ArrayList[JString]()
  private val bakvalues = new ArrayList[Array[Byte]]()
  private val okvalues = new ArrayList[Object]()

  initialize()

  private [this] def initialize(): Unit= {
    //construct the map data sources.
    val mapIds = new ArrayBuffer[Int]()
    val shmRegionIds = new ArrayBuffer[Long]()
    val offsetToIndexChunks = new ArrayBuffer[Long]()
    val sizes  = new ArrayBuffer[Long]()
    var isPrimitive = false
    //construct reduce status based on the collected map status from MapOutputTracker
    statuses.foreach { case (blockId, regionId, chunkOffset, bucket_size, isPrimitiveKey) =>
            val shuffleBlockId = blockId.asInstanceOf[ShuffleBlockId]
            mapIds.append(shuffleBlockId.mapId)
            sizes.append(bucket_size)
            shmRegionIds.append(regionId)
            offsetToIndexChunks.append(chunkOffset)
            isPrimitive = isPrimitiveKey
    }
    //start the sort-merge
    val reduceStatus = new ReduceStatus(mapIds.toArray, shmRegionIds.toArray,
      offsetToIndexChunks.toArray, sizes.toArray)

    if (!isPrimitive) {
      reduceShuffleStore.mergeSort(reduceStatus)
      //until after mergeSort, we can now fetch the real value type
      //if all of the input merge-sort channels (buckets) belonging to a reducer are empty.
      //we will choose the type of integer key type. But at the end, there will be no non-zero
      //iterator output.
      kvalueTypeId = reduceShuffleStore.getKValueTypeId()
    } else {
      // ReduceShuffleStore.mergeSort is actually factory method of the store
      // so that I decided to make the object shuffle store and its factory.
      kvalueTypeId = ShuffleDataModel.KValueTypeId.Object
      reduceShuffleStore.createShuffleStore(reduceStatus)
    }

    fetchRound=0
    fetch() //make the first fetch.
  }

  //NOTE: fetch() method gets called from hasNext() also!
  private def fetch(): Unit = {
    //retrieve the first batch of the objects.
    count = 0
    actualPairs=0
    kvalueTypeId match {
      case  ShuffleDataModel.KValueTypeId.Int => {
        //to retrieve a collection of {k, {v1, v2,...}} from the C++ shuffle engine where
        //create a holder, we need to move this holder in outer scope. so that we do not need
        //to create every time
        if (fetchRound == 0) {
          for (i <- 0 to SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE - 1) {
            ikvalues.add(0) //initialization to 0;
            vvalues.add(null) //initialization to null;
          }
        }
        actualPairs= reduceShuffleStore.getSimpleKVPairsWithIntKeys(
          ikvalues, vvalues, SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE)

        //now, push the data to the kvbuffer.
        for (i <- 0 to actualPairs-1) {
          kvBuffer(i) = (ikvalues(i), vvalues(i))
        }

        if (actualPairs < SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE ){
          endOfFetch = true
        }
      }
      case  ShuffleDataModel.KValueTypeId.Float => {
        //to retrieve a collection of {k, {v1, v2,...}} from the C++ shuffle engine where
        //create a holder, we need to move this holder in outer scope. so that we do not need
        //to create every time
        if (fetchRound == 0) {
          for (i <- 0 to SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE - 1) {
            fkvalues.add(0F) //initialization to 0;
            vvalues.add(null) //initialization to null;
          }
        }
        //WARNING: we need to provide list based APIS, instead of []. Need to see whether it
        //can be passed around
        actualPairs= reduceShuffleStore.getSimpleKVPairsWithFloatKeys(
          fkvalues, vvalues, SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE)
        //now, push the data to the kvbuffer.
        for (i <- 0 to actualPairs-1) {
          kvBuffer (i)= (fkvalues(i), vvalues(i))
        }

        if (actualPairs < SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE ){
          endOfFetch = true
        }
      }
      case  ShuffleDataModel.KValueTypeId.Long => {
        //to retrieve a collection of {k, {v1, v2,...}} from the C++ shuffle engine where
        //WARNING: we need to provide list based APIS, instead of []. Need to see whether it
        //can be passed around
        //create a holder, we need to move this holder in outer scope. so that we do not need
        //to create every time
        if (fetchRound == 0) {
          for (i <- 0 to SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE - 1) {
            lkvalues.add(0L) //initialization to 0;
            vvalues.add(null); //initialization to null;
          }
        }
        actualPairs= reduceShuffleStore.getSimpleKVPairsWithLongKeys(
          lkvalues, vvalues,SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE)
        //now, push the data to the kvbuffer.
        for (i <- 0 to actualPairs-1) {
          kvBuffer(i) = (lkvalues(i), vvalues(i))
        }

        if (actualPairs < SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE ){
           endOfFetch = true
        }
      }
      case  ShuffleDataModel.KValueTypeId.String => {
        //to retrieve a collection of {k, {v1, v2,...}} from the C++ shuffle engine where
        //WARNING: we need to provide list based APIS, instead of []. Need to see whether it
        //can be passed around
        //create a holder, we need to move this holder in outer scope. so that we do not need
        //to create every time
        if (fetchRound == 0) {
          for (i <- 0 to SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE - 1) {
            skvalues.add(null) //initialization to 0;
            vvalues.add(null); //initialization to null;
          }
        }
        actualPairs= reduceShuffleStore.getSimpleKVPairsWithStringKeys(
          skvalues, vvalues, SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE)
        //now, push the data to the kvbuffer.
        for (i <- 0 to actualPairs-1) {
           kvBuffer(i) = (skvalues(i), vvalues(i))
        }

        if (actualPairs < SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE ){
           endOfFetch = true;
        }
      }
     case  ShuffleDataModel.KValueTypeId.ByteArray => {
        //to retrieve a collection of {k, {v1, v2,...}} from the C++ shuffle engine where
        //WARNING: we need to provide list based APIS, instead of []. Need to see whether it
        //can be passed around create a holder, we need to move this holder in outer scope.
        //so that we do not need to create every time
        if (fetchRound == 0) {
          for (i <- 0 to SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE - 1) {
            bakvalues.add(null) //initialization to 0;
            vvalues.add(null); //initialization to null;
          }
        }
        actualPairs= reduceShuffleStore.getSimpleKVPairsWithByteArrayKeys(
          bakvalues, vvalues, SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE)
        //now, push the data to the kvbuffer.
	for (i <- 0 to actualPairs-1) {
          kvBuffer(i) = (bakvalues(i), vvalues(i))
        }

	if (actualPairs < SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE ){
          endOfFetch = true;
        }
      }
      case  ShuffleDataModel.KValueTypeId.Object => {
        //to retrieve a collection of {k, {v1, v2,...}} from the C++ shuffle engine where
        //WARNING: we need to provide list based APIS, instead of []. Need to see whether it
        //can be passed around
        //create a holder, we need to move this holder in outer scope. so that we do not need
        //to create every time
        if (fetchRound == 0) {
          for (i <- 0 to SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE - 1) {
            okvalues.add(null) //initialization to 0;
            vvalues.add(null); //initialization to null;
          }
        }
        actualPairs= reduceShuffleStore.getSimpleKVPairs(
          okvalues, vvalues,SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE)
        //now, push the data to the kvbuffer.
        for (i <- 0 to actualPairs-1) {
          kvBuffer(i) = (okvalues(i), vvalues(i))
        }

        if (actualPairs < SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE ){
          endOfFetch = true
        }
      }
      case _ => {
        throw new NoSuchElementException
      }
    }

    fetchRound=fetchRound + 1
  }

  override def hasNext: Boolean = {
    //when exhausted. get to the next batch of the (K, {V}) pair.
    if (count < actualPairs ) {
      true
    }
    else {
      if (!endOfFetch) {
        fetch() //make further fetch now, if not the last batch
      }
      else {
        //this is the end of the fetching. we need to stop our store to release DRAM resource
        //the shutdown to release NVM resource need to wait until the stage shutdown
        reduceShuffleStore.stop()

      }
      //right after the fetch
      if (count < actualPairs) {
        true
      }
      else {
        false
      }
    }
  }

  override def next(): (Any, Any) = {
    val result = kvBuffer(count) //it will return the dequeued element.
    count = count + 1
    result
  }
}
