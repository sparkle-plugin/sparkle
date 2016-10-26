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

import java.nio.ByteBuffer

import org.apache.spark.Aggregator
import org.apache.spark.LocalSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkEnv
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.CompactBuffer
import org.scalatest.FunSuite
import org.scalatest.FunSuite
import org.scalatest.Ignore

import com.hp.hpl.firesteel.shuffle.ShuffleDataModel.ReduceStatus
import org.apache.spark.TaskContext
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.shuffle.shm.ShmShuffleWithMultiValues._
import com.hp.hpl.firesteel.shuffle._
import com.hp.hpl.firesteel.shuffle.ShuffleDataModel.KValueTypeId
import com.hp.hpl.firesteel.shuffle.ShuffleDataModel.KValueTypeId._
import com.hp.hpl.firesteel.shuffle.ThreadLocalShuffleResourceHolder._
import com.hp.hpl.firesteel.shuffle.ShuffleDataModel
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue

//to pass collection between Java and Scala
import scala.collection.JavaConversions._
import java.util.ArrayList;
import java.lang.{Float => JFloat}
import java.lang.{Integer => JInteger}
import java.lang.{Long => JLong}
import java.lang.{String => JString}

import org.apache.spark._


class TestShuffleReaderSuite extends FunSuite with LocalSparkContext {
  //use variable sc instead.
  private val test= new SparkConf(false)

  //NOTE: after test result, this class will need to be merged back to the actual class
  // in "shm" package.
  private class ShmShuffleFetcherIterator(conf: SparkConf)
                     extends Iterator[(Any, Seq[Any])] with Logging {

    private val SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE =  10;

    //when the actual key/value pairs returned has number smaller than the specified size,
    //we know that that is the last batch.
    private var endOfFetch =false

    private val kvBuffer = new Queue[(Object, Seq[Object])]

    var reduceSHMShuffleStore = null.asInstanceOf[ReduceSHMShuffleStore]
    private var kvalueTypeId  = ShuffleDataModel.KValueTypeId.Unknown

    initialize()

    private def getThreadLocalShuffleResource(conf: SparkConf):
    ThreadLocalShuffleResourceHolder.ShuffleResource = {
       val SERIALIZATION_BUFFER_SIZE: Int =
        conf.getInt("spark.shuffle.shm.serializer.buffer.max.mb", 64) * 1024 * 1024;

             val resourceHolder= new ThreadLocalShuffleResourceHolder()
       var shuffleResource =
           ShuffleStoreManager.INSTANCE.getShuffleResourceTracker.getIdleResource()

       if (shuffleResource == null) {
          //still at the early thread launching phase for the executor, so create new resource
          val kryoInstance =  new KryoSerializer(SparkEnv.get.conf).newKryo(); //per-thread
          val serializationBuffer = ByteBuffer.allocateDirect(SERIALIZATION_BUFFER_SIZE)
          if (serializationBuffer.capacity() != SERIALIZATION_BUFFER_SIZE ) {
            logError("Thread: " + Thread.currentThread().getId
              + " created serialization buffer with size: "
              + serializationBuffer.capacity()
              + ": FAILED to match: " + SERIALIZATION_BUFFER_SIZE)
          }
          else {
            logInfo("Thread: " + + Thread.currentThread().getId
              + " created the serialization buffer with size: "
              + SERIALIZATION_BUFFER_SIZE + ": SUCCESS")
          }
          //add a logical thread id
          val logicalThreadId = ShuffleStoreManager.INSTANCE.getlogicalThreadCounter ()
          shuffleResource = new ShuffleResource(
              new ReusableSerializationResource (kryoInstance, serializationBuffer),
              logicalThreadId)
          //add to the resource pool
          ShuffleStoreManager.INSTANCE.getShuffleResourceTracker.addNewResource(shuffleResource)
          //push to the thread specific storage for future retrieval in the same task execution.
          resourceHolder.initialize (shuffleResource)

          logDebug ("Thread: " + Thread.currentThread().getId
            + " create kryo-bytebuffer resource for mapper writer")
       }

       shuffleResource

    }

    private [this] def initialize(): Unit= {

      val shuffleManager = SparkEnv.get.shuffleManager
      assert(shuffleManager.isInstanceOf[ShmShuffleManager])

      ShuffleStoreManager.INSTANCE.initialize(
           TestConstants.GLOBAL_HEAP_NAME, TestConstants.maxNumberOfTaskThreads, 0)
      val  nativePointer = ShuffleStoreManager.INSTANCE.getPointer()
      logInfo ("native pointer of shuffle store manager retrieved is:"
        + "0x"+ java.lang.Long.toHexString(nativePointer))

      val shuffleId = 0

      val numberOfPartitions = 100
      val keyType = ShuffleDataModel.KValueTypeId.Int

      val threadLocalResources = getThreadLocalShuffleResource(conf)

      val mapId1 = 1
      val logicalThreadId1 =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter()
      val ordering = true

      val mapSHMShuffleStore1 =
        ShuffleStoreManager.INSTANCE.createMapShuffleStore(
          threadLocalResources.getSerializationResource.getKryoInstance,
          threadLocalResources.getSerializationResource.getByteBuffer,
          logicalThreadId1,
          shuffleId, mapId1, numberOfPartitions, keyType,
          TestConstants.SIZE_OF_BATCH_SERIALIZATION, 
          ordering
          )

      val mapId2 = 4
      val logicalThreadId2 =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter()
      val mapSHMShuffleStore2 =
        ShuffleStoreManager.INSTANCE.createMapShuffleStore(
          threadLocalResources.getSerializationResource.getKryoInstance,
          threadLocalResources.getSerializationResource.getByteBuffer,
          logicalThreadId2, 
          shuffleId, mapId2, numberOfPartitions, keyType,
          TestConstants.SIZE_OF_BATCH_SERIALIZATION, 
          ordering
          )


      val reduceId = 0
      reduceSHMShuffleStore =
        ShuffleStoreManager.INSTANCE.createReduceShuffleStore(
          threadLocalResources.getSerializationResource.getKryoInstance,
          threadLocalResources.getSerializationResource.getByteBuffer,
          shuffleId, reduceId, numberOfPartitions, ordering, true)

      val numberOfVs = 10
      val testObjects = new ArrayList[RankingsClass] ()
      val partitions = new ArrayList[Int]()
      val kvalues = new ArrayList[Int] ()
      val voffsets = new ArrayList[Int] ()

      for (i <- 0 to numberOfVs-1 ) {
        val obj = new RankingsClass(i, "hello" +i, i+1)
        testObjects.add(obj)
        voffsets.add(0)
        partitions.add( i%2 )
        kvalues.add (i)
      }

      for (i <- 0 to numberOfVs-1) {
         mapSHMShuffleStore1.serializeKVPair(kvalues.get(i), testObjects.get(i),partitions.get(i),
                                            i, KValueTypeId.Int.getState())
      }

      val numberOfPairs1 = numberOfVs;
      mapSHMShuffleStore1.storeKVPairs(numberOfPairs1, KValueTypeId.Int.getState())

      val  mapStatusResult1 = mapSHMShuffleStore1.sortAndStore()

      logInfo("map status region id: " + mapStatusResult1.getRegionIdOfIndexBucket())
      logInfo ("map status offset to index chunk: 0x "
        + java.lang.Long.toHexString(mapStatusResult1.getOffsetOfIndexBucket()))
      val buckets = mapStatusResult1.getMapStatus()

      if (buckets != null) {
        for (i <- 0 to buckets.length-1) {
          logInfo ("for map store 1, map status, bucket: " + i + " has size: " + buckets(i))
        }
      }
      else {
        logInfo("for map store 1, map status buckets length is null.")
      }

      for (i <- 0 to numberOfVs-1) {
         mapSHMShuffleStore2.serializeKVPair(kvalues.get(i), testObjects.get(i),partitions.get(i),
                                            i, KValueTypeId.Int.getState())
      }

      val numberOfPairs2 = numberOfVs;
      mapSHMShuffleStore2.storeKVPairs(numberOfPairs2, KValueTypeId.Int.getState())

      val  mapStatusResult2 = mapSHMShuffleStore2.sortAndStore()

      logInfo("map status region id: " + mapStatusResult2.getRegionIdOfIndexBucket())
      logInfo ("map status offset to index chunk: 0x "
        + java.lang.Long.toHexString(mapStatusResult2.getOffsetOfIndexBucket()))
      val buckets2 = mapStatusResult2.getMapStatus()

      if (buckets2 != null) {
        for (i <- 0 to buckets2.length-1) {
          logInfo ("for map store 2, map status, bucket: " + i + " has size: " + buckets2(i))
        }
      }
      else {
        logInfo("for map store 2, map status buckets length is null.")
      }

      //reduce side:
      reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions, ordering, true)
      val mapIds = Seq (mapId1, mapId2).toArray
      val shmRegionNames = Seq (mapStatusResult1.getRegionIdOfIndexBucket(),
        mapStatusResult2.getRegionIdOfIndexBucket()).toArray
      val offsetToIndexChunks = Seq (mapStatusResult1.getOffsetOfIndexBucket(),
        mapStatusResult2.getOffsetOfIndexBucket()).toArray
      val sizes = Seq (mapStatusResult1.getMapStatus()(reduceId),
        mapStatusResult1.getMapStatus()(reduceId)).toArray //pick the first bucket

      val statuses =
        new ShuffleDataModel.ReduceStatus(mapIds, shmRegionNames, offsetToIndexChunks, sizes)
      //NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet.
      reduceSHMShuffleStore.mergeSort(statuses)
      kvalueTypeId=reduceSHMShuffleStore.getKValueTypeId


      fetch() //make the first fetch.
    }

    private def fetch(): Unit = {
      //retrieve the first batch of the objects.
      var actualPairs = 0
      kvalueTypeId match {
        case  ShuffleDataModel.KValueTypeId.Int => {
          //to retrieve a collection of {k, {v1, v2,...}} from the C++ shuffle engine where
          val kvalues = new ArrayList[JInteger]();
          val vvalues = new ArrayList[ArrayList[Object]]();
          //create a holder, we will need to move this folder out of this scope. so that we do not needt
          //to create every time
          for (i <- 0 to SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE-1) {
            kvalues.add(0); //initialization to 0;
            vvalues.add(null); //initialization to null;
          }

          actualPairs= reduceSHMShuffleStore.getKVPairsWithIntKeys(kvalues, vvalues,
            SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE)

          //now, push the data to the kvbuffer.
          for (i <- 0 to actualPairs-1) {
            val vv = vvalues(i).seq
            kvBuffer += Tuple2(kvalues(i), vv)
          }

          if (actualPairs < SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE ){
            endOfFetch = true;
          }
        }
        case  ShuffleDataModel.KValueTypeId.Float => {
          //to retrieve a collection of {k, {v1, v2,...}} from the C++ shuffle engine where
          val kvalues = new ArrayList[JFloat]();
          val vvalues = new ArrayList[ArrayList[Object]]();
          //WARNING: we need to provide list based APIS, instead of []. Need to see whether it
          //can be passed around
          actualPairs= reduceSHMShuffleStore.getKVPairsWithFloatKeys(kvalues, vvalues,
            SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE)
          //now, push the data to the kvbuffer.
          for (i <- 0 to actualPairs-1) {
            val vv = vvalues(i).seq
            kvBuffer += Tuple2(kvalues(i), vv)
          }

          if (actualPairs < SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE ){
            endOfFetch = true;
          }
        }
        case  ShuffleDataModel.KValueTypeId.Long => {
          //to retrieve a collection of {k, {v1, v2,...}} from the C++ shuffle engine where
          val kvalues = new ArrayList[JLong]();
          val vvalues = new ArrayList[ArrayList[Object]]();
          //WARNING: we need to provide list based APIS, instead of []. Need to see whether it
          // can be passed around
          actualPairs= reduceSHMShuffleStore.getKVPairsWithLongKeys(kvalues, vvalues,
            SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE)
          //now, push the data to the kvbuffer.
          for (i <- 0 to actualPairs-1) {
            val vv = vvalues(i).seq
            kvBuffer += Tuple2(kvalues(i), vv)
          }

          if (actualPairs < SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE ){
            endOfFetch = true;
          }
        }
        case  ShuffleDataModel.KValueTypeId.String => {
          //to retrieve a collection of {k, {v1, v2,...}} from the C++ shuffle engine where
          val kvalues = new ArrayList[JString]();
          val vvalues = new ArrayList[ArrayList[Object]]();
          //WARNING: we need to provide list based APIS, instead of []. Need to see whether it
          //can be passed around
          actualPairs= reduceSHMShuffleStore.getKVPairsWithStringKeys(kvalues, vvalues,
            SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE)
          //now, push the data to the kvbuffer.
          for (i <- 0 to actualPairs-1) {
            val vv = vvalues(i).seq
            kvBuffer += Tuple2(kvalues(i), vv)
          }

          if (actualPairs < SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE ){
            endOfFetch = true
          }
        }
        case  ShuffleDataModel.KValueTypeId.Object => {
          //to retrieve a collection of {k, {v1, v2,...}} from the C++ shuffle engine where
          val kvalues = new ArrayList[Object]();
          val vvalues = new ArrayList[ArrayList[Object]]();
          //WARNING: we need to provide list based APIS, instead of []. Need to see whether it
          //can be passed around
          actualPairs= reduceSHMShuffleStore.getKVPairs(kvalues, vvalues,
            SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE)
          //now, push the data to the kvbuffer.
          for (i <- 0 to actualPairs-1) {
            val vv = vvalues(i).seq
            kvBuffer += Tuple2(kvalues(i), vv)
          }

          if (actualPairs < SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE ){
            endOfFetch = true;
          }
        }
        case _ => {
          throw new NoSuchElementException
        }
      }


    }
    override def hasNext: Boolean = {
      //when exhausted. get to the next batch of the (K, {V}) pair.
      if (kvBuffer.size >0) {
        true;
      }
      else {
        if (!endOfFetch) {
          fetch() //make further fetch now
        }
        if (kvBuffer.size >0) {
          true;
        }
        else {
          false;
        }
      }
    }

    override def next(): (Any, Seq[Any]) = {
      kvBuffer.dequeue(); //it will return the dequeued element.
    }
  }

  private def shmFetch[T](conf:SparkConf):Iterator[T] = {
    val blockFetcherItr = new ShmShuffleFetcherIterator(conf)

    blockFetcherItr.asInstanceOf[Iterator[T]]

  }
  private def shmShuffleRead[K,V, C](conf: SparkConf,
                        aggregator: Option[Aggregator[K,V,C]]): Iterator[Product2[K,C]] = {

    val iter = shmFetch(conf)

    val aggregatedIter: Iterator[Product2[K, C]] = if (aggregator.isDefined) {

      //NOTE: why I do not have this problem in the ShmShuffleReader code?
      aggregator.get.combineMultiValuesByKey(iter, null)

    } else {
      // Convert the Product2s to pairs since this is what downstream RDDs currently expect
      // for us, we will need to revert back to <K,V> pair.
      //iter.asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
      iter.asInstanceOf[Iterator[(K, Seq[C])]].map(x=> convertValues(x._1, x._2)).flatten
    }

    //return
    aggregatedIter
  }

  private def convertValues [K, C] (k: K, multipleValues: Seq[C]): Iterator[(K,C)] ={
    val iter = multipleValues.iterator
    def iterator: Iterator[(K,C)] = new Iterator[(K,C)] {

      override def hasNext: Boolean = iter.hasNext
      override def next(): (K,C) = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        (k, iter.next())

      }
    }

    iterator
  }



  test ("to test how integer keys and associated values are retrieved by Shuffle Reader") {
    //construct the map data sources.
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "shm")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")

    //NOTE:!! to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.spill", "true")

    sc = new SparkContext("local", "test", conf)

    val agg = Some(new Aggregator[Int, RankingsClass, Int]( r => r.pagerank,
                     (pageRankV, r) => pageRankV + r.pagerank,
                     (v1, v2) => v1 + v2))
    val resultedIterator = shmShuffleRead (conf, agg);

    resultedIterator.foreach(e=> println("Int-first: " + e._1 + " second: " + e._2))

    sc.stop()
  }




  //NOTE: for more complext key is object and value is object, such as key is <int, int>, we will have to
  //close the loop from the map to the reduce, so that scala created object from the map, will then be de-serialized
  //correctly at the reduce size, so that the type can be matched at both side.  We can only do this when
  //close the loop.
}
