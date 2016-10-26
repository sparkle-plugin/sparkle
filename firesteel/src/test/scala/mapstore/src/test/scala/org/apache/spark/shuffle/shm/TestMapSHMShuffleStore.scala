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

import org.apache.spark.LocalSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkEnv
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.scalatest.FunSuite
import org.apache.spark.{SparkEnv, SparkContext, LocalSparkContext, SparkConf}
import org.apache.spark.serializer._
import com.hp.hpl.firesteel.shuffle._
import com.hp.hpl.firesteel.shuffle.ShuffleDataModel.KValueTypeId
import com.hp.hpl.firesteel.shuffle.ShuffleDataModel.KValueTypeId._
import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer._
import com.hp.hpl.firesteel.shuffle.ThreadLocalShuffleResourceHolder._

import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import java.util.ArrayList


class TestMapSHMShuffleStore extends FunSuite with LocalSparkContext with Logging {

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

  test ("loading shuffle store manager only") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "shm")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");

    sc = new SparkContext("local", "test", conf)
    val shuffleManager = SparkEnv.get.shuffleManager
    assert(shuffleManager.isInstanceOf[ShmShuffleManager])

    ShuffleStoreManager.INSTANCE.initialize(
           TestConstants.GLOBAL_HEAP_NAME, TestConstants.maxNumberOfTaskThreads, 0)
    val  nativePointer = ShuffleStoreManager.INSTANCE.getPointer()
    logInfo ("native pointer of shuffle store manager retrieved is:"
      + "0x"+ java.lang.Long.toHexString(nativePointer))

    val shuffleId = 0
    val mapId = 1
    val numberOfPartitions = 100
    val keyType = ShuffleDataModel.KValueTypeId.String


    val threadLocalResources = getThreadLocalShuffleResource(conf)
    val serializer =
      new MapSHMShuffleStore.LocalKryoByteBufferBasedSerializer (
        threadLocalResources.getSerializationResource.getKryoInstance, 
        threadLocalResources.getSerializationResource.getByteBuffer)

    val logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter()
    val ordering = true
    val mapSHMShuffleStore =
      ShuffleStoreManager.INSTANCE.createMapShuffleStore(
          threadLocalResources.getSerializationResource.getKryoInstance,
          threadLocalResources.getSerializationResource.getByteBuffer,
          logicalThreadId, 
          shuffleId, mapId, numberOfPartitions, keyType,
          TestConstants.SIZE_OF_BATCH_SERIALIZATION, 
          ordering
       )

    mapSHMShuffleStore.stop()
    mapSHMShuffleStore.shutdown()
    ShuffleStoreManager.INSTANCE.shutdown()

    sc.stop()
  }

  //use variable sc instead.
  test ("loading shuffle store manager with serialization of map data and class registration") {

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "shm")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");

    sc = new SparkContext("local", "test", conf)
    val shuffleManager = SparkEnv.get.shuffleManager
    assert(shuffleManager.isInstanceOf[ShmShuffleManager])

    ShuffleStoreManager.INSTANCE.initialize(
          TestConstants.GLOBAL_HEAP_NAME, TestConstants.maxNumberOfTaskThreads, 0)
    val  nativePointer = ShuffleStoreManager.INSTANCE.getPointer()
    logInfo ("native pointer of shuffle store manager retrieved is:"
      + "0x"+ java.lang.Long.toHexString(nativePointer))

    val shuffleId = 0
    val mapId = 1
    val numberOfPartitions = 100
    val keyType = ShuffleDataModel.KValueTypeId.Int

    val threadLocalResources = getThreadLocalShuffleResource(conf)
    val serializer =
      new MapSHMShuffleStore.LocalKryoByteBufferBasedSerializer (
        threadLocalResources.getSerializationResource.getKryoInstance,
        threadLocalResources.getSerializationResource.getByteBuffer)

    val logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter()
    val ordering = true
    val mapSHMShuffleStore =
      ShuffleStoreManager.INSTANCE.createMapShuffleStore(
          threadLocalResources.getSerializationResource.getKryoInstance,
          threadLocalResources.getSerializationResource.getByteBuffer,
          logicalThreadId, 
          shuffleId, mapId, numberOfPartitions, keyType,
          TestConstants.SIZE_OF_BATCH_SERIALIZATION, 
          ordering
        )

    val numberOfVs = 10
    val testObjects = new ArrayList[RankingsClass] ()
    val partitions = new ArrayList[Int]()
    val kvalues = new ArrayList[Int] ()
    val voffsets = new ArrayList[Int] ()
    for (i <- 0 to numberOfVs-1) {
          val obj = new RankingsClass(i, "hello" +i, i+1)
          testObjects.add(obj)
          voffsets.add(0)
          partitions.add( i%2 )
          kvalues.add (i)
    }

    for (i <- 0 to numberOfVs-1) {
      mapSHMShuffleStore.serializeKVPair(kvalues.get(i), testObjects.get(i),partitions.get(i),
                                            i, KValueTypeId.Int.getState())
    }

    val numberOfPairs = numberOfVs;
    mapSHMShuffleStore.storeKVPairs(numberOfPairs, KValueTypeId.Int.getState())

    val mapStatusResult = mapSHMShuffleStore.sortAndStore();

    logInfo("map status region id: " + mapStatusResult.getRegionIdOfIndexBucket())
    logInfo ("map status offset to index chunk: 0x "
          + java.lang.Long.toHexString(mapStatusResult.getOffsetOfIndexBucket()))
     val buckets = mapStatusResult.getMapStatus()

    if (buckets != null) {
          for (i <- 0 to buckets.length-1) {
            logInfo ("map status, bucket: " + i + " has size: " + buckets(i))
          }
     }
     else {
          logInfo("map status buckets length is null.")
     }
    mapSHMShuffleStore.stop()
    mapSHMShuffleStore.shutdown()
    ShuffleStoreManager.INSTANCE.shutdown()

    sc.stop()
  }

  //use variable sc instead.
  test ("loading shuffle store manager with serialization of map data without class registration") {

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "shm")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");

    sc = new SparkContext("local", "test", conf)
    val shuffleManager = SparkEnv.get.shuffleManager
    assert(shuffleManager.isInstanceOf[ShmShuffleManager])

    ShuffleStoreManager.INSTANCE.initialize(
           TestConstants.GLOBAL_HEAP_NAME, TestConstants.maxNumberOfTaskThreads, 0)
    val  nativePointer = ShuffleStoreManager.INSTANCE.getPointer()
    logInfo ("native pointer of shuffle store manager retrieved is:"
      + "0x"+ java.lang.Long.toHexString(nativePointer))

    val shuffleId = 0
    val mapId = 1
    val numberOfPartitions = 100
    val keyType = ShuffleDataModel.KValueTypeId.Int

    val threadLocalResources = getThreadLocalShuffleResource(conf)
    val serializer =
      new MapSHMShuffleStore.LocalKryoByteBufferBasedSerializer (
        threadLocalResources.getSerializationResource.getKryoInstance,
        threadLocalResources.getSerializationResource.getByteBuffer)

    val logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter()
    val ordering = true

    val mapSHMShuffleStore =
      ShuffleStoreManager.INSTANCE.createMapShuffleStore(
        threadLocalResources.getSerializationResource.getKryoInstance,
        threadLocalResources.getSerializationResource.getByteBuffer,
        logicalThreadId, 
        shuffleId, mapId, numberOfPartitions, keyType,
        TestConstants.SIZE_OF_BATCH_SERIALIZATION, 
        ordering
      )

    val numberOfVs = 10
    val testObjects = new ArrayList[RankingsClass] ()
    val partitions = new ArrayList[Int]()
    val kvalues = new ArrayList[Int] ()
    val voffsets = new ArrayList[Int] ()
    for (i <- 0 to numberOfVs-1) {
      val obj = new RankingsClass(i, "hello" +i, i+1)
      testObjects.add(obj)
      voffsets.add(0)
      partitions.add( i%2 )
      kvalues.add (i)
    }

    for (i <- 0 to numberOfVs-1) {
      mapSHMShuffleStore.serializeKVPair(kvalues.get(i), testObjects.get(i),partitions.get(i),
                                            i, KValueTypeId.Int.getState())
    }

    val numberOfPairs = numberOfVs;
    mapSHMShuffleStore.storeKVPairs(numberOfPairs, KValueTypeId.Int.getState())

    val mapStatusResult = mapSHMShuffleStore.sortAndStore();

    logInfo("map status region id: " + mapStatusResult.getRegionIdOfIndexBucket())
    logInfo ("map status offset to index chunk: 0x "
      + java.lang.Long.toHexString(mapStatusResult.getOffsetOfIndexBucket()))
    val buckets = mapStatusResult.getMapStatus()

    if (buckets != null) {
      for (i <- 0 to buckets.length-1) {
        logInfo ("map status, bucket: " + i + " has size: " + buckets(i))
      }
    }
    else {
      logInfo("map status buckets length is null.")
    }

    mapSHMShuffleStore.stop()
    mapSHMShuffleStore.shutdown()
    ShuffleStoreManager.INSTANCE.shutdown()
    sc.stop()
  }


  //use variable sc instead.
  test ("loading shuffle store manager with serialization of map data with repeated invocation") {

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "shm")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");

    sc = new SparkContext("local", "test", conf)
    val shuffleManager = SparkEnv.get.shuffleManager
    assert(shuffleManager.isInstanceOf[ShmShuffleManager])

    ShuffleStoreManager.INSTANCE.initialize(
           TestConstants.GLOBAL_HEAP_NAME, TestConstants.maxNumberOfTaskThreads, 0)
    val  nativePointer = ShuffleStoreManager.INSTANCE.getPointer()
    logInfo ("native pointer of shuffle store manager retrieved is:"
      + "0x"+ java.lang.Long.toHexString(nativePointer))

    val shuffleId = 0
    val mapId = 1
    val numberOfPartitions = 100
    val keyType = ShuffleDataModel.KValueTypeId.Int

    val repeatedNumber = 2
    for (k <- 0 to repeatedNumber -1) {
      logInfo ("**************repeat number*********************" + k)

      val threadLocalResources = getThreadLocalShuffleResource(conf)
      val serializer =
        new MapSHMShuffleStore.LocalKryoByteBufferBasedSerializer(
          threadLocalResources.getSerializationResource.getKryoInstance, 
          threadLocalResources.getSerializationResource.getByteBuffer)

      val logicalThreadId =  ShuffleStoreManager.INSTANCE.getlogicalThreadCounter()
      val ordering = true

      val mapSHMShuffleStore =
        ShuffleStoreManager.INSTANCE.createMapShuffleStore(
          threadLocalResources.getSerializationResource.getKryoInstance,
          threadLocalResources.getSerializationResource.getByteBuffer,
          logicalThreadId, 
          shuffleId, mapId, numberOfPartitions, keyType,
          TestConstants.SIZE_OF_BATCH_SERIALIZATION, 
          ordering
          )

      val numberOfVs = 10
      val testObjects = new ArrayList[RankingsClass]()
      val partitions = new ArrayList[Int]()
      val kvalues = new ArrayList[Int]()
      val voffsets = new ArrayList[Int]()
      for (i <- 0 to numberOfVs - 1) {
        val obj = new RankingsClass(i, "hello" + i, i + 1)
        testObjects.add(obj)
        voffsets.add(0)
        partitions.add(i % 2)
        kvalues.add(i)
      }

      for (i <- 0 to numberOfVs-1) {
        mapSHMShuffleStore.serializeKVPair(kvalues.get(i), testObjects.get(i),partitions.get(i),
                                            i, KValueTypeId.Int.getState())
      }

      val numberOfPairs = numberOfVs;
      mapSHMShuffleStore.storeKVPairs(numberOfPairs, KValueTypeId.Int.getState())

      val mapStatusResult = mapSHMShuffleStore.sortAndStore();

      logInfo("map status region id: " + mapStatusResult.getRegionIdOfIndexBucket())
      logInfo("map status offset to index chunk: 0x "
        + java.lang.Long.toHexString(mapStatusResult.getOffsetOfIndexBucket()))
      val buckets = mapStatusResult.getMapStatus()

      if (buckets != null) {
        for (i <- 0 to buckets.length - 1) {
          logInfo("map status, bucket: " + i + " has size: " + buckets(i))
        }
      }
      else {
        logInfo("map status buckets length is null.")
      }

      mapSHMShuffleStore.stop()
      mapSHMShuffleStore.shutdown()
    }


    ShuffleStoreManager.INSTANCE.shutdown()
    sc.stop()
  }
}

