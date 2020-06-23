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

import java.util.Comparator
import java.nio.ByteBuffer

import scala.annotation.switch
import scala.collection.mutable.Map
import scala.collection.mutable.OpenHashMap
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle._
import org.apache.spark.{SharedMemoryMapStatus, TaskContext, SparkEnv}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.serializer.SerializationStream
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

import com.esotericsoftware.kryo.io.ByteBufferOutput

import com.fasterxml.jackson.databind.util.ByteBufferBackedOutputStream

import com.hp.hpl.firesteel.shuffle._
import com.hp.hpl.firesteel.shuffle.ThreadLocalShuffleResourceHolder._

/**
 * Shuffle writer designed for shared-memory based access.
 */
private[spark] class ShmShuffleWriter[K, V]( shuffleStoreMgr:ShuffleStoreManager,
                                             handle: BaseShuffleHandle[K, V, _],
                                             mapId: Int,
                                             context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  //to mimic what is in SortShuffleWriter
  private var stopping = false
  private var mapStatus: MapStatus= null

  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics

  private val dep = handle.dependency
  private val numOutputSplits = dep.partitioner.numPartitions

  private val shuffleId=handle.shuffleId
  private val numberOfPartitions=handle.dependency.partitioner.numPartitions

  //we will introduce a Spark configuraton parameter for this one. and serialziation and
  //deserialization buffers should be the same size, as we need to support thread re-use.
  private val SERIALIZATION_BUFFER_SIZE = 
            SparkEnv.get.conf.getInt("spark.shuffle.shm.serializer.buffer.max.mb", 10)*1024*1024

  //we batch 5000 records before we go to the write.This can be configurable later.
  private val SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE = 2000
  //we will pool the kryo instance and ByteBuffer instance later.

  private val SHUFFLE_STORE_ENABLE_JNI_CALLBACK =
    SparkEnv.get.conf.getBoolean("spark.shm.enable.jni.callback", false)

  //per shuffle task resource
  private val threadLocalShuffleResource = getThreadLocalShuffleResource()
  private var mapShuffleStore = null.asInstanceOf[MapSHMShuffleStore]

  //to record the map output status that will be passed to the scheduler and map output tracker.
  private val blockManager = SparkEnv.get.blockManager

  //to check whether ordering and aggregation is defined.
  private val ordering = dep.keyOrdering.isDefined
  //no need to determine aggregation at the map side
  //private val aggregation = dep.aggregator.isDefined

  //record the kvalue's type id
  private var kvalueTypeId = ShuffleDataModel.KValueTypeId.Unknown

  private def getThreadLocalShuffleResource():
                            ThreadLocalShuffleResourceHolder.ShuffleResource = {
       val resourceHolder= new ThreadLocalShuffleResourceHolder()
       var shuffleResource = resourceHolder.getResource()
   
       if (shuffleResource == null) {
          //still at the early thread launching phase for the executor, so create new resource
          val kryoInstance =  new KryoSerializer(SparkEnv.get.conf).newKryo(); //per-thread
          val serializationBuffer = ByteBuffer.allocateDirect(SERIALIZATION_BUFFER_SIZE)
          if (serializationBuffer.capacity() != SERIALIZATION_BUFFER_SIZE ) {
            logError(" Map Thread: " + Thread.currentThread().getId
              + " created serialization buffer with size: "
              + serializationBuffer.capacity()
              + ": FAILED to match: " + SERIALIZATION_BUFFER_SIZE)
          }
          else {
            logInfo(" Map Thread: " +  Thread.currentThread().getId
              + " created the serialization buffer with size: "
              + SERIALIZATION_BUFFER_SIZE + ": SUCCESS")
          }
          //add a logical thread id
          val logicalThreadId = ShuffleStoreManager.INSTANCE.getlogicalThreadCounter ()
          shuffleResource = new ShuffleResource(
              new ReusableSerializationResource (kryoInstance, serializationBuffer),
              logicalThreadId)
          
          resourceHolder.initialize (shuffleResource)

       }

       shuffleResource
  }

  private def createMapShuffleStore[K](firstK: K): MapSHMShuffleStore ={
    val serializationResource = threadLocalShuffleResource.getSerializationResource()

    mapShuffleStore = {
      firstK match {
        case intValue: Int => {
          kvalueTypeId = ShuffleDataModel.KValueTypeId.Int
          shuffleStoreMgr.createMapShuffleStore(
            serializationResource.getKryoInstance(),
            serializationResource.getByteBuffer(),
            threadLocalShuffleResource.getLogicalThreadId,
            shuffleId, mapId,numberOfPartitions,
            ShuffleDataModel.KValueTypeId.Int, SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE,
            ordering) //true to allow sort/merge-sort with ordering.
        }
        case longValue: Long => {
          kvalueTypeId = ShuffleDataModel.KValueTypeId.Long
          shuffleStoreMgr.createMapShuffleStore(
            serializationResource.getKryoInstance(),
            serializationResource.getByteBuffer(),
            threadLocalShuffleResource.getLogicalThreadId,
            shuffleId, mapId,numberOfPartitions,
            ShuffleDataModel.KValueTypeId.Long, SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE,
            ordering) //true to allow sort/merge-sort with ordering.
        }
        case floatValue: Float => {
          kvalueTypeId = ShuffleDataModel.KValueTypeId.Float
          shuffleStoreMgr.createMapShuffleStore(
            serializationResource.getKryoInstance(),
            serializationResource.getByteBuffer(),
            threadLocalShuffleResource.getLogicalThreadId,
            shuffleId, mapId,numberOfPartitions,
            ShuffleDataModel.KValueTypeId.Float, SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE,
            ordering) //true to allow sort/merge-sort with ordering.
        }
        case byteArrayValue: Array[Byte] => {
          kvalueTypeId = ShuffleDataModel.KValueTypeId.ByteArray
          shuffleStoreMgr.createMapShuffleStore(
            serializationResource.getKryoInstance(),
            serializationResource.getByteBuffer(),
            threadLocalShuffleResource.getLogicalThreadId,
            shuffleId, mapId,numberOfPartitions,
            ShuffleDataModel.KValueTypeId.ByteArray, SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE,
            ordering) //true to allow sort/merge-sort with ordering.
        }
        case _ =>  {
          kvalueTypeId = ShuffleDataModel.KValueTypeId.Object
          shuffleStoreMgr.createMapShuffleStore(
            serializationResource.getKryoInstance(),
            serializationResource.getByteBuffer(),
            threadLocalShuffleResource.getLogicalThreadId,
            shuffleId, mapId,numberOfPartitions,
            ShuffleDataModel.KValueTypeId.Object,SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE,
            ordering) //true to allow sort/merge-sort with ordering.
        }
      }
    }

    mapShuffleStore
  }


  override def write (records: Iterator[Product2[K,V]]): Unit = {
    var kv: Product2[K, V] = null
    val bit = records.buffered
    var totalRecordCount: Long = 0L
    var partitionedBuffer = new OpenHashMap[Int, Buffer[AnyRef]](numberOfPartitions)

    var useJni = true

    //to handle the case where the partition has zero size.
    if (bit.hasNext) {
      val firstKV = bit.head //not advance to the next.

      mapShuffleStore = createMapShuffleStore(firstKV._1)
      // Once the store is created, we can identify the type of keys.
      if (kvalueTypeId == ShuffleDataModel.KValueTypeId.Object) {
        useJni = SHUFFLE_STORE_ENABLE_JNI_CALLBACK
      }
      mapShuffleStore.setEnableJniCallback(useJni)

      if (firstKV._2.isInstanceOf[UnsafeRow]) {
        mapShuffleStore.isUnsafeRow = true

        val resource =
          threadLocalShuffleResource.getSerializationResource()
        mapShuffleStore.setUnsafeRowSerializer(
          dep.serializer.newInstance.serializeStream(
            new ByteBufferBackedOutputStream(resource.getByteBuffer)))
      }

      var count: Int = 0
      //NOTE: we can not use records for iteration--It will miss the first value.
      val scode=kvalueTypeId.getState()

      while (bit.hasNext) {
        kv = bit.next()
        val partitionId = handle.dependency.partitioner.getPartition(kv._1)

        //NOTE: we will need to check whether overloading method works in this case.
        //NOTE: Obj && no jni callbacks -> just store in buffer.
        if (useJni) {
          mapShuffleStore.serializeKVPair(kv._1, kv._2, partitionId, count, scode)
        } else {
          var buffer = partitionedBuffer.getOrElseUpdate(partitionId, new ArrayBuffer[AnyRef]())
          buffer.append(kv._1.asInstanceOf[AnyRef])
          buffer.append(kv._2.asInstanceOf[AnyRef])
        }

        count = count + 1
        if (count == SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE) {
          mapShuffleStore.storeKVPairs(count, kvalueTypeId.getState())

          totalRecordCount += count
          count = 0 //reset
        }
      }

      if (count > 0) {
        //some lefover
        mapShuffleStore.storeKVPairs(count, kvalueTypeId.getState())
        totalRecordCount += count
      }
    }
    else {
      val firstKV = (0, 0) //make it an integer.
      //NOTE: such that map shuffle store is created with an integer key type. but the C++ engine
      //that pick up the type should not use the channel (bucket) that has zero size bucket to
      //determine the type.
      mapShuffleStore = createMapShuffleStore(firstKV._1)
    }

    if (mapShuffleStore.isUnsafeRow) {
      mapShuffleStore.finalizeUnsafeRowSerializer()
    }

    //when done, issue sort and store and get the map status information
    // Obj && no jni callbacks -> sort, serialize, store
    val mapStatusResult =
      if (useJni) mapShuffleStore.sortAndStore() else serializeAndStore(partitionedBuffer)
    writeMetrics.incWriteTime(mapStatusResult.getWrittenTimeNs)

    logInfo( "store id" + mapShuffleStore.getStoreId
         + " shm-shuffle map status region id: " + mapStatusResult.getRegionIdOfIndexBucket())
    logInfo("store id" + mapShuffleStore.getStoreId
         + " shm-shuffle map status offset to index chunk: 0x "
                      + java.lang.Long.toHexString(mapStatusResult.getOffsetOfIndexBucket()))
    val blockManagerId = blockManager.shuffleServerId

    val partitionLengths= mapStatusResult.getMapStatus ()
    writeMetrics.incBytesWritten(partitionLengths.sum)
    writeMetrics.incRecordsWritten(totalRecordCount)
    mapStatus = SharedMemoryMapStatus(blockManagerId,
      partitionLengths,
      mapStatusResult.getRegionIdOfIndexBucket,
      mapStatusResult.getOffsetOfIndexBucket,
      kvalueTypeId != ShuffleDataModel.KValueTypeId.Object)
  }

  // Obj && no jni callbacks -> serialize, then store.
  private def serializeAndStore(partitionedBuffer:Map[Int, Buffer[AnyRef]]):
      ShuffleDataModel.MapStatus = {
    val resource =
      threadLocalShuffleResource.getSerializationResource

    val kryo = resource.getKryoInstance
    val output = new ByteBufferOutput(resource.getByteBuffer)

    // empty the buffer that may contain previous serialized records.
    output.clear

    var partitionLengths = new Array[Int](numberOfPartitions)
    for (id <- partitionedBuffer.keySet.toSeq.sorted) {
      var firstPos = output.getByteBuffer.position()

      var it = partitionedBuffer(id).iterator
      while (it.hasNext) {
        var key = it.next
        var value = it.next
        kryo.writeClassAndObject(output, key)
        kryo.writeClassAndObject(output, value)
      }

      partitionLengths(id) = output.getByteBuffer.position() - firstPos
    }

    val res = mapShuffleStore.writeToHeap(output.getByteBuffer, partitionLengths)

    output.clear

    return res
  }

  /**
   * stop will force the completion of the shuffle write and return
   * MapStatus information.
   *
   * We will produce MapStatus information exactly like what is today, as otherwise, we will have
   * to change MapOutputTracker logic and corresponding data structures to do so.
   *
   * @param success when we get here, we already know that we can stop it with success or not.
   * @return record map shuffle status information that will be sent to the job scheduler
   */
  override def stop(success: Boolean): Option[MapStatus]= {
      try {
        if (stopping) {
          return None
        }
        stopping = true
        if (success) {
          return Option (mapStatus)
        }
        else {
          //TODO: we will need to remove the NVM data produced by this failed Map task.
          return None
        }
      }finally {
        //In sort-based shuffle, current sort shuffle writer stop the sorter, which is to clean up
        //the intermediate files
        //For shm-based shuffle, to shutdown the shuffle store for this map task (basically to
        // clean up the occupied DRAM based resource. but not NVM-based resource, which will be
        // cleaned up during unregister the shuffle.)
        val startTime = System.nanoTime
        mapShuffleStore.stop();
        writeMetrics.incWriteTime(System.nanoTime - startTime)
      }
  }
}
