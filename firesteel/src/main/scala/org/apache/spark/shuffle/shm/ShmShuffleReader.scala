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

import org.apache.spark.{InterruptibleIterator, TaskContext}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReader}
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.execution.UnsafeRowSerializer

import com.hp.hpl.firesteel.shuffle.{ThreadLocalShuffleResourceHolder, ShuffleStoreManager}
import com.hp.hpl.firesteel.shuffle.ReduceSHMShuffleStore
import com.hp.hpl.firesteel.shuffle.ThreadLocalShuffleResourceHolder._

private[spark] class ShmShuffleReader[K, C](shuffleStoreMgr:ShuffleStoreManager,
                                            handle: BaseShuffleHandle[K, _, C],
                                            startPartition: Int, endPartition: Int,
                                            context: TaskContext)
  extends ShuffleReader[K, C] with Logging {

  require (endPartition == startPartition + 1,
     "Shm shuffle currently only supports fetching one partition")

  private val dep = handle.dependency
  private val numReducePartitions = dep.partitioner.numPartitions
  private val shuffleId = handle.shuffleId
  private val reduceId = startPartition

  //to check whether ordering and aggregation is defined.
  private val ordering = dep.keyOrdering.isDefined
  //no need to determine aggregation at the map side
  private val aggregation = dep.aggregator.isDefined

  //we will introduce a Spark configuraton parameter for this one. and serialziation and
  //deserialization buffers should be the same size, as we need to support thread re-use.
  private val SERIALIZATION_BUFFER_SIZE =
            SparkEnv.get.conf.getInt("spark.shuffle.shm.serializer.buffer.max.mb", 10)*1024*1024

  //we will pool the kryo instance and ByteBuffer instance later.
  private val threadLocalShuffleResource = getThreadLocalShuffleResource()
  //per shuffle task
  private val reduceShuffleStore =
               shuffleStoreMgr.createReduceShuffleStore(
                    threadLocalShuffleResource.getSerializationResource().getKryoInstance(),
                    threadLocalShuffleResource.getSerializationResource().getByteBuffer(),
                    shuffleId, reduceId, numReducePartitions,
                    ordering, aggregation)
                    //true to allow sort/merge-sort with ordering.

  //this one is different, as when the Reducer starts. Normally when all of the required logical
  //threads should be launched already.
  private def getThreadLocalShuffleResource():
                                            ThreadLocalShuffleResourceHolder.ShuffleResource = {

      val resourceHolder= new ThreadLocalShuffleResourceHolder()
      var shuffleResource = resourceHolder.getResource()

      if (shuffleResource == null) {
        val kryoInstance =  new KryoSerializer(SparkEnv.get.conf).newKryo(); //per-thread
        val serializationBuffer = ByteBuffer.allocateDirect(SERIALIZATION_BUFFER_SIZE)
        if (serializationBuffer.capacity() != SERIALIZATION_BUFFER_SIZE ) {
          logError("Reduce Thread: " + Thread.currentThread().getId
            + " created serialization buffer with size: "
            + serializationBuffer.capacity()
            + ": FAILED to match: " + SERIALIZATION_BUFFER_SIZE)
        }
        else {
          logInfo("Reduce Thread: " + + Thread.currentThread().getId
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


  override  def read(): Iterator[Product2[K,C]] = {
    //initialize the store here, as we have the known information for initialization.
    reduceShuffleStore.initialize(shuffleId, reduceId,
                        numReducePartitions, ordering, aggregation)
                        //true to allow sort/merge-sort with ordering.
    reduceShuffleStore.setEnableJniCallback(
      SparkEnv.get.conf.getBoolean("spark.shm.enable.jni.callback", false));

    if (dep.serializer.isInstanceOf[UnsafeRowSerializer]) {
      reduceShuffleStore.isUnsafeRow = true
      reduceShuffleStore.setUnsafeRowSerializer(dep.serializer)
    }

    val iter =
      ShmShuffleStoreShuffleFetcher.fetch(reduceShuffleStore,shuffleId, reduceId,
                  ordering, aggregation, context)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      logInfo("ShmShuffleReader, aggregated result for downstream processing")

      val keyValuesIterator = iter.asInstanceOf[Iterator[(K, Seq[Nothing])]]

      keyValuesIterator.map(keyValues => {
        val valueIter = keyValues._2.iterator

        var combinedValue = dep.aggregator.get.createCombiner(valueIter.next)
        while (valueIter.hasNext) {
          combinedValue =
            dep.aggregator.get.mergeValue(combinedValue, valueIter.next)
        }

        (keyValues._1, combinedValue)
      })
    } else if (dep.aggregator.isEmpty && dep.mapSideCombine) {
      throw new IllegalStateException("Aggregator is empty for map-side combine")
    } else {
      //no aggregation,we already retrieve pair-byte-pair
      if (dep.keyOrdering.isDefined) {
        logInfo("ShmShuffleReader, merge-sort for downstream processing")
      } else {
        logInfo("ShmShuffleReader, pass-through for downstream processing")
      }

      iter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    aggregatedIter match {
      case _: InterruptibleIterator[Product2[K, C]] => aggregatedIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator.
        new InterruptibleIterator[Product2[K, C]](context, aggregatedIter)
    }
  }

  def stop(): Unit = {
    //NOTE: necessary clean up at the side reduce-shuffle-store, as some DRAM data structures
    reduceShuffleStore.stop()
    //then shutdown, as we should not have resources pending for reclaim after the shuffle fist.
    //This is different from map-side shuffle store, whose shutdown will be deferred until
    //shuffle un-register.
    reduceShuffleStore.shutdown()
  }
}
