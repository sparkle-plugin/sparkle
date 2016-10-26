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

import org.apache.spark.internal.Logging
import org.apache.spark.util.CompletionIterator
import org.apache.spark.{SharedMemoryMapOutputTracker, InterruptibleIterator, SparkEnv, TaskContext}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.ShuffleBlockFetcherIterator
import scala.collection.mutable.HashMap
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}
import scala.collection.mutable.ArrayBuffer


import com.hp.hpl.firesteel.shuffle.ReduceSHMShuffleStore

/**
 * to support fetching of serialized data from the natice C++ shuffle store
 */
private[spark] object ShmShuffleStoreShuffleFetcher extends Logging {
  def fetch[T](
                reduceShuffleStore: ReduceSHMShuffleStore,
                shuffleId: Int,
                reduceId: Int,
                ordering:Boolean, aggregation:Boolean,
                context: TaskContext)
  : Iterator[T] = {

    //contact the MapOutputTracker to get the corresponding information about map results
    logInfo("Fetching shm-shuffle outputs for shuffle %d, reduce %d".format(shuffleId, reduceId))
    val startTime = System.currentTimeMillis
    val mapOutputTracker = SparkEnv.get.mapOutputTracker
    val statuses =
      SharedMemoryMapOutputTracker.getMapSizesByExecutorId(shuffleId, reduceId, mapOutputTracker)
    logInfo("Fetching shm-shuffle map output location for shuffle %d, reduce %d took %d ms".format(
                shuffleId, reduceId, System.currentTimeMillis - startTime))

    for (i <- 0 until statuses.length) {
      val (blockId, regionId, chunkOffset, bucket_size) = statuses(i)
      if (log.isDebugEnabled) {
        val shuffleBlockId = blockId.asInstanceOf[ShuffleBlockId]
        logDebug("retrieved map output: map id: " + shuffleBlockId.mapId)
        logDebug("retrieved map output:  region id: " + regionId)
        logDebug("retrieved map output: chunkoffset: " + chunkOffset)

        logDebug("retrieved map output: bucket_size: " + bucket_size)
      }
    }

    val blockFetcherItr = if (aggregation) {
      new ShmShuffleFetcherKeyValuesIterator(
        context,
        statuses,
        reduceShuffleStore)
    }
    else {
      new ShmShuffleFetcherKeyValueIterator (
        context,
        statuses,
        reduceShuffleStore)
    }

    //the update shuffle read metetrics will zero. shm-shuffle will have its own meterics later.
    val completionIter = CompletionIterator[T, Iterator[T]](
      blockFetcherItr.asInstanceOf[Iterator[T]], { })

    new InterruptibleIterator[T](context, completionIter)
  }
}
