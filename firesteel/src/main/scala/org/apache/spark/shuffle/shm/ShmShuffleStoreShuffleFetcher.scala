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

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SharedMemoryMapOutputTracker, InterruptibleIterator, SparkEnv, TaskContext}
import org.apache.spark.{MapOutputTracker, MapOutputTrackerMaster}
import org.apache.spark.internal.Logging
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.Utils
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.ShuffleBlockFetcherIterator
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

import com.hp.hpl.firesteel.shuffle.ReduceSHMShuffleStore

/**
 * to support fetching of serialized data from the natice C++ shuffle store
 */
private[spark] object ShmShuffleStoreShuffleFetcher extends Logging {
  def fetch(
                reduceShuffleStore: ReduceSHMShuffleStore,
                shuffleId: Int,
                reduceId: Int,
                ordering:Boolean, aggregation:Boolean,
                context: TaskContext)
  : InterruptibleIterator[(Any, Any)] = {

    //contact the MapOutputTracker to get the corresponding information about map results
    logInfo("Fetching shm-shuffle outputs for shuffle %d, reduce %d".format(shuffleId, reduceId))
    val startTime = System.currentTimeMillis
    val mapOutputTracker = SparkEnv.get.mapOutputTracker
    val isLocal = Utils.isLocalMaster(SparkEnv.get.conf)

    val statuses = if (!isLocal) {
      SharedMemoryMapOutputTracker.getMapSizesByExecutorId(shuffleId, reduceId, mapOutputTracker)
    } else {
      mapOutputTracker.asInstanceOf[MapOutputTrackerMaster].shuffleStatuses.get(shuffleId) match {
        case Some(shuffleStatus) => {
          shuffleStatus.withMapStatuses { mapStatuses =>
            SharedMemoryMapOutputTracker.convertMapStatuses(shuffleId, reduceId, reduceId+1, mapStatuses)
          }
        }
        case None => SharedMemoryMapOutputTracker.convertMapStatuses(shuffleId, reduceId, reduceId+1, Array())
      }
    }

    logInfo("Fetching shm-shuffle map output location for shuffle %d, reduce %d took %d ms".format(
                shuffleId, reduceId, System.currentTimeMillis - startTime))

    for (i <- 0 until statuses.length) {
      val (blockId, regionId, chunkOffset, bucket_size, _) = statuses(i)
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

    val readMetrics =
      context.taskMetrics.createTempShuffleReadMetrics()

    val completionIter =
      CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      blockFetcherItr.map { record =>
        if (record._2.isInstanceOf[Seq[Any]]) {
          readMetrics.incRecordsRead(
            record._2.asInstanceOf[Seq[Any]].length)
        } else {
          readMetrics.incRecordsRead(1)

          if (record._2.isInstanceOf[UnsafeRow]) {
            val row = record._2.asInstanceOf[UnsafeRow]
            // read from [(Int, UnsafeRow)]:[Byte]
            readMetrics.incLocalBytesRead(Integer.BYTES + row.getSizeInBytes)
          }
        }
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    new InterruptibleIterator[(Any, Any)](context, completionIter)
  }
}
