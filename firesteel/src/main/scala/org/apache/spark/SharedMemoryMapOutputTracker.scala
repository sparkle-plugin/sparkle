/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.lang.{Integer => JInt}

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.control.NonFatal

import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.MetadataFetchFailedException
import org.apache.spark.storage.{ShuffleBlockId, BlockId, BlockManagerId}


/**
 * This is a wrapper of Spark's MapOutputTracker, This [[SharedMemoryMapOutputTracker]] is used
 * to get regionId and chunkOffset encoded in [[SharedMemoryCompressedMapStatus]]
 */
object SharedMemoryMapOutputTracker {

  def getMapSizesByExecutorId(shuffleId: Int, reduceId: Int, mapOutputTracker: MapOutputTracker)
      : Seq[(BlockId, Long, Long, Long)] = {

    getMapSizesByExecutorId(shuffleId, reduceId, reduceId + 1, mapOutputTracker)

  }

  def getMapSizesByExecutorId(
      shuffleId: Int,
      startPartition: Int,
      endPartition: Int,
      mapOutputTracker: MapOutputTracker)
    : Seq[(BlockId, Long, Long, Long)]= {

    val statuses = try {
      val method =
        mapOutputTracker.getClass.getDeclaredMethod("getStatuses", classOf[Int])
      method.setAccessible(true)
      method.invoke(mapOutputTracker, new JInt(shuffleId)).asInstanceOf[Array[MapStatus]]
    } catch {
      case NonFatal(e) => {
        throw new SparkException("MapOutputTracker getStatuses invoke error", e)
      }
    }

    statuses.synchronized {
      return convertMapStatuses(shuffleId, startPartition, endPartition, statuses)
    }
  }

  private def convertMapStatuses(
      shuffleId: Int,
      startPartition: Int,
      endPartition: Int,
      statuses: Array[MapStatus])
    : Seq[(BlockId, Long, Long, Long)] = {
    assert(statuses != null)

    //Each ArrayBuffer Element contains: block id, region id of map task, chunk offset of 
    //map task, the map bucket's size.
    val results = new ArrayBuffer[(BlockId, Long, Long, Long)]()

    for ((status, mapId) <- statuses.zipWithIndex) {
      if (status == null) {
        throw new MetadataFetchFailedException(
          shuffleId, startPartition, s"Missing an output location for shuffle $shuffleId")
      } else {
        val shmMapStatus = status.asInstanceOf[SharedMemoryAttributes]

        for (part <- startPartition until endPartition) {
         val shuffleBlockId: BlockId = new ShuffleBlockId(shuffleId, mapId, part)
         results +=
            ((shuffleBlockId,
               shmMapStatus.regionId, shmMapStatus.chunkOffset,
               status.getSizeForBlock(part)))
        }
      }
    }

    results.toSeq
  }
}
