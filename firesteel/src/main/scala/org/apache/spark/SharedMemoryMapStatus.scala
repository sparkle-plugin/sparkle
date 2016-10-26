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

import org.roaringbitmap.RoaringBitmap

import java.io.{ObjectInput, ObjectOutput}

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{CompressedMapStatus, HighlyCompressedMapStatus, MapStatus}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

trait SharedMemoryAttributes {
  /*to retrieve the region id associated with the map task*/
  def regionId: Long

  /*to retrieve the chunk offset associated with the map task*/
  def chunkOffset: Long

}

/**
 * Define our own [[MapStatus]] which extends Spark's [[CompressedMapStatus]] with two more
 * fields: regionId and chunkOffset. We will use this [[MapStatus]] in shm shuffle to manage
 * the [[BlockManagerId]] to regionId and chunkOffset mapping relation.
 */
class SharedMemoryCompressedMapStatus(
    private[this] var loc: BlockManagerId,
    private[this] var compressedSizes: Array[Byte],
    private[this] var _regionId: Long,
    private[this] var _chunkOffset: Long) extends CompressedMapStatus(loc, compressedSizes)
                                          with SharedMemoryAttributes {

  def this(
      loc: BlockManagerId,
      uncompressedSize: Array[Long],
      regionId: Long,
      chunkOffset: Long) =
    this(loc, uncompressedSize.map(MapStatus.compressSize), regionId, chunkOffset)

  protected def this() = this(null, null.asInstanceOf[Array[Byte]], 0L, 0L)

  override def regionId: Long = _regionId

  override def chunkOffset: Long = _chunkOffset

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    super.writeExternal(out)
    out.writeLong(_regionId)
    out.writeLong(_chunkOffset)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    super.readExternal(in)
    _regionId = in.readLong()
    _chunkOffset = in.readLong()
  }
}


class SharedMemoryHighlyCompressedMapStatus(
    private[this] var loc: BlockManagerId,
    private[this] var numNonEmptyBlocks: Int, 
    private[this] var emptyBlocks: RoaringBitmap,
    private[this] var avgSize: Long, 
    private[this] var _regionId: Long,
    private[this] var _chunkOffset: Long) 
    extends HighlyCompressedMapStatus 
    with SharedMemoryAttributes {


  // loc could be null when the default constructor is called during deserialization
  require(loc == null || avgSize > 0 || numNonEmptyBlocks == 0,
     "Average size can only be zero for map stages that produced no output")

  protected def this() = this(null, -1, null, -1, 0L, 0L)

  override def regionId: Long = _regionId

  override def chunkOffset: Long = _chunkOffset


  override def location: BlockManagerId = loc

  override def getSizeForBlock(reduceId: Int): Long = {
    if (emptyBlocks.contains(reduceId)) {
      0
    } else {
      avgSize
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    emptyBlocks.writeExternal(out)
    out.writeLong(avgSize)

    out.writeLong(_regionId)
    out.writeLong(_chunkOffset)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    emptyBlocks = new RoaringBitmap()
    emptyBlocks.readExternal(in)
    avgSize = in.readLong()

    _regionId = in.readLong()
    _chunkOffset = in.readLong()
  }
}


object SharedMemoryMapStatus extends Logging {
  def apply(
      loc: BlockManagerId,
      uncompressedSizes: Array[Long],
      regionId: Long,
      chunkOffset: Long): MapStatus = {
    if (uncompressedSizes.length > 2000) {
      //logInfo ("map bucket size is: " +
      //    uncompressedSizes.length + " with highly compressed map status chosen")
      SharedMemoryHighlyCompressedMapStatus(loc, uncompressedSizes, regionId, chunkOffset)
    } else  {
      SharedMemoryCompressedMapStatus(loc, uncompressedSizes, regionId, chunkOffset)
    }
      
  }
}

object SharedMemoryCompressedMapStatus {
  def apply(
      loc: BlockManagerId,
      uncompressedSizes: Array[Long],
      regionId: Long,
      chunkOffset: Long): MapStatus = {
    new SharedMemoryCompressedMapStatus(loc, uncompressedSizes, regionId, chunkOffset)
  }
}



object SharedMemoryHighlyCompressedMapStatus {
  def apply(
      loc: BlockManagerId,
      uncompressedSizes: Array[Long],
      regionId: Long,
      chunkOffset: Long): MapStatus = {

      // We must keep track of which blocks are empty so that we don't report a zero-sized
      // block as being non-empty (or vice-versa) when using the average block size.
      var i = 0
      var numNonEmptyBlocks: Int = 0
      var totalSize: Long = 0
      // From a compression standpoint, it shouldn't matter whether we track empty or non-empty
      // blocks. From a performance standpoint, we benefit from tracking empty blocks because
      // we expect that there will be far fewer of them, so we will perform fewer bitmap insertions.
      val emptyBlocks = new RoaringBitmap()
      val totalNumBlocks = uncompressedSizes.length
      while (i < totalNumBlocks) {
        var size = uncompressedSizes(i)
        if (size > 0) {
          numNonEmptyBlocks += 1
          totalSize += size
        } else {
          emptyBlocks.add(i)
        }
        i += 1
      }
      val avgSize = if (numNonEmptyBlocks > 0) {
        totalSize / numNonEmptyBlocks
      } else {
        0
      }

      emptyBlocks.trim()
      emptyBlocks.runOptimize()

      new SharedMemoryHighlyCompressedMapStatus(
             loc, numNonEmptyBlocks,emptyBlocks, avgSize, regionId, chunkOffset)
  }
}
