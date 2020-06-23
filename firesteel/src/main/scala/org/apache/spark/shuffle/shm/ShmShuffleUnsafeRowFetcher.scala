package org.apache.spark.shuffle.shm

import scala.collection.mutable.ArrayBuffer
import scala.language.existentials
import scala.collection.JavaConversions._
import scala.beans.BeanProperty

import java.util.List
import java.util.ArrayList

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}
import org.apache.spark.executor.TempShuffleReadMetrics

import com.hp.hpl.firesteel.shuffle.ShuffleDataModel.ReduceStatus
import com.hp.hpl.firesteel.shuffle.ReduceSHMShuffleStore
import com.hp.hpl.firesteel.shuffle.ShuffleDataModel

private[spark] class ShmShuffleUnsafeRowFetcher(
  context: TaskContext,
  statuses: Seq[(BlockId, Long, Long, Long, Boolean)],
  reduceShuffleStore: ReduceSHMShuffleStore,
  metrics: TempShuffleReadMetrics) {

  val mapIds = new ArrayBuffer[Int]()
  val shmRegionIds = new ArrayBuffer[Long]()
  val offsetToIndexChunks = new ArrayBuffer[Long]()
  val sizes  = new ArrayBuffer[Long]()
  statuses.foreach { case (blockId, regionId, chunkOffset, bucket_size, isPrimitiveKey) =>
    val shuffleBlockId = blockId.asInstanceOf[ShuffleBlockId]
    mapIds.append(shuffleBlockId.mapId)
    sizes.append(bucket_size)
    shmRegionIds.append(regionId)
    offsetToIndexChunks.append(chunkOffset)
  }

  val reduceStatus = new ReduceStatus(mapIds.toArray, shmRegionIds.toArray,
    offsetToIndexChunks.toArray, sizes.toArray, 0L, 0L, 0L, 0L, 0L)

  // create native store here.
  reduceShuffleStore.mergeSort(reduceStatus)

  def toIterator():Iterator[(Any, Any)] = {
    val shmMetrics = new ShmTempReadMetrics

    val itr = reduceShuffleStore.getSimpleKVPairsWithIntKeys(shmMetrics)

    metrics.incLocalBlocksFetched(shmMetrics.lbuckets)
    metrics.incLocalBytesRead(shmMetrics.lbytes)
    metrics.incRemoteBlocksFetched(shmMetrics.rbuckets)
    metrics.incRemoteBytesRead(shmMetrics.rbytes)

    reduceShuffleStore.stop
    reduceShuffleStore.shutdown

    return itr
  }
}

class ShmTempReadMetrics() {
  @BeanProperty var lbuckets = 0L
  @BeanProperty var rbuckets = 0L
  @BeanProperty var lbytes = 0L
  @BeanProperty var rbytes = 0L
}
