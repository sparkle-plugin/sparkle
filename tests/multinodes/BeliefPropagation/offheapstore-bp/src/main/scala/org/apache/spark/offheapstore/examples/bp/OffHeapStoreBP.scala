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

// Note: OffHeapStoreBP belongs to graphx package in order to use a private class, GraphXPrimitiveKeyOpenHashMap
package org.apache.spark.graphx

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkContext, Partitioner, HashPartitioner, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.offheapstore.impl._
import org.apache.spark.graphx.impl._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.rdd._
import org.apache.spark.util.collection.{PrimitiveVector, BitSet, Sorter, OpenHashSet}
import scala.reflect._
import com.hp.hpl.firesteel.offheapstore._
import com.hp.hpl.edgepartition.EdgeMessageComputeAggregate
import scala.sys.process._

object OffHeapStoreBP extends Logging {

  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
     (vertices: RDD[(VertexId, VD)],
      edges: RDD[Edge[ED]],
      maxIterations: Int = Int.MaxValue)
     (vprog: (VertexId, VD, A) => VD,
      mapFunc: (EdgeTriplet[VD, ED], Int, UnsafeDoubleArray) => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A)
    : RDD[(VertexId, (Double, Double))] =
  {

    var maxDiff:Double = 1.0
    var i = 0

    val vertexArr:Array[Array[AttributeTable]] = vertexTblAddr.value
    val edgeArr:Array[Array[AttributeTable]] = edgeTblAddr.value
    val verticesPartitioner: Partitioner =
        vertices.partitioner.getOrElse(new HashPartitioner(vertices.partitions.length))
    val global_heap_name = bc_global_heap_name.value

    while (i <= maxIterations && maxDiff >= 1e-3) {

      def sendMsgContext(ctx: AggregatingEdgeContext[VD, ED, A]) {
        mapFunc(ctx.toEdgeTriplet,ctx.edge_idx,ctx.array_attr).foreach { kv =>
          val id = kv._1
          val msg = kv._2
          if (id == ctx.srcId) {
            ctx.sendToSrc(msg)
          } else {
            assert(id == ctx.dstId)
            ctx.sendToDst(msg)
          }
        }
      }
      val preAgg = edges.mapPartitionsWithIndex(
        (pid, iter) =>
          aggregateMessagesOffHeap(i,
            sendMsgContext, mergeMsg,
            TripletFields.All, EdgeActiveness.Both,
            edgeArr(pid),
            vertexArr,
            global_heap_name)
      )
      val shuffled = preAgg.partitionBy(verticesPartitioner)
      maxDiff = shuffled.mapPartitionsWithIndex(
        (pid, iter) => Iterator(innerJoinOffHeap(i, pid, vertexArr(pid), iter, mergeMsg, global_heap_name)(vprog)), true
      ).max()

      println("iteration=" + i + ", maxDiff=" + maxDiff)

      i += 1
    }
    vertices.mapPartitionsWithIndex({
      case (pid, _) => output(vertexArr(pid), global_heap_name)
    })
  } // end of apply

  class AttributeTable extends Serializable {
    var shmAddress: ShmAddress = new ShmAddress()
    var size: Int = 0
    var count: Int = 0
    var memsize: Int = 0
  }
  var vertexTblAddr: Broadcast[Array[Array[AttributeTable]]] = null
  var edgeTblAddr: Broadcast[Array[Array[AttributeTable]]] = null
  var bc_global_heap_name: Broadcast[String] = null
  var GLOBAL_HEAP_NAME:String = ""

  def output(vertexTblAddr: Array[AttributeTable], global_heap_name: String): Iterator[(VertexId, (Double, Double))] = {
    GLOBAL_HEAP_NAME = global_heap_name
    // ** OFF-HEAP STORE
    var arrAttr = new UnsafeDoubleArray()
    val addr = getAttributePartition(vertexTblAddr(TBL_VERTEX_ATTR))
    arrAttr.addr = addr
    arrAttr.size = vertexTblAddr(TBL_VERTEX_ATTR).size
    arrAttr.count = vertexTblAddr(TBL_VERTEX_ATTR).count

    val output = new Array[(VertexId,(Double, Double))](arrAttr.size)
    var i = 0
    while(i < arrAttr.size) {
      val key = arrAttr.getKey(i)
      val attr:(Double,Double,Double,Double,Double) =
        arrAttr.getValue[(Double,Double,Double,Double,Double)](i)
      output(i) = (key, (attr._3, attr._4))
      i += 1
    }
    output.iterator
  }


  /** Inner join another VertexPartition. */
  def innerJoinOffHeap[U: ClassTag, VD: ClassTag](iter_no: Int, pid: Int, vertexTblAddr: Array[AttributeTable], 
                                                  iter: Iterator[(VertexId, U)],
                                                  mergeMsg: (U, U) => U,
                                                  global_heap_name: String)
                                                 (f: (VertexId, VD, U) => VD): Double = {
    GLOBAL_HEAP_NAME = global_heap_name
    var maxDiff = 0.0
    // retrieve two attribute tables from Off-heap store
    // ** OFF-HEAP STORE
    var arrAttr = new UnsafeDoubleArray()
    var attrTbl = vertexTblAddr(TBL_VERTEX_ATTR)
    arrAttr.addr = getAttributePartition(attrTbl)
    arrAttr.size = attrTbl.size
    arrAttr.count = attrTbl.count

    val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, U]

    iter.foreach { pair =>
      map.setMerge(pair._1, pair._2, mergeMsg)
      logDebug(pid + ",aggregate:" +  pair._1 + "," + pair._2)
    }

    var j = 0
    var maxidx = 0
    var total = 0
    var convergedcount = 0
    while (j < arrAttr.size ) {
      val value = map.getOrElse(arrAttr.getKey(j), null.asInstanceOf[U]) 
      if(value != null.asInstanceOf[U]) {
          arrAttr.setValues[VD](j,
                   f(arrAttr.getKey(j), arrAttr.getValue[VD](j), value))

          logDebug(pid + ",iter:" + iter_no + 
                   ",addr[" + attrTbl.shmAddress.offset_attrTable + 
                   "],attr:" +  arrAttr.getKey(j) + "," + j + "," + 
                   arrAttr.getValue[U](j) + "," + value)

          val diff = arrAttr.getValue(j, 5)
          if (diff > maxDiff) {
              maxDiff = diff
              maxidx = j
          }
          total += 1
          if( diff < 1e-3 ) convergedcount += 1
      }
      j += 1
    }
    logDebug("maxvertex_" + iter_no + "," + pid + "," + arrAttr.getKey(maxidx) + "," + arrAttr.getValue[U](maxidx) + 
             "," + total + "," + convergedcount + "," + (convergedcount.asInstanceOf[Double]/total.asInstanceOf[Double])) 
    maxDiff
  }

  def build[VD: ClassTag, ED: ClassTag, VD1: ClassTag, ED1: ClassTag](sc: SparkContext,
                                         vertices: RDD[(VertexId, VD)],
                                        edges: RDD[Edge[ED]]): Unit = {


    bc_global_heap_name = sc.broadcast(sc.getConf.get("spark.offheapstore.globalheap.name", "/dev/shm/nvm/global0"))
    val global_heap_name = bc_global_heap_name.value

    val vertex_arr_addr = vertices.mapPartitionsWithIndex((pid, iter) =>
      Iterator(toVertexPartition[VD](pid, iter, global_heap_name)), true).collect()

    vertexTblAddr = sc.broadcast(vertex_arr_addr)

    var verticesPartitioner: Partitioner =
      vertices.partitioner.getOrElse(new HashPartitioner(vertices.partitions.length))

    val arr:Array[Array[AttributeTable]] = vertexTblAddr.value
    val edge_arr_addr = edges.mapPartitionsWithIndex ( (pid, iter) =>
        Iterator(toEdgePartition[VD, ED](pid, iter, arr, verticesPartitioner, global_heap_name))
    ).collect()

    edgeTblAddr = sc.broadcast(edge_arr_addr)
  }

  val TBL_VERTEX_ATTR = 0
  def toVertexPartition[VD: ClassTag](pid: Int,
                                      iter: Iterator[(VertexId, VD)],
                                      global_heap_name: String): (Array[AttributeTable]) = {

    val arrKeys = new PrimitiveVector[Long]
    val arrValue = new PrimitiveVector[VD]

    iter.foreach { pair =>
      arrKeys += pair._1
      arrValue += pair._2
      logDebug("toVertexPartition:" + pid + "," + pair._1 + "," + pair._2)
    }

    val arrKeyValues = new UnsafeDoubleArray()
    arrKeyValues.initKeyValues(arrKeys.size, 5)
    var i = 0
    while(i < arrKeys.size) {
      arrKeyValues.setKeyValue(i, arrKeys(i), arrValue(i))
      i += 1
    }

    // ** OFF-HEAP STORE
    // create off-heap store table
    val attrTbl = new AttributeTable()
    val valuesize: Int = 8*5

    GLOBAL_HEAP_NAME = global_heap_name

    createAttributeHashPartition(arrKeyValues.addr, arrKeyValues.memsize, valuesize, attrTbl)

    attrTbl.count = arrKeyValues.count
    attrTbl.size = arrKeyValues.size
    arrKeyValues.free()

    logDebug("createAttributeHashPartition:" + attrTbl.shmAddress.regionId + "," + 
             attrTbl.shmAddress.offset_attrTable + "," + 
             attrTbl.shmAddress.offset_hash1stTable + "," + 
             attrTbl.shmAddress.offset_hash2ndTable)
    val arrAddr = new Array[AttributeTable](1)
    arrAddr(0) = attrTbl

    arrAddr
  }

  def toEdgePartition[VD: ClassTag, ED: ClassTag](pid: Int,
                                                  edges: Iterator[Edge[ED]],
                                                  vertexTblAddr: Array[Array[AttributeTable]],
                                                  verticesPartitioner: Partitioner,
                                                  global_heap_name: String): Array[AttributeTable] = {


    GLOBAL_HEAP_NAME = global_heap_name
    val edgeArray = edges.toArray
    new Sorter(Edge.edgeArraySortDataFormat[ED])
      .sort(edgeArray, 0, edgeArray.length, Edge.lexicographicOrdering)

    val localSrcIds = new UnsafeIntArray()
    localSrcIds.init(edgeArray.size)
    val localDstIds = new UnsafeIntArray()
    localDstIds.init(edgeArray.size)
    val data = new UnsafeDoubleArray()
    data.init(edgeArray.size, 8)
    val index = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]

    val global2local = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    val local2global = new PrimitiveVector[VertexId]
    val partTable = new GraphXPrimitiveKeyOpenHashMap[Int, OpenHashSet[VertexId]]

    // Copy edges into columnar structures, tracking the beginnings of source vertex id clusters and
    // adding them to the index. Also populate a map from vertex id to a sequential local offset.
    if (edgeArray.length > 0) {
      index.update(edgeArray(0).srcId, 0)

      var currSrcId: VertexId = edgeArray(0).srcId
      var currLocalId = -1
      var i = 0

      data.init(edgeArray.length, 8)
      while (i < edgeArray.size) {
        val srcId = edgeArray(i).srcId
        val dstId = edgeArray(i).dstId
        localSrcIds.set(i, global2local.changeValue(srcId,
        { currLocalId += 1; local2global += srcId; currLocalId }, identity))
        localDstIds.set(i, global2local.changeValue(dstId,
        { currLocalId += 1; local2global += dstId; currLocalId }, identity))

        val partSrcId: Int = verticesPartitioner.getPartition(srcId)
        var vecSrcIds = partTable.getOrElse(partSrcId, null)
        if(vecSrcIds == null)
          vecSrcIds = new OpenHashSet[VertexId]()
        vecSrcIds.add(srcId)
        partTable.update(partSrcId, vecSrcIds)

        val partDstId: Int = verticesPartitioner.getPartition(dstId)
        var vecDstIds = partTable.getOrElse(partDstId, null)
        if(vecDstIds == null)
          vecDstIds = new OpenHashSet[VertexId]()
        vecDstIds.add(dstId)
        partTable.update(partDstId, vecDstIds)

        data.set(i, edgeArray(i).attr)

        if (srcId != currSrcId) {
          currSrcId = srcId
          index.update(currSrcId, i)
        }

        i += 1
      }
    }

    val unsafe_cluster_index = new UnsafeIntArray
    unsafe_cluster_index.init(index.size)
    var index_i = 0
    index.iterator.foreach { cluster =>
      val clusterPos = cluster._2
      unsafe_cluster_index.set(index_i, clusterPos)
      index_i += 1
    }
    val unsafe_local2global = new UnsafeLongArray
    unsafe_local2global.init(local2global.trim().array.size)
    unsafe_local2global.set(local2global.trim().array)

    val unsafe_partIds = new UnsafeIntArray()
    unsafe_partIds.init(partTable.keySet.size)

    var i = partTable.keySet.getBitSet.nextSetBit(0)
    val vec_unsafe_localIds = new PrimitiveVector[UnsafeIntArray]
    val vec_unsafe_addr_vertexattr = new PrimitiveVector[UnsafeIntArray]
    val unsafe_partLocalIds = new UnsafeArrays()
    val unsafe_partAddr = new UnsafeArrays()

    var totalmemsz = 0
    var ipos = 0
    while (i >= 0) {
      val partId = partTable.keySet.getValue(i)
      val vertexIds = partTable.getOrElse(partId, null)

      val unsafe_localIds = new UnsafeIntArray()
      unsafe_localIds.init(vertexIds.iterator.size)
      val unsafe_addr_vertexattr = new UnsafeIntArray()
      unsafe_addr_vertexattr.init(vertexIds.iterator.size)

      var j = vertexIds.getBitSet.nextSetBit(0)
      var jpos = 0
      while(j >= 0) {
        val vid = vertexIds.getValue(j)
        val offset =
          getAttributeOffsetInHashPartition(vertexTblAddr(partId)(TBL_VERTEX_ATTR), vid)
        logDebug(partId + ",offset:" + vid + "," +  
                 vertexTblAddr(partId)(TBL_VERTEX_ATTR).shmAddress.offset_attrTable + "," + offset)
        unsafe_localIds.set(jpos, global2local(vid))
        unsafe_addr_vertexattr.set(jpos, offset.asInstanceOf[Int])
        j = vertexIds.getBitSet.nextSetBit(j + 1)
        jpos += 1
      }
      unsafe_partIds.set(ipos, partId)
      vec_unsafe_localIds += unsafe_localIds
      vec_unsafe_addr_vertexattr += unsafe_addr_vertexattr

      i = partTable.keySet.getBitSet.nextSetBit(i + 1)
      ipos += 1
      totalmemsz += unsafe_localIds.memsize + unsafe_partLocalIds.HEADER_SIZE
    }
    unsafe_partLocalIds.alloc(totalmemsz)
    unsafe_partAddr.alloc(totalmemsz)

    i = 0
    var pos = 0
    while(i<unsafe_partIds.size) {
      unsafe_partLocalIds.set(pos, vec_unsafe_localIds(i))
      unsafe_partAddr.set(pos, vec_unsafe_addr_vertexattr(i))
      pos = vec_unsafe_localIds(i).nextPos(pos)
      vec_unsafe_localIds(i).free()
      vec_unsafe_addr_vertexattr(i).free()
      i += 1
    }

    // **OFF-HEAP STORE creating attribute tables for edge RDD
    val arrAddr = new Array[AttributeTable](8)

    var attrTbl = new AttributeTable()
    createAttributePartition(unsafe_local2global.addr, unsafe_local2global.memsize, attrTbl)
    attrTbl.count = unsafe_local2global.count
    attrTbl.size = unsafe_local2global.size
    unsafe_local2global.free()
    arrAddr(TBL_EDGE_LOCAL2GLOBAL) = attrTbl

    attrTbl = new AttributeTable()
    createAttributePartition(unsafe_cluster_index.addr, unsafe_cluster_index.memsize, attrTbl)
    attrTbl.count = unsafe_cluster_index.count
    attrTbl.size = unsafe_cluster_index.size
    unsafe_cluster_index.free()
    arrAddr(TBL_EDGE_CLUSTER_INDEX) = attrTbl

    attrTbl = new AttributeTable()
    createAttributePartition(data.addr, data.memsize, attrTbl)
    attrTbl.count = data.count
    attrTbl.size = data.size
    data.free()
    arrAddr(TBL_EDGE_ATTR) = attrTbl

    attrTbl = new AttributeTable()
    createAttributePartition(unsafe_partIds.addr, unsafe_partIds.memsize, attrTbl)
    attrTbl.memsize = unsafe_partIds.memsize
    attrTbl.size = unsafe_partIds.size
    unsafe_partIds.free()
    arrAddr(TBL_EDGE_PARTID) = attrTbl

    attrTbl = new AttributeTable()
    createAttributePartition(unsafe_partLocalIds.addr, unsafe_partLocalIds.memsize, attrTbl)
    attrTbl.memsize = unsafe_partLocalIds.memsize
    unsafe_partLocalIds.free()
    arrAddr(TBL_EDGE_PARTLOCALIDS) = attrTbl

    attrTbl = new AttributeTable()
    createAttributePartition(unsafe_partAddr.addr, unsafe_partAddr.memsize, attrTbl)
    attrTbl.memsize = unsafe_partAddr.memsize
    unsafe_partAddr.free()
    arrAddr(TBL_EDGE_PARTADDR) = attrTbl

    attrTbl = new AttributeTable()
    createAttributePartition(localSrcIds.addr, localSrcIds.memsize, attrTbl)
    attrTbl.size = localSrcIds.size
    localSrcIds.free()
    arrAddr(TBL_EDGE_LOCALSRCIDS) = attrTbl

    attrTbl = new AttributeTable()
    createAttributePartition(localDstIds.addr, localDstIds.memsize, attrTbl)
    attrTbl.size = localDstIds.size
    localDstIds.free()
    arrAddr(TBL_EDGE_LOCALDSTIDS) = attrTbl

    // return shared memory address
    arrAddr
  }

  val TBL_EDGE_LOCAL2GLOBAL = 0
  val TBL_EDGE_CLUSTER_INDEX = 1
  val TBL_EDGE_ATTR = 2
  val TBL_EDGE_PARTID = 3
  val TBL_EDGE_PARTLOCALIDS = 4
  val TBL_EDGE_PARTADDR = 5
  val TBL_EDGE_LOCALSRCIDS = 6
  val TBL_EDGE_LOCALDSTIDS = 7


  def aggregateMessagesOffHeap[VD: ClassTag, ED: ClassTag, A: ClassTag]( iter: Int,
                                       sendMsg: AggregatingEdgeContext[VD, ED, A] => Unit,
                                       mergeMsg: (A, A) => A,
                                       tripletFields: TripletFields,
                                       activeness: EdgeActiveness,
                                       edgeTblAddr: Array[AttributeTable], 
                                       vertexTblAddr: Array[Array[AttributeTable]],
                                       global_heap_name: String):
                                       Iterator[(VertexId, A)] = {


    GLOBAL_HEAP_NAME = global_heap_name

    // ** OFF-HEAP STORE
    val unsafe_cluster_index = new UnsafeIntArray
    unsafe_cluster_index.addr =
      getAttributePartition(edgeTblAddr(TBL_EDGE_CLUSTER_INDEX))
    unsafe_cluster_index.size = edgeTblAddr(TBL_EDGE_CLUSTER_INDEX).size

    val localSrcIds = new UnsafeIntArray()
    localSrcIds.addr = getAttributePartition(edgeTblAddr(TBL_EDGE_LOCALSRCIDS))
    localSrcIds.size = edgeTblAddr(TBL_EDGE_LOCALSRCIDS).size

    val localDstIds = new UnsafeIntArray()
    localDstIds.addr = getAttributePartition(edgeTblAddr(TBL_EDGE_LOCALDSTIDS))
    localDstIds.size = edgeTblAddr(TBL_EDGE_LOCALSRCIDS).size

    val unsafe_local2global = new UnsafeLongArray
    unsafe_local2global.addr = getAttributePartition(edgeTblAddr(TBL_EDGE_LOCAL2GLOBAL))
    unsafe_local2global.size = edgeTblAddr(TBL_EDGE_LOCAL2GLOBAL).size

    val data = new UnsafeDoubleArray()
    data.addr = getAttributePartition(edgeTblAddr(TBL_EDGE_ATTR))
    data.size = edgeTblAddr(TBL_EDGE_ATTR).size
    data.count = edgeTblAddr(TBL_EDGE_ATTR).count

    val vertexAttrs = new UnsafeDoubleArray() //Array[VD](currLocalId + 1)
    vertexAttrs.init(unsafe_local2global.size, 5)


    // OUTPUT
    var bitset = new BitSet(vertexAttrs.size)
    val aggregates = new UnsafeDoubleArray()
    aggregates.init(vertexAttrs.size, 2) 

    val part_arr = new UnsafeIntArray()
    part_arr.addr = getAttributePartition(edgeTblAddr(TBL_EDGE_PARTID))
    part_arr.size = edgeTblAddr(TBL_EDGE_PARTID).size

    val part_vid_arr = new UnsafeArrays()
    val part_voffset_arr = new UnsafeArrays()
    part_vid_arr.addr = getAttributePartition(edgeTblAddr(TBL_EDGE_PARTLOCALIDS))
    part_vid_arr.memsize = edgeTblAddr(TBL_EDGE_PARTLOCALIDS).memsize
    part_voffset_arr.addr = getAttributePartition(edgeTblAddr(TBL_EDGE_PARTADDR))
    part_voffset_arr.memsize = edgeTblAddr(TBL_EDGE_PARTADDR).memsize


    var i = 0
    var pos = 0
    while(i < part_arr.size) {
      val vpid = part_arr.get(i) 
      val vattr_arr = new UnsafeDoubleArray()
      vattr_arr.addr = getAttributePartition(vertexTblAddr(vpid)(TBL_VERTEX_ATTR))
      vattr_arr.size = vertexTblAddr(vpid)(TBL_VERTEX_ATTR).size
      vattr_arr.count = vertexTblAddr(vpid)(TBL_VERTEX_ATTR).count

      val vid_arr = new UnsafeIntArray()
      val voffset_arr = new UnsafeIntArray()
      vid_arr.setHeader(part_vid_arr.getSize(pos), part_vid_arr.getCount(pos))
      vid_arr.addr = part_vid_arr.getAddr(pos)
      voffset_arr.setHeader(part_voffset_arr.getSize(pos), part_voffset_arr.getCount(pos))
      voffset_arr.addr = part_voffset_arr.getAddr(pos)

      var k = 0
      while (k < vid_arr.size) {
        // update vertexAttrs
        vertexAttrs.set[VD](vid_arr.get(k), vattr_arr.getValue[VD](voffset_arr.get(k)))
        bitset.set(vid_arr.get(k))
        logDebug(vpid + ",iter:" + iter + ",addr[" + vertexTblAddr(vpid)(TBL_VERTEX_ATTR).shmAddress.offset_attrTable + 
                 "]: vertexAttrs[" + unsafe_local2global.get(vid_arr.get(k)) + "]=" + 
                 vattr_arr.getValue[VD](voffset_arr.get(k)) + "," + voffset_arr.get(k))
        k += 1
      }
      i += 1
      pos = vid_arr.nextPos(pos)
    }
    EdgeMessageComputeAggregate.INSTANCE.aggregateComputeMessages(
      unsafe_cluster_index.size, vertexAttrs.size, localSrcIds.size, unsafe_cluster_index.addr,
      vertexAttrs.addr, localSrcIds.addr, localDstIds.addr,
      data.addr, aggregates.addr);

    vertexAttrs.free()

    // aggregated messages
    val iterBitset: Iterator[(VertexId, A)] =
          bitset.iterator.map { localId => (unsafe_local2global.get(localId), aggregates.get[A](localId)) }

    new Iterator[(VertexId, A)] {

        override def hasNext: Boolean =
        {
          if (iterBitset.hasNext) {
            true
          } else {
            aggregates.free()
            false
          }
        }
        override def next() =
        {
          iterBitset.next()
        }
     }
  }

  // For JNI
  def createAttributePartition (addr: Long, size: Int, attrTbl: AttributeTable) = {

    if(!OffHeapStore.INSTANCE.isInitialized())
      OffHeapStore.INSTANCE.initialize(GLOBAL_HEAP_NAME, 0)

    OffHeapStore.INSTANCE.createAttributePartition(addr, size, attrTbl.shmAddress)

    //attrTbl.shmAddress.offset_attrTable = addr
  }
  def createAttributeHashPartition (addr: Long, size: Int, valuesize: Int, attrTbl: AttributeTable) = {

    if(!OffHeapStore.INSTANCE.isInitialized())
      OffHeapStore.INSTANCE.initialize(GLOBAL_HEAP_NAME, 0)

    OffHeapStore.INSTANCE.createAttributeHashPartition(addr, size, valuesize, attrTbl.shmAddress)

    //attrTbl.shmAddress.offset_attrTable = addr
  }
  def getAttributePartition (attrTbl: AttributeTable): Long = {

    if(!OffHeapStore.INSTANCE.isInitialized())
      OffHeapStore.INSTANCE.initialize(GLOBAL_HEAP_NAME, 0)

    OffHeapStore.INSTANCE.getAttributePartition(attrTbl.shmAddress)

    //attrTbl.shmAddress.offset_attrTable
  }
  def getAttributeOffsetInHashPartition (attrTbl: AttributeTable, key: Long): Long = {
/*
    val arr = new UnsafeDoubleArray()
	arr.addr = getAttributePartition(attrTbl)
    //arr.addr = attrTbl.shmAddress.offset_attrTable
    arr.size = attrTbl.size
    arr.count = 5
    var value = 0
    var i = 0
    while (i < arr.size) {
      val k = arr.getKey(i)
      if(k == key) {
        value = i
      }
      i += 1
    }
    value
*/

    if(!OffHeapStore.INSTANCE.isInitialized())
      OffHeapStore.INSTANCE.initialize(GLOBAL_HEAP_NAME, 0)

    OffHeapStore.INSTANCE.getAttributeOffsetInHashPartition(attrTbl.shmAddress, key)
  }
}


private class AggregatingEdgeContext[VD, ED, A](
                                                 mergeMsg: (A, A) => A,
                                                 aggregates: Array[A],
                                                 bitset: BitSet)
  extends EdgeContext[VD, ED, A] {

  private[this] var _srcId: VertexId = _
  private[this] var _dstId: VertexId = _
  private[this] var _localSrcId: Int = _
  private[this] var _localDstId: Int = _
  private[this] var _srcAttr: VD = _
  private[this] var _dstAttr: VD = _
  private[this] var _attr: ED = _

  /* Added by Mijung for in-place edge attributes */
  private[this] var _array_attr: UnsafeDoubleArray = _
  private[this] var _edge_idx: Int = _
  private[this] var _starting_addr: Long = _


  /* Added by Mijung for in-place edge attributes */
  def set(
           srcId: VertexId, dstId: VertexId,
           localSrcId: Int, localDstId: Int,
           srcAttr: VD, dstAttr: VD,
           attr: ED, array_attr: UnsafeDoubleArray, edge_idx:Int) {
    _srcId = srcId
    _dstId = dstId
    _localSrcId = localSrcId
    _localDstId = localDstId
    _srcAttr = srcAttr
    _dstAttr = dstAttr
    _attr = attr
    _array_attr = array_attr
    _edge_idx = edge_idx
  }

  def set(
           srcId: VertexId, dstId: VertexId,
           localSrcId: Int, localDstId: Int,
           srcAttr: VD, dstAttr: VD,
           edge_idx:Int,
           starting_addr: Long) {
    _srcId = srcId
    _dstId = dstId
    _localSrcId = localSrcId
    _localDstId = localDstId
    _srcAttr = srcAttr
    _dstAttr = dstAttr
    _edge_idx = edge_idx
    _starting_addr = starting_addr

  }

  /* Added by Mijung for in-place edge attributes */
  def setRest(dstId: VertexId, localDstId: Int, dstAttr: VD, attr: ED,
              array_attr: UnsafeDoubleArray, edge_idx:Int) {
    _dstId = dstId
    _localDstId = localDstId
    _dstAttr = dstAttr
    _attr = attr
    _array_attr = array_attr
    _edge_idx = edge_idx
  }

  /* Added by Mijung for in-place unsafe edge attributes */
  def setRest(dstId: VertexId, localDstId: Int, dstAttr: VD, edge_idx: Int, starting_addr: Long) {
    _dstId = dstId
    _localDstId = localDstId
    _dstAttr = dstAttr
    _edge_idx = edge_idx
    _starting_addr = starting_addr
  }

  def set(
           srcId: VertexId, dstId: VertexId,
           localSrcId: Int, localDstId: Int,
           srcAttr: VD, dstAttr: VD,
           attr: ED) {
    _srcId = srcId
    _dstId = dstId
    _localSrcId = localSrcId
    _localDstId = localDstId
    _srcAttr = srcAttr
    _dstAttr = dstAttr
    _attr = attr
  }

  def setSrcOnly(srcId: VertexId, localSrcId: Int, srcAttr: VD) {
    _srcId = srcId
    _localSrcId = localSrcId
    _srcAttr = srcAttr
  }

  def setRest(dstId: VertexId, localDstId: Int, dstAttr: VD, attr: ED) {
    _dstId = dstId
    _localDstId = localDstId
    _dstAttr = dstAttr
    _attr = attr
  }

  override def srcId: VertexId = _srcId
  override def dstId: VertexId = _dstId
  override def srcAttr: VD = _srcAttr
  override def dstAttr: VD = _dstAttr
  override def attr: ED = _attr

  /* Added by Mijung for in-place edge attributes */
  def array_attr = _array_attr
  def edge_idx = _edge_idx

  override def sendToSrc(msg: A) {
    send(_localSrcId, msg)
  }
  override def sendToDst(msg: A) {
    send(_localDstId, msg)
  }

  @inline private def send(localId: Int, msg: A) {
    if (bitset.get(localId)) {
      aggregates(localId) = mergeMsg(aggregates(localId), msg)
    } else {
      aggregates(localId) = msg
      bitset.set(localId)
    }
  }
}
