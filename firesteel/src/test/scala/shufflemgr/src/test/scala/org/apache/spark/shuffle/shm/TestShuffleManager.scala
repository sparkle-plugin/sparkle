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
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle._
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.scalatest.FunSuite
import org.apache.spark.{SparkEnv, SparkContext, LocalSparkContext, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer._
import com.hp.hpl.firesteel.shuffle._
import com.hp.hpl.firesteel.shuffle.ThreadLocalShuffleResourceHolder._
import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer._
import org.scalatest.FunSuite

import scala.annotation.meta.param

import scala.collection.mutable.ArrayBuffer

class TestShuffleManager extends FunSuite with LocalSparkContext with Logging {

 class MyRDD(
    sc: SparkContext,
    numPartitions: Int,
    dependencies: List[Dependency[_]],
    locations: Seq[Seq[String]] = Nil,
    @(transient @param) tracker: MapOutputTrackerMaster = null)
  extends RDD[(Int, Int)](sc, dependencies) with Serializable {

  override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] =
    throw new RuntimeException("should not be reached")

  override def getPartitions: Array[Partition] = (0 until numPartitions).map(i => new Partition {
    override def index: Int = i
  }).toArray

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    if (locations.isDefinedAt(partition.index)) {
      locations(partition.index)
    } else if (tracker != null && dependencies.size == 1 &&
        dependencies(0).isInstanceOf[ShuffleDependency[_, _, _]]) {
      // If we have only one shuffle dependency, use the same code path as ShuffledRDD for locality
      val dep = dependencies(0).asInstanceOf[ShuffleDependency[_, _, _]]
      tracker.getPreferredLocationsForShuffle(dep, partition.index)
    } else {
      Nil
    }
  }

  override def toString: String = "DAGSchedulerSuiteRDD " + id
  }

  private def getThreadLocalShuffleResource(conf: SparkConf):
          ThreadLocalShuffleResourceHolder.ShuffleResource = {
      val SERIALIZATION_BUFFER_SIZE: Int =
      conf.getInt("spark.shuffle.shm.serializer.buffer.max.mb", 64) * 1024 * 1024;

       val resourceHolder= new ThreadLocalShuffleResourceHolder()
       var shuffleResource = resourceHolder.getResource()

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

          //push to the thread specific storage for future retrieval in the same task execution.
          resourceHolder.initialize (shuffleResource)

          logDebug ("Thread: " + Thread.currentThread().getId
            + " create kryo-bytebuffer resource for mapper writer")
       }

       shuffleResource

  }

  //use variable sc instead.
  test("loading shuffle store manager") {

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    //NOTE: after reverting back SparkEnv's shuffle manager loading, we need to specify the full
    //path to the ShmShuffleManager, rather just a name and use the name to look up the hash map
    //for the full path to the ShmShuffleManager
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    //this needs to be set for the ShmShuffleManager to be launched.
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val shuffleManager = SparkEnv.get.shuffleManager
    assert(shuffleManager.isInstanceOf[ShmShuffleManager])

    //due to lazy construction of the shmshuffle manager. we need to invoke the reader in order to 
    //have the internal construction of the Java side shuffle store manager to be created.
    val shuffleMapRdd = new MyRDD(sc, 1, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(1))
    val shuffleHandle = shuffleManager.registerShuffle(0, 1, shuffleDep)
    //until we get to the reader, which triggers the native shuffle store manager's construction.
    val reader = shuffleManager.getReader[Int, Int](shuffleHandle, 0, 1, null)
                        
    val nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
    logInfo("native pointer of shuffle store manager retrieved is:"
      + "0x"+ java.lang.Long.toHexString(nativePointer));
    ShuffleStoreManager.INSTANCE.shutdown();

    sc.stop()
  }

}
