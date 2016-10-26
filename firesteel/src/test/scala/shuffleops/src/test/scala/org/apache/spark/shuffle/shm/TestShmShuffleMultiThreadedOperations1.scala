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
 *
 * Portions of this file are (c) Copyright 2016 Hewlett Packard Enterprise Development LP
 *
 */

package org.apache.spark.shuffle.shm

import java.nio.ByteBuffer

import org.apache.spark.SparkContext._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.MutablePair
import org.scalatest.{Matchers, FunSuite}
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.log4j.PropertyConfigurator

import org.apache.spark.rdd.{CoGroupedRDD, OrderedRDDFunctions, RDD, ShuffledRDD, SubtractedRDD}

import scala.collection.mutable.HashSet
import com.hp.hpl.firesteel.shuffle.ShuffleStoreManager

class  TestShmShuffleMultiThreadedOperations1 extends FunSuite with Matchers with LocalSparkContext with Logging {

  //to configure log4j properties to follow what are specified in log4j.properties
   override def beforeAll() {
     //when invoke from maven, what gets loaded is the one at: src/test/resources
     //PropertyConfigurator.configure("log4j.properties")
   }

   test("groupBy with two partitions") {

    logInfo(" test:groupBy with two partitions at process: " + 
           java.lang.management.ManagementFactory.getRuntimeMXBean().getName());

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local[2]", "test", conf)
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1)), 2)
    val groups = pairs.groupByKey(2).collect()
    //the group means the key group: 1 and 2, in this test case.
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))

    sc.stop()

    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()

  }

  test ("groupBy with two partitions with four threads") {

    logInfo(" test:groupBy with two partitions with four threads at process: " + 
           java.lang.management.ManagementFactory.getRuntimeMXBean().getName());

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local[4]", "test", conf)
    val pairs = sc.parallelize(Array((2, 1),(1, 1), (1, 3), (1, 2)), 6)
    val groups = pairs.groupByKey(6).collect()
    assert(groups.size == 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))


    sc.stop()

    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("shuffle non-zero block size") {

    logInfo(" test:shuffle non-zero block size at process: " + 
           java.lang.management.ManagementFactory.getRuntimeMXBean().getName());

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local[4]", "test", conf)

    val NUM_BLOCKS = 3

    val a = sc.parallelize(1 to 10, 2)
    val b = a.map { x =>
      (x, new NonJavaSerializableClass(x * 2))
    }
    // If the Kryo serializer is not used correctly, the shuffle would fail because the
    // default Java serializer cannot handle the non serializable class.
    val c = new ShuffledRDD[Int,
      NonJavaSerializableClass,
      NonJavaSerializableClass](b, new HashPartitioner(NUM_BLOCKS))
    c.setSerializer(new KryoSerializer(conf))
    val shuffleId = c.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId

    assert(c.count === 10)

    // All blocks must have non-zero size
    val mapOutputTracker = SparkEnv.get.mapOutputTracker
    (0 until NUM_BLOCKS).foreach { id =>
      val statuses = 
          SharedMemoryMapOutputTracker.getMapSizesByExecutorId(shuffleId, id, mapOutputTracker)
      assert(statuses.forall(s => s._2 > 0))
    }

    //(0 until NUM_BLOCKS).foreach { id =>
    //  val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, id)
    //  assert(statuses.forall(s => s._2 > 0))
    //}

    sc.stop()

    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("shuffle serializer") {

    logInfo(" test:shuffle serializer at process: " + 
           java.lang.management.ManagementFactory.getRuntimeMXBean().getName());

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local[4]", "test", conf)

    val a = sc.parallelize(1 to 10, 2)
    val b = a.map { x =>
      (x, new NonJavaSerializableClass(x * 2))
    }
    // If the Kryo serializer is not used correctly, the shuffle would fail because the
    // default Java serializer cannot handle the non serializable class.
    val c = new ShuffledRDD[Int,
      NonJavaSerializableClass,
      NonJavaSerializableClass](b, new HashPartitioner(3))
    c.setSerializer(new KryoSerializer(conf))
    assert(c.count === 10)

    sc.stop()

    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("zero sized blocks") {

    logInfo(" test:zero sized blocks at process: " + 
           java.lang.management.ManagementFactory.getRuntimeMXBean().getName());

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    //stress testing: using 10 threads.
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local[10]", "test", conf)

    // 201 partitions (greater than "spark.shuffle.sort.bypassMergeThreshold") from 4 keys
    val NUM_BLOCKS = 201
    val a = sc.parallelize(1 to 4, NUM_BLOCKS)
    val b = a.map(x => (x, x*2))

    // NOTE: The default Java serializer doesn't create zero-sized blocks.
    //       So, use Kryo
    val c = new ShuffledRDD[Int, Int, Int](b, new HashPartitioner(NUM_BLOCKS))
      .setSerializer(new KryoSerializer(conf))

    val shuffleId = c.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
    assert(c.count === 4)


    val mapOutputTracker = SparkEnv.get.mapOutputTracker
    val blockSizes = (0 until NUM_BLOCKS).flatMap { id =>
      val statuses = 
          SharedMemoryMapOutputTracker.getMapSizesByExecutorId(shuffleId, id, mapOutputTracker)
      //based on the definition in SharedMemoryMapOutputTracker, which is different from MapOutputTracker.
      statuses.map(x => x._4)
    }
    val nonEmptyBlocks = blockSizes.filter(x => x > 0)

    // We should have at most 4 non-zero sized partitions
    assert(nonEmptyBlocks.size <= 4)

    sc.stop()

    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("zero sized blocks without kryo") {

    logInfo(" test:zero sized blocks without kryo at process: " + 
           java.lang.management.ManagementFactory.getRuntimeMXBean().getName());

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local[10]", "test", conf)

    // 201 partitions (greater than "spark.shuffle.sort.bypassMergeThreshold") from 4 keys
    val NUM_BLOCKS = 201
    val a = sc.parallelize(1 to 4, NUM_BLOCKS)
    val b = a.map(x => (x, x*2))

    // NOTE: The default Java serializer should create zero-sized blocks
    val c = new ShuffledRDD[Int, Int, Int](b, new HashPartitioner(NUM_BLOCKS))

    val shuffleId = c.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
    assert(c.count === 4)

    // All blocks must have non-zero size
    val mapOutputTracker = SparkEnv.get.mapOutputTracker
    val blockSizes = (0 until NUM_BLOCKS).flatMap { id =>
      val statuses = 
          SharedMemoryMapOutputTracker.getMapSizesByExecutorId(shuffleId, id, mapOutputTracker)
      //based on the definition in SharedMemoryMapOutputTracker, which is different from MapOutputTracker.
      statuses.map(x => x._4)
    }
    val nonEmptyBlocks = blockSizes.filter(x => x > 0)

    // We should have at most 4 non-zero sized partitions
    assert(nonEmptyBlocks.size <= 4)

    sc.stop()

    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("shuffle on mutable pairs without registration of mutable pair") {

    logInfo(" test:shuffle on mutable pairs without registration of mutable pair at process: " + 
           java.lang.management.ManagementFactory.getRuntimeMXBean().getName());

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local[4]", "test", conf)

    def p[T1, T2](_1: T1, _2: T2) = MutablePair(_1, _2)
    val data = Array(p(1, 1), p(1, 2), p(1, 3), p(2, 1))
    val pairs: RDD[MutablePair[Int, Int]] = sc.parallelize(data, 2)
    val results = new ShuffledRDD[Int, Int, Int](pairs,
      new HashPartitioner(2)).collect()

    data.foreach { pair => results should contain ((pair._1, pair._2)) }

    sc.stop()

    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  //NOTE: compare to class registration, there is no bucket size change at the map side
  //due to registration of the mutable pair.
  test ("shuffle on mutable pairs with registration of mutable pair") {

    logInfo(" test:shuffle on mutable pairs with registration of mutable pair at process: " + 
           java.lang.management.ManagementFactory.getRuntimeMXBean().getName());

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.registerKryoClasses(Array(classOf[MutablePair[Int, Int]]))
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local[4]", "test", conf)

    def p[T1, T2](_1: T1, _2: T2) = MutablePair(_1, _2)
    val data = Array(p(1, 1), p(1, 2), p(1, 3), p(2, 1))
    val pairs: RDD[MutablePair[Int, Int]] = sc.parallelize(data, 2)
    val results = new ShuffledRDD[Int, Int, Int](pairs,
      new HashPartitioner(2)).collect()

    data.foreach { pair => results should contain ((pair._1, pair._2)) }

    sc.stop()

    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }
}

