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
import org.apache.spark.serializer._
import com.hp.hpl.firesteel.shuffle._
import com.esotericsoftware.kryo.Kryo

import org.apache.spark.rdd.{CoGroupedRDD, OrderedRDDFunctions, RDD, ShuffledRDD, SubtractedRDD}

import scala.collection.mutable.HashSet
import org.apache.log4j.PropertyConfigurator

import java.lang.management.ManagementFactory

class  TestShmShuffleOperations extends FunSuite with Matchers with LocalSparkContext with Logging {

  //to configure log4j properties to follow what are specified in log4j.properties
  override def beforeAll() {
     //when invoke from maven, what gets loaded is the one at: src/test/resources
     //PropertyConfigurator.configure("log4j.properties")
  }

  test ("groupBy with one single partition") {
    logInfo(" test:groupBy with one single partition at process: " + 
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
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES );
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1)), 1)
    val groups = pairs.groupByKey(1).collect()
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
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)
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

    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local", "test", conf)

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

    val mapOutputTracker = SparkEnv.get.mapOutputTracker
    // All blocks must have non-zero size
    (0 until NUM_BLOCKS).foreach { id =>
      val statuses = 
          SharedMemoryMapOutputTracker.getMapSizesByExecutorId(shuffleId, id, mapOutputTracker)
      assert(statuses.forall(s => s._2 > 0))
    }

    sc.stop()

    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("shuffle serializer") {
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

    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local", "test", conf)

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

    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local", "test", conf)

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

    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local", "test", conf)

    // 201 partitions (greater than "spark.shuffle.sort.bypassMergeThreshold") from 4 keys
    val NUM_BLOCKS = 201
    val a = sc.parallelize(1 to 4, NUM_BLOCKS)
    val b = a.map(x => (x, x*2))

    // NOTE: The default Java serializer should create zero-sized blocks
    val c = new ShuffledRDD[Int, Int, Int](b, new HashPartitioner(NUM_BLOCKS))

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

  test ("shuffle on mutable pairs without registration of mutable pair") {
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

    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local", "test", conf)

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
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local", "test", conf)

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

  //NOTE: my current C++ shuffle engine should have an ascending order in terms of key ordering.
  test ("sorting on mutable pairs") {
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

    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local", "test", conf)

    def p[T1, T2](_1: T1, _2: T2) = MutablePair(_1, _2)
    val data = Array(p(1, 11), p(3, 33), p(100, 100), p(2, 22))
    val pairs: RDD[MutablePair[Int, Int]] = sc.parallelize(data, 4)
    val results = new OrderedRDDFunctions[Int, Int, MutablePair[Int, Int]](pairs)
      .sortByKey().collect()
    results(0) should be ((1, 11))
    results(1) should be ((2, 22))
    results(2) should be ((3, 33))
    results(3) should be ((100, 100))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("cogroup using mutable pairs") {
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

    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local", "test", conf)

    def p[T1, T2](_1: T1, _2: T2) = MutablePair(_1, _2)
    val data1 = Seq(p(1, 1), p(1, 2), p(1, 3), p(2, 1))
    val data2 = Seq(p(1, "11"), p(1, "12"), p(2, "22"), p(3, "3"))
    val pairs1: RDD[MutablePair[Int, Int]] = sc.parallelize(data1, 4)
    val pairs2: RDD[MutablePair[Int, String]] = sc.parallelize(data2, 4)
    val results = new CoGroupedRDD[Int](Seq(pairs1, pairs2), new HashPartitioner(4))
      .map(p => (p._1, p._2.map(_.toArray)))
      .collectAsMap()

    assert(results(1)(0).length === 3)
    assert(results(1)(0).contains(1))
    assert(results(1)(0).contains(2))
    assert(results(1)(0).contains(3))
    assert(results(1)(1).length === 2)
    assert(results(1)(1).contains("11"))
    assert(results(1)(1).contains("12"))
    assert(results(2)(0).length === 1)
    assert(results(2)(0).contains(1))
    assert(results(2)(1).length === 1)
    assert(results(2)(1).contains("22"))
    assert(results(3)(0).length === 0)
    assert(results(3)(1).contains("3"))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("subtract mutable pairs") {
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

    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local", "test", conf)

    def p[T1, T2](_1: T1, _2: T2) = MutablePair(_1, _2)
    val data1 = Seq(p(1, 1), p(1, 2), p(1, 3), p(2, 1), p(3, 33))
    val data2 = Seq(p(1, "11"), p(1, "12"), p(2, "22"))
    val pairs1: RDD[MutablePair[Int, Int]] = sc.parallelize(data1, 6)
    val pairs2: RDD[MutablePair[Int, String]] = sc.parallelize(data2, 6)
    val results = new SubtractedRDD(pairs1, pairs2, new HashPartitioner(6)).collect()
    results should have length (1)
    // substracted rdd return results as Tuple2
    results(0) should be ((3, 33))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  //NOTE: the orginal size of the array is 10000, I reduced it to 100, for debugging
  //purpose.
  test ("shuffle with different compression settings (SPARK-3426)") {
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

    for (
      shuffleSpillCompress <- Set(true, false);
      shuffleCompress <- Set(true, false)
    ) {
       conf
        .setAppName("test")
        .setMaster("local")
        .set("spark.shuffle.spill.compress", shuffleSpillCompress.toString)
        .set("spark.shuffle.compress", shuffleCompress.toString)
        .set("spark.shuffle.memoryFraction", "0.001")
      resetSparkContext()
      //sc = new SparkContext(conf)
      sc = new SparkContext("local", "test", conf)

      try {
        sc.parallelize(0 until 100, 10).map(i => (i / 4, i)).groupByKey(10).collect()
      } catch {
        case e: Exception =>
          val errMsg = s"Failed with spark.shuffle.spill.compress=$shuffleSpillCompress," +
            s" spark.shuffle.compress=$shuffleCompress"
          throw new Exception(errMsg, e)
      }
    }

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }


  //NOTE: the following is supposed to be put into a different test suite.
  test ("aggregateByKey") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val pairs = sc.parallelize(Array((1, 1), (1, 1), (3, 2), (5, 1), (5, 3)), 10)

    val sets = pairs.aggregateByKey(new HashSet[Int](), 6)(_ += _, _ ++= _).collect()
    assert(sets.size === 3)
    val valuesFor1 = sets.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1))
    val valuesFor3 = sets.find(_._1 == 3).get._2
    assert(valuesFor3.toList.sorted === List(2))
    val valuesFor5 = sets.find(_._1 == 5).get._2
    assert(valuesFor5.toList.sorted === List(1, 3))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  //NOTE: the following is supposed to be put into a different test suite.
  test ("groupByKey") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1)), 10)
    val groups = pairs.groupByKey(7).collect()
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("groupByKey with duplicates") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)), 10)
    val groups = pairs.groupByKey(6).collect()
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("groupByKey with negative key hash codes") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val pairs = sc.parallelize(Array((-1, 1), (-1, 2), (-1, 3), (2, 1)), 10)
    val groups = pairs.groupByKey(6).collect()
    assert(groups.size === 2)
    val valuesForMinus1 = groups.find(_._1 == -1).get._2
    assert(valuesForMinus1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("groupByKey with many output partitions") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1)), 3)
    val groups = pairs.groupByKey(10).collect()
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("reduceByKey") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)), 20)
    val sums = pairs.reduceByKey(_+_, 5).collect()
    assert(sums.toSet === Set((1, 7), (2, 1)))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("reduceByKey with collectAsMap") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)), 10)
    val sums = pairs.reduceByKey(_+_, 5).collectAsMap()
    assert(sums.size === 2)
    assert(sums(1) === 7)
    assert(sums(2) === 1)

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("reduceByKey with many output partitons") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)), 10)
    val sums = pairs.reduceByKey(_+_, 7).collect()
    assert(sums.toSet === Set((1, 7), (2, 1)))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("reduceByKey with partitioner") {

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val p = new Partitioner() {
      def numPartitions = 20
      def getPartition(key: Any) = key.asInstanceOf[Int]
    }
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 1), (0, 1))).partitionBy(p)
    val sums = pairs.reduceByKey(_+_)
    assert(sums.collect().toSet === Set((1, 4), (0, 1)))
    assert(sums.partitioner === Some(p))
    // count the dependencies to make sure there is only 1 ShuffledRDD
    val deps = new HashSet[RDD[_]]()
    def visit(r: RDD[_]) {
      for (dep <- r.dependencies) {
        deps += dep.rdd
        visit(dep.rdd)
      }
    }
    visit(sums)
    assert(deps.size === 2) // ShuffledRDD, ParallelCollection.

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("join") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)), 10)
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')), 8)
    val joined = rdd1.join(rdd2, 6).collect()
    assert(joined.size === 4)
    assert(joined.toSet === Set(
      (1, (1, 'x')),
      (1, (2, 'x')),
      (2, (1, 'y')),
      (2, (1, 'z'))
    ))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("join all-to-all") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (1, 3)), 10)
    val rdd2 = sc.parallelize(Array((1, 'x'), (1, 'y')), 11)
    val joined = rdd1.join(rdd2, 7).collect()
    assert(joined.size === 6)
    assert(joined.toSet === Set(
      (1, (1, 'x')),
      (1, (1, 'y')),
      (1, (2, 'x')),
      (1, (2, 'y')),
      (1, (3, 'x')),
      (1, (3, 'y'))
    ))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test  ("leftOuterJoin") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)), 10)
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')), 20)
    val joined = rdd1.leftOuterJoin(rdd2, 8).collect()
    assert(joined.size === 5)
    assert(joined.toSet === Set(
      (1, (1, Some('x'))),
      (1, (2, Some('x'))),
      (2, (1, Some('y'))),
      (2, (1, Some('z'))),
      (3, (1, None))
    ))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("rightOuterJoin") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)), 10)
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')), 11)
    val joined = rdd1.rightOuterJoin(rdd2, 5).collect()
    assert(joined.size === 5)
    assert(joined.toSet === Set(
      (1, (Some(1), 'x')),
      (1, (Some(2), 'x')),
      (2, (Some(1), 'y')),
      (2, (Some(1), 'z')),
      (4, (None, 'w'))
    ))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("fullOuterJoin") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)), 10)
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')), 12)
    val joined = rdd1.fullOuterJoin(rdd2, 7).collect()
    assert(joined.size === 6)
    assert(joined.toSet === Set(
      (1, (Some(1), Some('x'))),
      (1, (Some(2), Some('x'))),
      (2, (Some(1), Some('y'))),
      (2, (Some(1), Some('z'))),
      (3, (Some(1), None)),
      (4, (None, Some('w')))
    ))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test  ("join with no matches") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)), 10)
    val rdd2 = sc.parallelize(Array((4, 'x'), (5, 'y'), (5, 'z'), (6, 'w')), 20)
    val joined = rdd1.join(rdd2, 9).collect()
    assert(joined.size === 0)

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test  ("join with many output partitions") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)), 20)
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')), 11)
    val joined = rdd1.join(rdd2, 10).collect()
    assert(joined.size === 4)
    assert(joined.toSet === Set(
      (1, (1, 'x')),
      (1, (2, 'x')),
      (2, (1, 'y')),
      (2, (1, 'z'))
    ))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("groupWith") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)), 10)
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')), 11)
    val joined = rdd1.groupWith(rdd2).collect()
    assert(joined.size === 4)
    val joinedSet = joined.map(x => (x._1, (x._2._1.toList, x._2._2.toList))).toSet
    assert(joinedSet === Set(
      (1, (List(1, 2), List('x'))),
      (2, (List(1), List('y', 'z'))),
      (3, (List(1), List())),
      (4, (List(), List('w')))
    ))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("groupWith3") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)), 10)
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')), 12)
    val rdd3 = sc.parallelize(Array((1, 'a'), (3, 'b'), (4, 'c'), (4, 'd')), 9)
    val joined = rdd1.groupWith(rdd2, rdd3).collect()
    assert(joined.size === 4)
    val joinedSet = joined.map(x => (x._1,
      (x._2._1.toList, x._2._2.toList, x._2._3.toList))).toSet
    assert(joinedSet === Set(
      (1, (List(1, 2), List('x'), List('a'))),
      (2, (List(1), List('y', 'z'), List())),
      (3, (List(1), List(), List('b'))),
      (4, (List(), List('w'), List('c', 'd')))
    ))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("groupWith4") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME)

    sc = new SparkContext("local", "test", conf)

    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)), 10)
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')), 8)
    val rdd3 = sc.parallelize(Array((1, 'a'), (3, 'b'), (4, 'c'), (4, 'd')), 14)
    val rdd4 = sc.parallelize(Array((2, '@')), 3)
    val joined = rdd1.groupWith(rdd2, rdd3, rdd4).collect()
    assert(joined.size === 4)
    val joinedSet = joined.map(x => (x._1,
      (x._2._1.toList, x._2._2.toList, x._2._3.toList, x._2._4.toList))).toSet
    assert(joinedSet === Set(
      (1, (List(1, 2), List('x'), List('a'), List())),
      (2, (List(1), List('y', 'z'), List(), List('@'))),
      (3, (List(1), List(), List('b'), List())),
      (4, (List(), List('w'), List('c', 'd'), List()))
    ))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("default partitioner uses partition size") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME)

    sc = new SparkContext("local", "test", conf)

    // specify 2000 partitions
    val a = sc.makeRDD(Array(1, 2, 3, 4), 200)
    // do a map, which loses the partitioner
    val b = a.map(a => (a, (a * 2).toString))
    // then a group by, and see we didn't revert to 2 partitions
    val c = b.groupByKey()
    assert(c.partitions.size === 200)

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("default partitioner uses largest partitioner") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME)

    sc = new SparkContext("local", "test", conf)


    val a = sc.makeRDD(Array((1, "a"), (2, "b")), 2)
    val b = sc.makeRDD(Array((1, "a"), (2, "b")), 200)
    val c = a.join(b)
    assert(c.partitions.size === 200)

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("lookup with partitioner") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", TestConstants.SPARK_WORKER_CORES);
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME)


    sc = new SparkContext("local", "test", conf)

    val pairs = sc.parallelize(Array((1,2), (3,4), (5,6), (5,7)))

    val p = new Partitioner {
      def numPartitions: Int = 2

      def getPartition(key: Any): Int = Math.abs(key.hashCode() % 2)
    }
    val shuffled = pairs.partitionBy(p)

    assert(shuffled.partitioner === Some(p))
    assert(shuffled.lookup(1) === Seq(2))
    //NOTE: we need to have sort, in multi-threading case.
    assert(shuffled.lookup(5).sorted === Seq(6,7)) 
    assert(shuffled.lookup(-1) === Seq())

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }
}

