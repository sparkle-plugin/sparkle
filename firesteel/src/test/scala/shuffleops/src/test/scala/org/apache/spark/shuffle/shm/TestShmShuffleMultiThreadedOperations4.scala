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

class  TestShmShuffleMultiThreadedOperations4 extends FunSuite with Matchers with LocalSparkContext with Logging {

  //to configure log4j properties to follow what are specified in log4j.properties
   override def beforeAll() {
     //when invoke from maven, what gets loaded is the one at: src/test/resources
     //PropertyConfigurator.configure("log4j.properties")
   }

  //NOTE: my current C++ shuffle engine should have an ascending order in terms of key ordering.
  test ("sorting on mutable pairs") {

    logInfo(" test:sorting on mutable pairs at process: " + 
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

    logInfo(" test:cogroup using mutable pairs at process: " + 
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

    logInfo(" test:subtract mutable pairs at process: " + 
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
  ignore ("shuffle with different compression settings (SPARK-3426)") {

    logInfo(" test:shuffle with different compression settings (SPARK-3426) at process: " + 
           java.lang.management.ManagementFactory.getRuntimeMXBean().getName());
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");

    //added by Retail Memory Broker related. 
    //NOTE: Because the context gets create withthe new one, the new batch of threads will be 
    //created to be associated with the new context.
    //4*4 because we will have 4 combinations and each context use 4 threads.
    //if we put to 15, then it will complain that logical thread id: 15, is not in range of (0, 15)
    conf.set("SPARK_WORKER_CORES", "16");
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
      sc = new SparkContext("local[4]", "test", conf)

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
    logInfo(" test:aggregateByKey at process: " + 
           java.lang.management.ManagementFactory.getRuntimeMXBean().getName());

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local[4]", "test", conf)

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

    logInfo(" test:groupByKey at process: " + 
           java.lang.management.ManagementFactory.getRuntimeMXBean().getName());

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local[4]", "test", conf)

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

    logInfo(" test:groupByKey with duplicates at process: " + 
           java.lang.management.ManagementFactory.getRuntimeMXBean().getName());

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local[4]", "test", conf)

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

    logInfo(" test:groupByKey with negative key hash codes at process: " + 
           java.lang.management.ManagementFactory.getRuntimeMXBean().getName());

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local[4]", "test", conf)

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
    logInfo(" test:groupByKey with many output partitions at process: " + 
           java.lang.management.ManagementFactory.getRuntimeMXBean().getName());

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local[4]", "test", conf)

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

    logInfo(" test:reduceByKey at process: " + 
           java.lang.management.ManagementFactory.getRuntimeMXBean().getName());

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name",TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local[4]", "test", conf)

    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)), 20)
    val sums = pairs.reduceByKey(_+_, 5).collect()
    assert(sums.toSet === Set((1, 7), (2, 1)))

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()

  }

  test ("reduceByKey with collectAsMap") {

    logInfo(" test:reduceByKey with collectAsMap at process: " + 
           java.lang.management.ManagementFactory.getRuntimeMXBean().getName());

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local[4]", "test", conf)

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

    logInfo(" test:reduceByKey with many output partitons at process: " + 
           java.lang.management.ManagementFactory.getRuntimeMXBean().getName());

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local[10]", "test", conf)

    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)), 10)
    val sums = pairs.reduceByKey(_+_, 7).collect()
    assert(sums.toSet === Set((1, 7), (2, 1)))

    sc.stop()

    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("reduceByKey with partitioner") {
    logInfo(" test:reduceByKey with partitioner at process: " + 
           java.lang.management.ManagementFactory.getRuntimeMXBean().getName());

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local[4]", "test", conf)

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
}

