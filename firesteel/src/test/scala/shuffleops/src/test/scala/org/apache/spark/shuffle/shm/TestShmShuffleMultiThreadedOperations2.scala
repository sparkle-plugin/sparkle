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

class  TestShmShuffleMultiThreadedOperations2 extends FunSuite with Matchers with LocalSparkContext with Logging {

  //to configure log4j properties to follow what are specified in log4j.properties
   override def beforeAll() {
     //when invoke from maven, what gets loaded is the one at: src/test/resources
     //PropertyConfigurator.configure("log4j.properties")
   }

  test ("join") {

    logInfo(" test:join " + 
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

    logInfo(" test:join all-to-all at process: " + 
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

  test ("leftOuterJoin") {
    logInfo(" test:leftOuterJoin at process: " + 
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

  test("rightOuterJoin") {
    logInfo(" test:rightOuterJoin at process: " + 
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

 ignore ("fullOuterJoin") {
    logInfo(" test:fullOuterJoin at process: " + 
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

  test ("join with no matches") {
    logInfo(" test:join with no matches at process: " + 
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

    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)), 10)
    val rdd2 = sc.parallelize(Array((4, 'x'), (5, 'y'), (5, 'z'), (6, 'w')), 20)
    val joined = rdd1.join(rdd2, 9).collect()
    assert(joined.size === 0)

    sc.stop()
    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()

  }

  test ("join with many output partitions") {

    logInfo(" test:join with many output partitions at process: " + 
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
    logInfo(" test:groupWith at process: " + 
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

    logInfo(" test:groupWith3 at process: " + 
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

    logInfo(" test:groupWith4 at process: " + 
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
}
