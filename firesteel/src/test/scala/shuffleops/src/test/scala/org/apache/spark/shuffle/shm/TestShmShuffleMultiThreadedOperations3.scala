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

class  TestShmShuffleMultiThreadedOperations3 extends FunSuite with Matchers with LocalSparkContext with Logging {

  //to configure log4j properties to follow what are specified in log4j.properties
   override def beforeAll() {
     //when invoke from maven, what gets loaded is the one at: src/test/resources
     //PropertyConfigurator.configure("log4j.properties")
   }

  test ("default partitioner uses partition size") {

    logInfo(" test:default partitioner uses partition size at process: " + 
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

  test("default partitioner uses largest partitioner") {

    logInfo(" test:default partitioner uses largest partitioner at process: " + 
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


    val a = sc.makeRDD(Array((1, "a"), (2, "b")), 2)
    val b = sc.makeRDD(Array((1, "a"), (2, "b")), 200)
    val c = a.join(b)
    assert(c.partitions.size === 200)

    sc.stop()

    //format for next test:
    ShuffleStoreManager.INSTANCE.formatshm()
  }

  test ("lookup with partitioner") {

    logInfo(" test:lookup with partitioner at process: " + 
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
