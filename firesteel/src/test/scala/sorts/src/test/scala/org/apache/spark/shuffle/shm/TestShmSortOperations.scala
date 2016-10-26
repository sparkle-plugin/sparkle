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


class  TestShmSortOperations  extends FunSuite with Matchers with LocalSparkContext with Logging {

  //to configure log4j properties to follow what are specified in log4j.properties
  override def beforeAll() {
     PropertyConfigurator.configure("log4j.properties")
  }

  ignore ("simple sortByKey") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "shm")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)
    val pairs = sc.parallelize(Array((1, 0), (2, 0), (0, 0), (3, 0)), 2)
    assert(pairs.sortByKey().collect() === Array((0,0), (1,0), (2,0), (3,0)))

    sc.stop()
  }


  ignore ("large array") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "shm")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr, 2)
    val sorted = pairs.sortByKey()
    assert(sorted.partitions.size === 2)
    assert(sorted.collect() === pairArr.sortBy(_._1))

    sc.stop()
  }


  ignore ("large array with many partitions") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "shm")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr, 2)
    val sorted = pairs.sortByKey(true, 20)
    assert(sorted.partitions.size === 20)
    assert(sorted.collect() === pairArr.sortBy(_._1))

    sc.stop()
  }


  ignore ("more partitions than elements") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "shm")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val rand = new scala.util.Random()
    val pairArr = Array.fill(10) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr, 30)
    assert(pairs.sortByKey().collect() === pairArr.sortBy(_._1))


    sc.stop()
  }


  ignore ("empty RDD for sorting") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "shm")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val pairArr = new Array[(Int, Int)](0)
    val pairs = sc.parallelize(pairArr, 2)
    assert(pairs.sortByKey().collect() === pairArr.sortBy(_._1))

    sc.stop()
  }


  test ("partition balancing") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "shm")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");
    //added by Retail Memory Broker related 
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local", "test", conf)

    val pairArr = (1 to 1000).map(x => (x, x)).toArray
    val sorted = sc.parallelize(pairArr, 4).sortByKey()
    assert(sorted.collect() === pairArr.sortBy(_._1))
    val partitions = sorted.collectPartitions()
    logInfo("Partition lengths: " + partitions.map(_.length).mkString(", "))
    val lengthArr = partitions.map(_.length)
    lengthArr.foreach { len =>
      assert(len > 100 && len < 400)
    }
    partitions(0).last should be < partitions(1).head
    partitions(1).last should be < partitions(2).head
    partitions(2).last should be < partitions(3).head

    sc.stop()
  }


}

