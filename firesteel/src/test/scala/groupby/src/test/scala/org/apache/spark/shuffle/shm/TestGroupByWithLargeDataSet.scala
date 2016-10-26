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
import org.scalatest.{Matchers, FunSuite}
import org.apache.spark._
import org.apache.spark.internal.Logging

import org.apache.spark.rdd.{CoGroupedRDD, OrderedRDDFunctions, RDD, ShuffledRDD, SubtractedRDD}

import java.util.Random
import org.apache.log4j.PropertyConfigurator

class  TestGroupByWithLargeDataSet extends FunSuite with Matchers with LocalSparkContext with Logging {

  //to configure log4j properties to follow what are specified in log4j.properties
  override def beforeAll() {
     PropertyConfigurator.configure("log4j.properties")
  }

  test ("groupBy test with large data set and partitions") {
    val  numMappers = 200
    val  numKVPairs = 1000000
    val  valSize = 90 //similar to TeraSort
    val numReducers = numMappers


    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "shm")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")
    //added by Retail Memory Broker related
    conf.set("SPARK_WORKER_CORES", "10");
    conf.set("spark.executor.shm.globalheap.name", TestConstants.GLOBAL_HEAP_NAME);

    sc = new SparkContext("local[9]", "test", conf)
    //sc = new SparkContext("local", "test", conf)

    val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random
      var arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
      }
      arr1
    }.cache()

    //NOTE: important: Enforce that everything has been calculated and in cache
    val pairs1Count= pairs1.count();

    val timeSort0 =System.currentTimeMillis()
    val pairsCountAfterGroup  = pairs1.groupByKey(numReducers).count()
    val timeSort1 = System.currentTimeMillis()

    println("the groupped pairs have the count: " + pairsCountAfterGroup +" vs." 
                      + pairs1Count)
    println("sort time is (milliseconds): " + (timeSort1-timeSort0))

    sc.stop()
  }

}

