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

package org.apache.spark.examples

import java.util.Random
import org.apache.spark._

//NOTE: you need the following import to get to the key/value pair RDD.
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.{CoGroupedRDD, OrderedRDDFunctions, RDD, ShuffledRDD, SubtractedRDD}
import scala.collection.mutable.HashSet

/** simple multi-processed join test, with number of keys greater than 
 ** what an integer key field can hold
 */
object SparkJoinPerformanceTestWithLong {
  def printList(args: TraversableOnce[_]): Unit= {
     args.foreach(println)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("simple multi-processed join")
    val sc = new SparkContext(conf)

    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    //conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")

    //added by Retail Memory Broker related
    //should be bigger than specified next in local[*]
    //conf.set("SPARK_WORKER_CORES", "2");
    conf.set("spark.executor.shm.globalheap.name", "/dev/shm/nvm/global0");


    val  numMappers = 200
    val  numKVPairs = 1000000; //reduce to half, compared to groupBy and reduceBy
    val  valSize = 16 //2 doubles
    val  numReducers = numMappers
    val  repetitions = 5 //roughly we will have 5 identifical keys
    val  upperRange =numKVPairs.toLong/repetitions.toLong * numMappers.toLong

    //to contruct RDD 1
    val LabelVal_1 = 1*17
    val RDD1 = sc.parallelize(0 until numMappers, numMappers).zipWithIndex.flatMap {
      case (index, value) =>
      //also this has to be at the begining of the iteration, not for each iteration.
      val ranGen =
        new Random(index.toLong * numKVPairs.toLong *valSize.toLong *numMappers.toLong * LabelVal_1.toLong)
      var arr1 = new Array[(Long, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) =( (ranGen.nextDouble() *upperRange).toLong, byteArr)
      }
      arr1
    }.mapPartitions(x => x).cache()

    //to construct RDD_1
    val LabelVal_2 = 1*37
    val RDD2 = sc.parallelize(0 until numMappers, numMappers).zipWithIndex.flatMap {
      case (index, value) =>
      //also this has to be at the begining of the iteration, not for each iteration.
      val ranGen =
        new Random(index.toLong * numKVPairs.toLong *valSize.toLong *numMappers.toLong * LabelVal_2.toLong)
      var arr1 = new Array[(Long, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) =( (ranGen.nextDouble() *upperRange).toLong, byteArr)
      }
      arr1
    }.mapPartitions(x => x).cache()


    //NOTE: important: Enforce that everything has been calculated and in cache
    RDD1.foreachPartition( x => {} );
    RDD2.foreachPartition( x => {} );

    val timeJoin0 =System.currentTimeMillis()
    val joinedRdd  = RDD1.join(RDD2, numReducers)
    //need to use count instead.
    //joinedRdd.foreachPartition( x => {} )
    joinedRdd.count()
    val timeJoin1 = System.currentTimeMillis()

    //for key correctness
    val sumKeys =joinedRdd.map (x => x._1).sum

    //for value correctness
    val sumValues= joinedRdd.values.map (pair=> {
       var vsum = 0
       pair._1.foreach(vsum += _.toInt)
       pair._2.foreach(vsum += _.toInt)

       vsum
      }
    ).sum


    val rdd1Count= RDD1.count();
    val rdd2Count= RDD2.count();
    val countAfterJoinedRDD  = joinedRdd.count()

    println("the RDD1 has the count: " + rdd1Count +" vs. RDD 2's count: " + rdd2Count)
    println("the joined RDD has the count: " + countAfterJoinedRDD)

    println("join time is (milliseconds): " + (timeJoin1-timeJoin0))

    println("total sum of the keys is: " + sumKeys)
    println("total sum of the values is: " + sumValues)


    sc.stop()

  }
}
