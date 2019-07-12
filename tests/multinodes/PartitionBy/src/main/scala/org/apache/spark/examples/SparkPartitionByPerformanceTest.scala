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

/** simple multi-processed partitionby test*/
object SparkPartitionByPerformanceTest {
  def printList(args: TraversableOnce[_]): Unit= {
     args.foreach(println)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("simple multi-processed PartitionBy")
    val sc = new SparkContext(conf)

    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.shm.ShmShuffleManager")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64")

    //added by Retail Memory Broker related
    //should be bigger than specified next in local[*]
    //conf.set("SPARK_WORKER_CORES", "2");
    conf.set("spark.executor.shm.globalheap.name", "/dev/shm/nvm/global0");


    val  numMappers = 200
    val  numKVPairs = 2000000; //10 times to 20 million is affordable.
    val  valSize = 16 //similar to TeraSort
    val  numReducers = numMappers
    val  repetitions = 5 //roughly we will have 5 identifical keys
    val  upperRange =numKVPairs/repetitions*numMappers

    val pairs1 = sc.parallelize(0 until numMappers, numMappers).zipWithIndex.flatMap {
      case (index, value) =>
      //also this has to be at the begining of the iteration, not for each iteration.
      val ranGen = new Random(index*numKVPairs*valSize*numMappers)
      var arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(upperRange), byteArr)
      }
      arr1
    }.cache()


    //NOTE: important: Enforce that everything has been calculated and in cache
    pairs1.count()

    val timeSort0 =System.currentTimeMillis()
    val partitionedByRdd  = pairs1.partitionBy(new HashPartitioner(numReducers))
    partitionedByRdd.count()
    val timeSort1 = System.currentTimeMillis()

    //for key correctness
    val sumKeys = partitionedByRdd.map (x => x._1).sum

    //for value correctness
    val sumValues= partitionedByRdd.map(x => x._2).map (v => {
       var vsum = 0
       v.foreach(vsum += _.toInt)
       vsum
      }
    ).sum

    val pairsCountAfterGroup=partitionedByRdd.count()
    val pairs1Count = pairs1.count()

    println("the groupped pairs have the count: " + pairsCountAfterGroup +" vs."
                      + pairs1Count)
    println("sort time is (milliseconds): " + (timeSort1-timeSort0))
    println("total sum of the keys is: " + sumKeys)

    println("total sum of the values is: " + sumValues)

    sc.stop()

  }
}
