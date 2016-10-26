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

package org.apache.spark.examples

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 */
object SparkPageRank {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter> <re-partition numbers>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("PageRank")
    val iters = if (args.length > 0) args(1).toInt else 10
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile(args(0), 1)

    //added by Jun Li
    val numberOfTasks = if (args.length > 0) args(2).toInt else 200

    //added by Jun Li, to set the number of the tasks 
    //NOTE: the first wide-dependency operator will be dependent on the block-size of HDFS. 
    //modified by Jun Li: to change the key from the default string to become Int.
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0).toLong, parts(1).toLong)
    }.groupByKey(numberOfTasks).cache()

    //NOTE: I remove "distinct", as I do not support key to be an object at this time.
    //used to b: .repartition(..).distinct().groupByKey()

    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).
          values.
          flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).
                       mapValues(0.15 + 0.85 * _)
    }

    //val output = ranks.collect()
    val output = ranks.top(10) {
           Ordering.by(e => e._2)
    }
    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

    val count = ranks.count();
    println("total ranks' count is: " + count);

    ctx.stop()
  }
}
