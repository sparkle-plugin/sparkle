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

package org.apache.spark.offheapstore.examples.bp

import org.apache.spark.graphx.{OffHeapStoreBP,Edge}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext, HashPartitioner}
import org.apache.spark.SparkContext._

object RunBP {

  def main(args: Array[String]): Unit = {

    val conf =
      new SparkConf().setAppName("Off-Heap Store Belief Propagation Application")

    if (args.length < 4) {
        println("Usage:  spark-submit org.apache.spark.offheapstore.examples.bp.RunBP " +
                "offheapstore-2.0.0-SNAPSHOT-jar-with-dependencies.jar " +
                "<vertex input file> <edge input file> <maximum iteration no> " + 
                "<vertex partition no> <edge partition no (optional)>")
        System.exit(0) 
    }

    val sc = new SparkContext(conf)
    val vertices: RDD[String] = sc.textFile(args(0))
    val edges: RDD[String] = sc.textFile(args(1))
    val maxIterations = args(2).toInt

    val vertexRDD = vertices.map { line =>
      val fields = line.split(' ')
      (fields(0).toLong, (fields(1).toDouble, fields(2).toDouble, fields(1).toDouble, fields(2).toDouble, 0.0))
    }
    val edgeRDD = edges.map { line =>
      val fields = line.split(' ')
      Edge(fields(0).toLong, fields(1).toLong,
          (fields(2).toDouble, fields(3).toDouble,
           fields(4).toDouble, fields(5).toDouble, 1.0, 1.0, 1.0, 1.0))
    }

    var repVertexRDD = vertexRDD
    var repEdgeRDD = edgeRDD

    val nvertexpart = if (args.length > 3) args(3).toInt else 0
    val nedgepart = if (args.length > 4) args(4).toInt else 0

    if(nvertexpart !=0 && nedgepart != 0) {

      //repVertexRDD = vertexRDD.repartition(nvertexpart)
      repVertexRDD = vertexRDD.partitionBy(new HashPartitioner(nvertexpart))
      repEdgeRDD = edgeRDD.repartition(nedgepart)


    } else if(nvertexpart !=0) {

      //repVertexRDD = vertexRDD.repartition(nvertexpart)
      repVertexRDD = vertexRDD.partitionBy(new HashPartitioner(nvertexpart))

    }

    // get rid of attributes of vertices and edges
    val repVertexRDDwithoutAttr = repVertexRDD.map{ a => (a._1, ()) }
    val repEdgeRDDwithoutAttr = 
                    repEdgeRDD.map{ a => Edge(a.srcId, a.dstId, ()) }

    OffHeapStoreBP.build(sc, repVertexRDD, repEdgeRDD)

    val time = System.currentTimeMillis()

    BP_logscale.runUntilConvergence(repVertexRDDwithoutAttr, repEdgeRDDwithoutAttr, maxIterations)

    println("Total time: " + (System.currentTimeMillis() - time) + " ms." )
  }
}
