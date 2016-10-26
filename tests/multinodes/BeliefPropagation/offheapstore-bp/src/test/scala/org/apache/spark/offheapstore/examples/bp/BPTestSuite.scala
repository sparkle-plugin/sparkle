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

import org.apache.spark.graphx._
import org.scalatest.FunSuite
import org.apache.spark.LocalSparkContext
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

class BPTestSuite extends FunSuite with LocalSparkContext {


  test("off-heap store bp test") {

    withSpark { sc =>

      val tol = 1e-4


      val vertices: RDD[String] = sc.textFile("data/vertex100.txt")
      val edges: RDD[String] = sc.textFile("data/edge100.txt")
      val result: RDD[String] = sc.textFile("data/result100.txt")

      def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
      println(getCurrentDirectory)
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
      val trueProbabilities: RDD[(Long, (Double, Double))] =
        result.map { line =>
        val fields = line.split(' ')
        (fields(0).toLong, (fields(1).toDouble, fields(2).toDouble))
      }
      val repVertexRDD = vertexRDD //.repartition(2)
      val repEdgeRDD = edgeRDD //.repartition(3)

      val repVertexRDDwithoutAttr = repVertexRDD.map{ a => (a._1, ()) }
      val repEdgeRDDwithoutAttr = repEdgeRDD.map{ a => Edge(a.srcId, a.dstId, ()) }

      OffHeapStoreBP.build(sc, repVertexRDD, repEdgeRDD)

      val time1 = System.currentTimeMillis()
      val calculatedProbabilities = BP_logscale.runUntilConvergence(repVertexRDDwithoutAttr, repEdgeRDDwithoutAttr, 50)
      val time2 = System.currentTimeMillis()
      val eps = 10e-5
      calculatedProbabilities.join(trueProbabilities).collect().foreach {
        case (vertexId, (calP, trueP)) => 
          assert(trueP._1 - calP._1 < eps && trueP._2 - calP._2 < eps)
      }
      println("BP_logscale.runUntilConvergence time is: " + (time2 - time1) + " (ms) " )
    }
  }
}
