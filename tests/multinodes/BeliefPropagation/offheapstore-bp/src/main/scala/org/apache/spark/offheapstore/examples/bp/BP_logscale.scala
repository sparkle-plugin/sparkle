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

import org.apache.spark.internal.Logging
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import org.apache.spark.offheapstore.impl.UnsafeDoubleArray

import scala.reflect.ClassTag

object BP_logscale extends Logging {
def runUntilConvergence[VD: ClassTag, ED: ClassTag](vertices: RDD[(VertexId, VD)],
                                                    edges: RDD[Edge[ED]], iter: Int) :
RDD[(VertexId, (Double, Double))] =
{

  def vertexProgram(id: VertexId,
                    vd: VD,
                    msgProd: (Double, Double)): VD = {
    /***** belief computation *****/
    // attr: potentials, beliefs
    val attr = vd.asInstanceOf[(Double, Double, Double, Double, Double)]

    val oldBelief1 = attr._3
    val oldBelief2 = attr._4
    val tmpBelief1 = math.log(attr._1) + msgProd._1
    val tmpBelief2 = math.log(attr._2) + msgProd._2

    val maxBelief = math.max(tmpBelief1, tmpBelief2)
    val tmpBelief1_1 = math.exp(tmpBelief1 - maxBelief)
    val tmpBelief1_2 = math.exp(tmpBelief2 - maxBelief)

    val sum = tmpBelief1_1 + tmpBelief1_2
    val newBelief1 = tmpBelief1_1/sum
    val newBelief2 = tmpBelief1_2/sum

    val maxDiff = math.max(math.abs(newBelief1-oldBelief1),math.abs(newBelief2-oldBelief2))
    (attr._1, attr._2, newBelief1, newBelief2, maxDiff).asInstanceOf[VD]
  }

  def sendMessage(edge: EdgeTriplet[VD,ED], idx: Int,
    arr: UnsafeDoubleArray) = {


      /***** message computation

 edge->curMessages[0][0] =
                    edge->potential[0][0]*
(belief_propagation<F>::vertex_list[edge->v[0]].belief[0]/
edge->prevMessages[0][1]) +
edge->potential[1][0]*(belief_propagation<F>::vertex_list[edge->v[0]].belief[1]/
edge->prevMessages[1][1]);
        edge->curMessages[1][0] =
                    edge->potential[0][1]*
(belief_propagation<F>::vertex_list[edge->v[0]].belief[0]/
edge->prevMessages[0][1]) +
edge->potential[1][1]*(belief_propagation<F>::vertex_list[edge->v[0]].belief[1]/
edge->prevMessages[1][1]);

        // edge message for the other direction v2 -> v1
        edge->curMessages[0][1] =
                    edge->potential[0][0]*
(belief_propagation<F>::vertex_list[edge->v[1]].belief[0]/
edge->prevMessages[0][0]) +
edge->potential[0][1]*(belief_propagation<F>::vertex_list[edge->v[1]].belief[1]/
edge->prevMessages[1][0]);
        edge->curMessages[1][1] =
                    edge->potential[1][0]*
(belief_propagation<F>::vertex_list[edge->v[1]].belief[0]/
edge->prevMessages[0][0]) +
edge->potential[1][1]*
(belief_propagation<F>::vertex_list[edge->v[1]].belief[1]/
edge->prevMessages[1][0]);

        ******/

    val attr = arr.get[(Double,Double,Double,Double,Double,Double,Double,Double)](idx)
    val srcAttr = edge.srcAttr.asInstanceOf[(Double,Double,Double,Double,Double)]
    val dstAttr = edge.dstAttr.asInstanceOf[(Double,Double,Double,Double,Double)]
    //println(edge._2 + ":" + edge._1.srcId + ","+ edge._1.dstId + ", oldAttr=" + attr);

      var curMessage00 = attr._1*srcAttr._3/attr._6 +
        attr._2*srcAttr._4/attr._8
      var curMessage10 = attr._3*srcAttr._3/attr._6 +
        attr._4*srcAttr._4/attr._8
      var curMessage01 = attr._1*dstAttr._3/attr._5 +
        attr._3*dstAttr._4/attr._7
      var curMessage11 = attr._2*dstAttr._3/attr._5 +
        attr._4*dstAttr._4/attr._7


      val sum = curMessage00 + curMessage10
      curMessage00 = curMessage00/sum
      curMessage10 = curMessage10/sum

      val sum1 = curMessage01 + curMessage11
      curMessage01 = curMessage01/sum1
      curMessage11 = curMessage11/sum1

    val newVal: (Double, Double, Double, Double) =
      (curMessage00,curMessage01,curMessage10,curMessage11)


    arr.set(idx, 5, newVal._1)
    arr.set(idx, 6, newVal._2)
    arr.set(idx, 7, newVal._3)
    arr.set(idx, 8, newVal._4)


    (Iterator((edge.srcId, (math.log(curMessage01), math.log(curMessage11))),
      (edge.dstId, (math.log(curMessage00), math.log(curMessage10)))))


  }

  def messageCombiner(a: (Double, Double),
                      b: (Double, Double)):
  (Double, Double) = {

    (a._1 + b._1, a._2 + b._2)
  }
  // Execute a dynamic version of Pregel.
  OffHeapStoreBP(vertices,
    edges, maxIterations=iter)(
    vertexProgram, sendMessage, messageCombiner)

  }
}
