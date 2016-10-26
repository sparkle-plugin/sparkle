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

package org.apache.spark.shuffle.shm

import  org.apache.spark.shuffle.shm.ShmShuffleWithMultiValues._
import org.scalatest.FunSuite

import org.apache.spark.{SparkEnv, SparkContext, LocalSparkContext, SparkConf}
import org.apache.spark.Aggregator;


class TestBasicIteratorSuite extends FunSuite with LocalSparkContext {
   //use variable sc instead.
  private val test= new SparkConf(false)

  /**
   * to conver (k, {v}) into (k,v) pairs
   *
   */
  private def convertValues [K, V] (k: K, multipleValues: Seq[V]): Iterator[(K,V)] ={
    val iter = multipleValues.iterator
    def iterator: Iterator[(K,V)] = new Iterator[(K,V)] {

      override def hasNext: Boolean = iter.hasNext
      override def next(): (K,V) = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        (k, iter.next())

      }
    }

    iterator
  }


  test("to test on flat the key and multiple values into multiple key/value pairs") {
    //the iterator
    val x = Iterator ((1, Seq(2,3)), (4, Seq(5, 6, 11)), (4, Seq(11)))

    val transformedIterator = x.map(e => convertValues(e._1, e._2)).flatten

    assert(transformedIterator.next === (1,2))
    assert(transformedIterator.next === (1,3))
    assert(transformedIterator.next === (4,5))
    assert(transformedIterator.next === (4,6))
    assert(transformedIterator.next === (4,11))
    assert(transformedIterator.next === (4,11))
  }


  test("to test aggregator's newly introduced method for C++ shuffle engine") {
    val conf = new SparkConf(false)
    sc = new SparkContext("local", "test", conf)

    conf.set("spark.shuffle.spill", "true")
    val externalSorting = SparkEnv.get.conf.getBoolean("spark.shuffle.spill", true)

    val x = Iterator ((1, Seq(2,3)), (4, Seq(5, 6, 11)), (4, Seq(11)))
    //creat an aggregator
    val agg = new Aggregator[Int, Int, Int](i => i, (i, j) => i + j, (i, j) => i + j)
    val ord = implicitly[Ordering[Int]]
    //invoke the method
    val iterator = agg.combineMultiValuesByKey(x, null)

    assert(iterator.next === (1,5))
    assert(iterator.next === (4, 22))
    assert(iterator.next === (4, 11))

    //iterator.foreach(e=> println("first: " + e._1 + " second: " + e._2))


  }
}
