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

import org.apache.spark.shuffle.shm.ShmShuffleWithMultiValues._
import org.apache.spark.util.CompletionIterator
import org.scalatest.FunSuite
import org.scalatest.Ignore

import com.hp.hpl.firesteel.shuffle.ShuffleDataModel.ReduceStatus
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.CompactBuffer
import com.hp.hpl.firesteel.shuffle.simulated.SimulatedReduceSHMShuffleStore
import com.hp.hpl.firesteel.shuffle.ShuffleDataModel
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue

//to pass collection between Java and Scala
import scala.collection.JavaConversions._
import java.util.ArrayList;
import java.lang.{Float => JFloat}
import java.lang.{Integer => JInteger}
import java.lang.{Long => JLong}
import java.lang.{String => JString}

import org.apache.spark._


class TestShuffleSimulatedReaderSuite extends FunSuite with LocalSparkContext {
   //use variable sc instead.
  private val test= new SparkConf(false)

  //NOTE: after test result, this class will need to be merged back to the actual class
  // in "shm" package.
  private class ShmShuffleFetcherIterator()
                 extends Iterator[(Any, Seq[Any])] with Logging {

    private val SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE =  10;

    //when the actual key/value pairs returned has number smaller than the specified size,
    //we know that that is the last batch.
    private var endOfFetch =false;

    private val kvBuffer = new Queue[(Object, Seq[Object])]

    val reduceShuffleStore = new SimulatedReduceSHMShuffleStore(null, null, null);
    //control which option to go.
    reduceShuffleStore.setKValueTypeId(simulatedKValueTypeId)
    private val kvalueTypeId  = reduceShuffleStore.getKValueTypeId
    initialize()

    private [this] def initialize(): Unit= {
      //construct the map data sources.
      val executorIds =  new ArrayBuffer[String]()
      val mapIds = new ArrayBuffer[Int]()
      val sizes  = new ArrayBuffer[Long]();

      fetch() //make the first fetch.
    }

    private def fetch(): Unit = {
      //retrieve the first batch of the objects.
      var actualPairs = 0
      kvalueTypeId match {
        case  ShuffleDataModel.KValueTypeId.Int => {
          //to retrieve a collection of {k, {v1, v2,...}} from the C++ shuffle engine where
          val kvalues = new ArrayList[JInteger]();
          val vvalues = new ArrayList[ArrayList[Object]]();
          actualPairs= reduceShuffleStore.getKVPairsWithIntKeys(kvalues, vvalues,
            SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE)

          //now, push the data to the kvbuffer.
          for (i <- 0 to actualPairs-1) {
            val vv = vvalues(i).seq
            kvBuffer += Tuple2(kvalues(i), vv)
          }

          if (actualPairs < SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE ){
            endOfFetch = true;
          }
        }
        case  ShuffleDataModel.KValueTypeId.Float => {
          //to retrieve a collection of {k, {v1, v2,...}} from the C++ shuffle engine where
          val kvalues = new ArrayList[JFloat]();
          val vvalues = new ArrayList[ArrayList[Object]]();
          //WARNING: we need to provide list based APIS, instead of []. Need to see whether it
          //can be passed around
          actualPairs= reduceShuffleStore.getKVPairsWithFloatKeys(kvalues, vvalues,
            SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE)
          //now, push the data to the kvbuffer.
          for (i <- 0 to actualPairs-1) {
            val vv = vvalues(i).seq
            kvBuffer += Tuple2(kvalues(i), vv)
          }

          if (actualPairs < SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE ){
            endOfFetch = true;
          }
        }
        case  ShuffleDataModel.KValueTypeId.Long => {
          //to retrieve a collection of {k, {v1, v2,...}} from the C++ shuffle engine where
          val kvalues = new ArrayList[JLong]();
          val vvalues = new ArrayList[ArrayList[Object]]();
          //WARNING: we need to provide list based APIS, instead of []. Need to see whether it
          // can be passed around
          actualPairs= reduceShuffleStore.getKVPairsWithLongKeys(kvalues, vvalues,
            SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE)
          //now, push the data to the kvbuffer.
          for (i <- 0 to actualPairs-1) {
            val vv = vvalues(i).seq
            kvBuffer += Tuple2(kvalues(i), vv)
          }

          if (actualPairs < SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE ){
            endOfFetch = true;
          }
        }
        case  ShuffleDataModel.KValueTypeId.String => {
          //to retrieve a collection of {k, {v1, v2,...}} from the C++ shuffle engine where
          val kvalues = new ArrayList[JString]();
          val vvalues = new ArrayList[ArrayList[Object]]();
          //WARNING: we need to provide list based APIS, instead of []. Need to see whether it
          //can be passed around
          actualPairs= reduceShuffleStore.getKVPairsWithStringKeys(kvalues, vvalues,
            SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE)
          //now, push the data to the kvbuffer.
          for (i <- 0 to actualPairs-1) {
            val vv = vvalues(i).seq
            kvBuffer += Tuple2(kvalues(i), vv)
          }

          if (actualPairs < SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE ){
            endOfFetch = true;
          }
        }
        case  ShuffleDataModel.KValueTypeId.Object => {
          //to retrieve a collection of {k, {v1, v2,...}} from the C++ shuffle engine where
          val kvalues = new ArrayList[Object]();
          val vvalues = new ArrayList[ArrayList[Object]]();
          //WARNING: we need to provide list based APIS, instead of []. Need to see whether it
          //can be passed around
          actualPairs= reduceShuffleStore.getKVPairs(kvalues, vvalues,
            SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE)
          //now, push the data to the kvbuffer.
          for (i <- 0 to actualPairs-1) {
            val vv = vvalues(i).seq
            kvBuffer += Tuple2(kvalues(i), vv)
          }

          if (actualPairs < SHUFFLE_MERGED_KEYVALUE_PAIRS_RETRIEVAL_SIZE ){
            endOfFetch = true;
          }
        }
        case _ => {
          throw new NoSuchElementException
        }
      }


    }
    override def hasNext: Boolean = {
      //when exhausted. get to the next batch of the (K, {V}) pair.
      if (kvBuffer.size >0) {
        true;
      }
      else {
        if (!endOfFetch) {
          fetch() //make further fetch now
        }
        if (kvBuffer.size >0) {
          true;
        }
        else {
          false;
        }
      }
    }

    override def next(): (Any, Seq[Any]) = {
      kvBuffer.dequeue(); //it will return the dequeued element.
    }
  }

  private def simulatedFetch[T]():Iterator[T] = {
    val blockFetcherItr = new ShmShuffleFetcherIterator()

    blockFetcherItr.asInstanceOf[Iterator[T]]

  }
  private def simualtedShuffleRead[K,V, C](aggregator: Option[Aggregator[K,V,C]]): Iterator[Product2[K,C]] = {

    val iter = simulatedFetch()

    val aggregatedIter: Iterator[Product2[K, C]] = if (aggregator.isDefined) {

        //NOTE: why I do not have this problem in the ShmShuffleReader code?
        aggregator.get.combineMultiValuesByKey(iter, null)

    } else {
      // Convert the Product2s to pairs since this is what downstream RDDs currently expect
      // for us, we will need to revert back to <K,V> pair.
      //iter.asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
      iter.asInstanceOf[Iterator[(K, Seq[C])]].map(x=> convertValues(x._1, x._2)).flatten
    }

    //return
    aggregatedIter
  }

  private def convertValues [K, C] (k: K, multipleValues: Seq[C]): Iterator[(K,C)] ={
    val iter = multipleValues.iterator
    def iterator: Iterator[(K,C)] = new Iterator[(K,C)] {

      override def hasNext: Boolean = iter.hasNext
      override def next(): (K,C) = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        (k, iter.next())

      }
    }

    iterator
  }


  private  var simulatedKValueTypeId = ShuffleDataModel.KValueTypeId.Object

  ignore("to test how integer keys and associated values are retrieved by Shuffle Reader") {
    val conf = new SparkConf(false)
    sc = new SparkContext("local", "test", conf)

    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.spill", "true")
    val externalSorting = SparkEnv.get.conf.getBoolean("spark.shuffle.spill", true)

    //aggregator needs to have [K,V,C] three templated types.
    simulatedKValueTypeId = ShuffleDataModel.KValueTypeId.Int
    val agg = Some(new Aggregator[Int, Int, Int](i => i, (i, j) => i + j, (i, j) => i + j))
    val resultedIterator = simualtedShuffleRead (agg);

    resultedIterator.foreach(e=> println("Int-first: " + e._1 + " second: " + e._2))
  }

  ignore ("to test how float keys and associated values are retrieved by Shuffle Reader") {
    val conf = new SparkConf(false)
    sc = new SparkContext("local", "test", conf)

    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.spill", "true")
    val externalSorting = SparkEnv.get.conf.getBoolean("spark.shuffle.spill", true)

    simulatedKValueTypeId= ShuffleDataModel.KValueTypeId.Float
    val agg= Some (new Aggregator [Float, Float, Float](fi => fi, (fi, fj) => fi+ fj, (fi, fj) => fi+ fj))
    val resultedIterator = simualtedShuffleRead (agg);
    resultedIterator.foreach(e=> println("Float-first: " + e._1 + " second: " + e._2))
  }

  ignore("to test how long keys and associated values are retrieved by Shuffle Reader") {
    val conf = new SparkConf(false)
    sc = new SparkContext("local", "test", conf)

    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.spill", "true")
    val externalSorting = SparkEnv.get.conf.getBoolean("spark.shuffle.spill", true)

    simulatedKValueTypeId= ShuffleDataModel.KValueTypeId.Long
    val agg= Some (new Aggregator [Long, Long, Long](li => li, (li, lj) => li+ lj, (li, lj) => li + lj))
    val resultedIterator = simualtedShuffleRead (agg);
    resultedIterator.foreach(e=> println("Long-first: " + e._1 + " second: " + e._2))
  }

  ignore ("to test how string keys and associated values are retrieved by Shuffle Reader") {
    val conf = new SparkConf(false)
    sc = new SparkContext("local", "test", conf)

    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.spill", "true")
    val externalSorting = SparkEnv.get.conf.getBoolean("spark.shuffle.spill", true)

    simulatedKValueTypeId= ShuffleDataModel.KValueTypeId.String
    //set union for string.
    val createCombiner = (v: String) => CompactBuffer(v)
    val mergeValue = (buf: CompactBuffer[String], v: String) => buf += v
    val mergeCombiners = (c1: CompactBuffer[String], c2: CompactBuffer[String]) => c1 ++= c2

    val agg= Some (new Aggregator [String, String, CompactBuffer[String]](
                             createCombiner, mergeValue, mergeCombiners))
    val resultedIterator = simualtedShuffleRead (agg);
    resultedIterator.foreach(e=> println("String-first: " + e._1 + " second: " + e._2))
  }

  test("to test how object keys and associated values are retrieved by Shuffle Reader") {
    val conf = new SparkConf(false)
    sc = new SparkContext("local", "test", conf)

    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.spill", "true")
    val externalSorting = SparkEnv.get.conf.getBoolean("spark.shuffle.spill", true)

    simulatedKValueTypeId= ShuffleDataModel.KValueTypeId.Object
    //simulate the word count example..
    val createCombiner = (v: Int) => v.toInt
    val mergeValue = (v1:  Int, v2: Int) => v1.toInt + v2.toInt
    val mergeCombiners = (c1: Int, c2: Int) => c1 + c2

    val agg= Some (new Aggregator [String, Int, Int](
      createCombiner, mergeValue, mergeCombiners))
    val resultedIterator = simualtedShuffleRead (agg);
    resultedIterator.foreach(e=> println("Object-first: " + e._1 + " second: " + e._2))
  }

  //NOTE: for more complext key is object and value is object, such as key is <int, int>, we will have to
  //close the loop from the map to the reduce, so that scala created object from the map, will then be de-serialized
  //correctly at the reduce size, so that the type can be matched at both side.  We can only do this when
  //close the loop.
}
