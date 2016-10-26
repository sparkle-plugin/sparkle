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

import java.nio.ByteBuffer

import org.scalatest.FunSuite
import org.apache.spark.{SparkEnv, SparkContext, LocalSparkContext, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer._
import com.hp.hpl.firesteel.shuffle._
import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer._

import scala.collection.mutable.ArrayBuffer

case class RankingsClass (pagerank: Int,
                          pageurl: String,
                          avgduration: Int)

//NOTE: both case class and register class will have to be at the outer-most class scope. If I
//move these two into the test class private scope. It does not work!!!
class MyRegistrator extends KryoRegistrator with Logging  {

  def registerClasses (k: Kryo) {
    var registered = false
    try {
      k.register(classOf[RankingsClass])
      registered = true
    }
    catch {
      case e: Exception =>
        logError ("fails to register vis MyRegistrator", e)
    }

    if (registered) {
      logInfo("in test suite, successfully register class RankingsClass")
    }
  }
}


class NonJavaSerializableClass(val value: Int) extends Comparable[NonJavaSerializableClass] {
    override def compareTo(o: NonJavaSerializableClass): Int = {
      value - o.value
    }
}

object TestConstants {
  //this will need to be bigger than the maximum number of the test cases in this package
  val maxNumberOfTaskThreads = 150
  val GLOBAL_HEAP_NAME = "/dev/shm/nvm/global0"
  val SPARK_WORKER_CORES = "64"
  val SIZE_OF_BATCH_SERIALIZATION = 100
}
