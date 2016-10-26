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

import org.apache.spark.Aggregator
import org.apache.spark.TaskContext

object ShmShuffleWithMultiValues {
  implicit def toAggregatorWithMultiValues[K, V, C] (aggregator: Aggregator[K, V, C]):
             AggregatorWithMultiValues[K, V, C] = 
     new AggregatorWithMultiValues[K, V, C] (aggregator:  Aggregator[K, V, C])

  case class AggregatorWithMultiValues[K, V, C] (
       aggregator: Aggregator[K, V, C]) {

    def combineMultiValuesByKey(iter: Iterator[_ <: Product2[K, Seq[V]]],
                         context: TaskContext): Iterator[(K, C)] = {


      def iterator: Iterator[(K, C)] = new Iterator[(K, C)] {

        override def hasNext: Boolean = iter.hasNext

        override def next(): (K, C) = {
          if (!hasNext) {
            throw new NoSuchElementException
          }

          val kMultiValues = iter.next()
          val kvalue = kMultiValues._1
          val multipleValues = kMultiValues._2.iterator.buffered
          //first value
          var combinedValues = aggregator.createCombiner(multipleValues.head)
          //comsume the head value, to advance to the next one.
          multipleValues.next()
          while (multipleValues.hasNext) {
            val nextValue = multipleValues.next();
            combinedValues = aggregator.mergeValue(combinedValues, nextValue)
          }
 
          (kvalue, combinedValues)
        }
      }

      iterator
    }
  }


}