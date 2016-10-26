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

package org.apache.spark.offheapstore.impl

import org.apache.spark.util.collection.BitSet
import org.apache.spark.unsafe.Platform

import scala.reflect.ClassTag

class UnsafeDoubleArray() extends UnsafeArray {

  def initKeyValues(size: Int, count: Int): Unit = {
    this.size = size
    this.count = count
    memsize = size * (8 + 8 * count)
    addr = Platform.allocateMemory(memsize)
  }
  def init(size: Int, count: Int): Unit = {
    this.size = size
    this.count = count
    memsize = size * 8 * count
    addr = Platform.allocateMemory(memsize)
  }

  override def setHeader(size:Int, count: Int): Unit = {
    this.size = size
    this.count = count
    memsize = size * 8 * count
  }

  def set (index: Int, elem_idx: Int, value: Double): Unit = {
    Platform.putDouble(null, addr + 8L * count * index  + 8L * (elem_idx-1) , value)
  }
  def setKey (index: Int, key: Long): Unit = {
    Platform.putLong(null, addr + (8L + 8L * count)* index, key)
  }
  def setValue (index: Int, elem_idx: Int, value: Double): Unit = {
    Platform.putDouble(null,
      addr + (8L + 8L * count)* index  + 8L + 8L * (elem_idx-1) , value)
  }

  def get (index: Int, elem_idx: Int): Double = {
    Platform.getDouble(null, addr + 8L * count * index + 8L * (elem_idx-1))
  }
  def getKey (index: Int): Long = {
    Platform.getLong(null, addr + (8L + 8L * count)* index)
  }
  def getValue (index: Int, elem_idx: Int): Double = {
    Platform.getDouble(null,
      addr + (8L + 8L * count)* index + 8L + 8L * (elem_idx-1))
  }

  def setZero(): Unit = {

    var i = 0
    while (i < size) {
      if (count == 2) {
        this.set(i, 1, 0.0)
        this.set(i, 2, 0.0)
      } else if (count == 3) {
        this.set(i, 1, 0.0)
        this.set(i, 2, 0.0)
        this.set(i, 3, 0.0)
      } else if (count == 4) {
        this.set(i, 1, 0.0)
        this.set(i, 2, 0.0)
        this.set(i, 3, 0.0)
        this.set(i, 4, 0.0)
      } else if (count == 5) {
        this.set(i, 1, 0.0)
        this.set(i, 2, 0.0)
        this.set(i, 3, 0.0)
        this.set(i, 4, 0.0)
        this.set(i, 5, 0.0)
      }
      i = i + 1
    }
  }

  def set[VD: ClassTag] (values: Array[VD], mask: BitSet): Unit = {


    var i = mask.nextSetBit(0)
    while (i >= 0) {
      if (count == 2) {
        val attr = values(i).asInstanceOf[(Double, Double)]
        this.set(i, 1, attr._1)
        this.set(i, 2, attr._2)
      } else if (count == 3) {
          val attr = values(i).asInstanceOf[(Double, Double, Double)]
          this.set(i, 1, attr._1)
          this.set(i, 2, attr._2)
          this.set(i, 3, attr._3)
      } else if (count == 4) {
        val attr = values(i).asInstanceOf[(Double, Double, Double, Double)]
        this.set(i, 1, attr._1)
        this.set(i, 2, attr._2)
        this.set(i, 3, attr._3)
        this.set(i, 4, attr._4)
      } else if (count == 5) {
        val attr = values(i).asInstanceOf[(Double, Double, Double, Double, Double)]
        //println(i + ":" + attr)
        this.set(i, 1, attr._1)
        this.set(i, 2, attr._2)
        this.set(i, 3, attr._3)
        this.set(i, 4, attr._4)
        this.set(i, 5, attr._5)
      }
      i = mask.nextSetBit(i + 1)
    }
  }

  def setKeyValues[VD: ClassTag] (keys: Array[Long], mask: BitSet, values: Array[VD]): Unit = {

    var i = mask.nextSetBit(0)
    var j = 0
    while (i >= 0) {
      this.setKey(j, keys(j))
      if (count == 2) {
        val attr = values(i).asInstanceOf[(Double, Double)]
        this.setValue(j, 1, attr._1)
        this.setValue(j, 2, attr._2)
      } else if (count == 3) {
        val attr = values(i).asInstanceOf[(Double, Double, Double)]
        this.setValue(j, 1, attr._1)
        this.setValue(j, 2, attr._2)
        this.setValue(j, 3, attr._3)
      } else if (count == 4) {
        val attr = values(i).asInstanceOf[(Double, Double, Double, Double)]
        this.setValue(j, 1, attr._1)
        this.setValue(j, 2, attr._2)
        this.setValue(j, 3, attr._3)
        this.setValue(j, 4, attr._4)
      } else if (count == 5) {
        val attr = values(i).asInstanceOf[(Double, Double, Double, Double, Double)]
        //println(i + ":" + attr)
        this.setValue(j, 1, attr._1)
        this.setValue(j, 2, attr._2)
        this.setValue(j, 3, attr._3)
        this.setValue(j, 4, attr._4)
        this.setValue(j, 5, attr._5)
      }
      i = mask.nextSetBit(i + 1)
      j += 1
    }
  }

  def setValues[VD: ClassTag] (j: Int, values: VD): Unit = {

      if (count == 2) {
        val attr = values.asInstanceOf[(Double, Double)]
        this.setValue(j, 1, attr._1)
        this.setValue(j, 2, attr._2)
      } else if (count == 3) {
        val attr = values.asInstanceOf[(Double, Double, Double)]
        this.setValue(j, 1, attr._1)
        this.setValue(j, 2, attr._2)
        this.setValue(j, 3, attr._3)
      } else if (count == 4) {
        val attr = values.asInstanceOf[(Double, Double, Double, Double)]
        this.setValue(j, 1, attr._1)
        this.setValue(j, 2, attr._2)
        this.setValue(j, 3, attr._3)
        this.setValue(j, 4, attr._4)
      } else if (count == 5) {
        val attr = values.asInstanceOf[(Double, Double, Double, Double, Double)]
        //println(i + ":" + attr)
        this.setValue(j, 1, attr._1)
        this.setValue(j, 2, attr._2)
        this.setValue(j, 3, attr._3)
        this.setValue(j, 4, attr._4)
        this.setValue(j, 5, attr._5)
      }
  }
  def set[VD: ClassTag] (i: Int, vd: VD): Unit = {

    if (count == 2) {
      val attr = vd.asInstanceOf[(Double, Double)]
      this.set(i, 1, attr._1)
      this.set(i, 2, attr._2)
    } else if (count == 3) {
      val attr = vd.asInstanceOf[(Double, Double, Double)]
      this.set(i, 1, attr._1)
      this.set(i, 2, attr._2)
      this.set(i, 3, attr._3)
    } else if (count == 4) {
      val attr = vd.asInstanceOf[(Double, Double, Double, Double)]
      this.set(i, 1, attr._1)
      this.set(i, 2, attr._2)
      this.set(i, 3, attr._3)
      this.set(i, 4, attr._4)
    } else if (count == 5) {
      val attr = vd.asInstanceOf[(Double, Double, Double, Double, Double)]
      this.set(i, 1, attr._1)
      this.set(i, 2, attr._2)
      this.set(i, 3, attr._3)
      this.set(i, 4, attr._4)
      this.set(i, 5, attr._5)
    } else if (count == 8) {
      val attr = vd.asInstanceOf[(Double, Double, Double, Double, Double, Double, Double, Double)]
      this.set(i, 1, attr._1)
      this.set(i, 2, attr._2)
      this.set(i, 3, attr._3)
      this.set(i, 4, attr._4)
      this.set(i, 5, attr._5)
      this.set(i, 6, attr._6)
      this.set(i, 7, attr._7)
      this.set(i, 8, attr._8)
    }
  }
  def setKeyValue[VD: ClassTag] (i: Int, key: Long, vd: VD): Unit = {

    if (count == 2) {
      this.setKey(i, key)
      val attr = vd.asInstanceOf[(Double, Double)]
      this.setValue(i, 1, attr._1)
      this.setValue(i, 2, attr._2)
    } else if (count == 3) {
      this.setKey(i, key)
      val attr = vd.asInstanceOf[(Double, Double, Double)]
      this.setValue(i, 1, attr._1)
      this.setValue(i, 2, attr._2)
      this.setValue(i, 3, attr._3)
    } else if (count == 4) {
      this.setKey(i, key)
      val attr = vd.asInstanceOf[(Double, Double, Double, Double)]
      this.setValue(i, 1, attr._1)
      this.setValue(i, 2, attr._2)
      this.setValue(i, 3, attr._3)
      this.setValue(i, 4, attr._4)
    } else if (count == 5) {
      this.setKey(i, key)
      val attr = vd.asInstanceOf[(Double, Double, Double, Double, Double)]
      this.setValue(i, 1, attr._1)
      this.setValue(i, 2, attr._2)
      this.setValue(i, 3, attr._3)
      this.setValue(i, 4, attr._4)
      this.setValue(i, 5, attr._5)
    } else if (count == 8) {
      this.setKey(i, key)
      val attr = vd.asInstanceOf[(Double, Double, Double, Double, Double, Double, Double, Double)]
      this.setValue(i, 1, attr._1)
      this.setValue(i, 2, attr._2)
      this.setValue(i, 3, attr._3)
      this.setValue(i, 4, attr._4)
      this.setValue(i, 5, attr._5)
      this.setValue(i, 6, attr._6)
      this.setValue(i, 7, attr._7)
      this.setValue(i, 8, attr._8)
    }
  }

  def get[VD: ClassTag] (i: Int): VD = {

    if(count == 2) {
      val attr: (Double, Double) =
        (this.get(i, 1), this.get(i, 2))
      attr.asInstanceOf[VD]
    } else if(count == 3) {
      val attr: (Double, Double, Double) =
        (this.get(i, 1), this.get(i, 2), this.get(i, 3))
      attr.asInstanceOf[VD]
    } else if(count == 4) {
      val attr: (Double, Double, Double, Double) =
        (this.get(i, 1), this.get(i, 2), this.get(i, 3), this.get(i, 4))
      attr.asInstanceOf[VD]
    } else if(count == 5){
      val attr: (Double, Double, Double, Double, Double) =
        (this.get(i, 1), this.get(i, 2), this.get(i, 3), this.get(i, 4), this.get(i, 5))
      attr.asInstanceOf[VD]
    } else if(count == 8){
      val attr: (Double, Double, Double, Double, Double, Double, Double, Double) =
        (this.get(i, 1), this.get(i, 2), this.get(i, 3), this.get(i, 4), this.get(i, 5), this.get(i, 6), this.get(i, 7), this.get(i, 8))
      attr.asInstanceOf[VD]
    } else {
        val attr: (Double, Double) =
          (this.get(i, 1), this.get(i, 2))
        attr.asInstanceOf[VD]
    }
  }

  def getValue[VD: ClassTag] (i: Int): VD = {

    if(count == 2) {
      val attr: (Double, Double) =
        (this.getValue(i, 1), this.getValue(i, 2))
      attr.asInstanceOf[VD]
    } else if(count == 3) {
      val attr: (Double, Double, Double) =
        (this.getValue(i, 1), this.getValue(i, 2), this.getValue(i, 3))
      attr.asInstanceOf[VD]
    } else if(count == 4) {
      val attr: (Double, Double, Double, Double) =
        (this.getValue(i, 1), this.getValue(i, 2), this.getValue(i, 3), this.getValue(i, 4))
      attr.asInstanceOf[VD]
    } else if(count == 5){
      val attr: (Double, Double, Double, Double, Double) =
        (this.getValue(i, 1), this.getValue(i, 2), this.getValue(i, 3), this.getValue(i, 4), this.getValue(i, 5))
      attr.asInstanceOf[VD]
    } else if(count == 8){
      val attr: (Double, Double, Double, Double, Double, Double, Double, Double) =
        (this.getValue(i, 1), this.getValue(i, 2), this.getValue(i, 3), this.getValue(i, 4), this.getValue(i, 5), this.getValue(i, 6), this.getValue(i, 7), this.getValue(i, 8))
      attr.asInstanceOf[VD]
    } else {
      val attr: (Double, Double) =
        (this.getValue(i, 1), this.getValue(i, 2))
      attr.asInstanceOf[VD]
    }
  }
}
