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

import org.apache.spark.unsafe.Platform

class UnsafeIntArray() extends UnsafeArray {

  def init(size: Int): Unit = {
    this.size = size
    memsize = size * 4 * count
    addr = Platform.allocateMemory(size * 4L)
  }
  override def setHeader(size:Int, count: Int): Unit = {
    this.size = size
    this.count = count
    memsize = size * 4 * count
  }
  def set (index: Int, value: Int): Unit = {
    Platform.putInt(null, addr + 4L * index, value)
  }
  def get (index: Int): Int = {
    Platform.getInt(null, addr + 4L * index)
  }
}
