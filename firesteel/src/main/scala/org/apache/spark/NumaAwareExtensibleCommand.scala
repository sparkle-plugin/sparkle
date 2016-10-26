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
 */

package org.apache.spark

import org.apache.spark.util.ExtensibleCommand

/**
  * This is an example to expand executor launch command with numactl, user should configure
  * several numazone through configurations, like:
  *
  * spark.executorEnv.SPARK_MEMNODE_0_AFFINITY 0
  * spark.executorEnv.SPARK_CORE_0_AFFINITY 0,1,2,3
  *
  * spark.executorEnv.SPARK_MEMNODE_1_AFFINITY 1
  * spark.executorEnv.SPARK_CORE_1_AFFINITY 4,5,6,7
  *
  * spark.executorEnv.SPARK_MEMNODE_2_AFFINITY 2
  * spark.executorEnv.SPARK_CORE_2_AFFINITY 8,9,10,11
  *
  * spark.executorEnv.SPARK_MEMNODE_3_AFFINITY 3
  * spark.executorEnv.SPARK_CORE_3_AFFINITY 12,13,14,15
  *
  * This [[NumaAwareExtensibleCommand()]] will bind executor with different zone through
  * round-robin mechanism. If we have 4 zones and 6 executor, executor 0, 1, 2, 3 will be in
  * different zones, executor 4 will be in zone 0, executor 5 will be in zone 1.
  */
class NumaAwareExtensibleCommand extends ExtensibleCommand {

  private val sparkConf = new SparkConf()

  private var currAffinityNum = 0

  private val numactlCmd = "numactl --membind=%s --physcpubind=%s"

  private val MEM_BIND_CONF = "spark.executorEnv.SPARK_MEMNODE_%d_AFFINITY"
  private val CPU_BIND_CONF = "spark.executorEnv.SPARK_CORE_%d_AFFINITY"

  override def extendCommand(command: Seq[String], executorId: String): Seq[String] = synchronized {

    val effectiveMemAffinity = sparkConf.getOption(MEM_BIND_CONF.format(currAffinityNum))
      .getOrElse(throw new IllegalStateException("Failed to find out memory affinity " +
        s"configuration for executor $executorId"))

    val effectiveCpuAffinity = sparkConf.getOption(CPU_BIND_CONF.format(currAffinityNum))
      .getOrElse(throw new IllegalStateException("Failed to find out cpu affinity " +
        s"configuration for executor $executorId"))

    currAffinityNum = {
      val affinityNum = currAffinityNum + 1
      if (sparkConf.contains(MEM_BIND_CONF.format(affinityNum))) {
        affinityNum
      } else {
        0
      }
    }

    val effectiveNumactlCmd = numactlCmd.format(effectiveMemAffinity, effectiveCpuAffinity)
      .split(" ")

    effectiveNumactlCmd ++ command
  }
}
