# * (c) Copyright 2016 Hewlett Packard Enterprise Development LP
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
#

1. build edgepartition and install it in the local mvn repository for a dependency for offheapstore-bp
(1) cd edgepartition-2.0.0
(2) mvn install  

2. build offheapstore-bp 
(1) make sure firesteel-2.0.0-SNAPSHOT.jar is installed in the local .m2 repository (See Section 5 in "Compile and Run Spark Applications with Spark HPC Package on a Multicore Big-Memory Machine")
(2) cd offheapstore-bp 
(3) mvn package 

3. make sure that the shared-memory has been created by ALPS tool, for example:  
    globalheap-util create /dev/shm/nvm/global0 --size=128G

  if the shared-memory has been created, issue the following command to format
    the region before proceeding to step 5.

    globalheap-util format /dev/shm/nvm/global0

4. make sure the global heap file name is specified for "spark.offheapstore.globalheap.name" in Spark configuration
file spark-defaults.conf located under SPARK_HOME/conf.

(when not specified, in default, /dev/shm/nvm/global0 will be used)

spark.offheapstore.globalheap.name <global heap file name (should be the full directory)> 

e.g., --conf spark.offheapstore.globalheap.name=/dev/shm/nvm/global0

5. to run BP for test data (./offheapstore-bp/data): 

  ./runOffHeapStoreBP.sh <SPARK_HOME> <HOST_MACHNE>

where SPARK_HOME and HOST_MACHINE need to be provided as two input parameters.
