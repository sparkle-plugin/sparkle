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

(1) download the code from https://github.com/jerryshao/spark-terasort 
    to the sub-directory "spark-terasort"

(2) to compile the terasort code: 
    mvn -Dhadoop.version=2.4.0 clean package 


(3) make sure that the shared-memory has been created by ALPS tool, for example
    for terasort of 256 GB, we will need to choose the heap size of at least 400 GB
    (the overhead is due to internal block-size allocation algorithm of the Retail
     Memory Broker implementation), for example:  

    globalheap-util create /dev/shm/nvm/global0 --size=512G

    if the shared-memory has been created, issue the following command to format
    the region before proceeding to step 4:

    globalheap-util format /dev/shm/nvm/global0

(4) make sure that $SPARK_HOME/conf/spark-env.sh has the HADOOP_CONF_DIR set and hdfs-site.xml
    under $HADOOP_CONF_DIR is set to be with 1 GB block size. That is, the following parameters 
    need to be set in hdfs-site.xml:

      <configuration>
        <property>
          <name>dfs.block.size</name>
          <value>1073741824</value>
          <description>
          </description>
        </property>
        <property>
          <name>mapred.min.split.size</name>
          <value>1073741824</value>
          <description>
          </description>
        </property>
        <property>
          <name>mapred.max.split.size</name>
          <value>1073741824</value> 
          <description>
          </description>
        </property>
      </configuration>

(5) to generate 256 GB of TeraSort data:
    
     sudo ./teragen-launch.sh SPARK_HOME HOST_MACHINE

    where SPARK_HOME and HOST_MACHINE need to be provided as two input parameters.
    
    the output file is set to be /dev/shm/terasort_in

    
(6) to run terasort:

     sudo ./terasort-launch.sh  SPARK_HOME HOST_MACHINE

    the input file is set to be /dev/shm/terasort_in
    the output file is set to be /dev/shm/terasort_out

(7) to run tera validate:

     sudo ./teravalidate-launch.sh  SPARK_HOME HOST_MACHINE

    the input file is set to be /dev/shm/terasort_out
    the output validate file is set to be /dev/shm/terasort_validate

    at the console output, we should see the following summary:

      num records: 2560000000
      checksum: 4c4b0847c9cc516f

    the checksum is determinstic if we repeat the runs of: tera-generate, tera-sort
    and tera-validate, as long as we follow the same input parameters. 

