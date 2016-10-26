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

to build the package:

(1) sbt clean
(2) sbt package

(3) make sure that the shared-memory has been created by ALPS tool, for example:  
    globalheap-util create /dev/shm/nvm/global0 --size=128G

    if the shared-memory has been created, issue the following command to format
    the region before proceeding to step 4:

    globalheap-util format /dev/shm/nvm/global0
  

(4) to download an example data set called "soc-LiveJournal1.txt.gz"
    from https://snap.stanford.edu/data/soc-LiveJournal1.html.

(4) unzip the file with "gunzip", and in the unzipped text file, remove the first several lines
    that are the text comments started with "#".

(5) create the directory called "data" in tmpfs file system /dev/shm, and move the
    unzipped data to /dev/shm/data/soc-LiveJournal1.txt

(6) make sure in $SPARK_HOME/conf/hdfs-site.xml has the corresponding input file partition size 
    to be set accordingly, for example, the default size of 64 MB.

(7) to run: 

     ./soc-launch.sh  SPARK_HOME HOST_MACHINE

where SPARK_HOME and HOST_MACHINE need to be provided as two input parameters.

Note: the corresponding data file will have to be seperately provided 
