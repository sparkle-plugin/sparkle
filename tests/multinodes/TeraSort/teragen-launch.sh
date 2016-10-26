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

# first input parameter: SPARK_HOME 
# second input parameter: HOST_MACHINE
export SPARK_HOME=$1
echo "SPARK_HOME is chosen to be: $1"

export HOST_MACHINE=$2
echo "HOST_MACHINE (fully qualified path) is chosen to be: $2"

## the JAR file 
export JAR=./spark-terasort/target/spark-terasort-1.0-jar-with-dependencies.jar
 
## the output generate file 
export OUTPUT_FILE=/dev/shm/terasort_in

if [ -d "$OUTPUT_FILE" ]
then
    echo "$OUTPUT_FILE found."
    rm -f -r $OUTPUT_FILE
else
    echo "$OUTPUT_FILE does not exist."
fi

$SPARK_HOME/bin/spark-submit --class "com.github.ehiggs.spark.terasort.TeraGen" --master spark://$HOST_MACHINE:7077 $JAR 256G 1280 $OUTPUT_FILE
