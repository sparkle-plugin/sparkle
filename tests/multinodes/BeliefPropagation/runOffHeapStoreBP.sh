#!/bin/sh

if [ "$1" = "" ]
then
  echo 'Usage : ./runOffHeapStoreBP.sh <SPARK_HOME> <HOST_MACHNE>' 
  exit
fi
if [ "$2" = "" ]
then
  echo 'Usage : ./runOffHeapStoreBP.sh <SPARK_HOME> <HOST_MACHNE>' 
  exit
fi

export NODES=./offheapstore-bp/data/vertex100.txt #vertex input file
export EDGES=./offheapstore-bp/data/edge100.txt #edge input file

export JAR=./offheapstore-bp/target/offheapstore-2.0.0-SNAPSHOT-jar-with-dependencies.jar

#Usage:  SPARK_HOME/bin/spark-submit --class "org.apache.spark.offheapstore.examples.bp.RunBP"  --master <master> <jar file> <vertex input file> <edge input file> <maximum iteration no> <vertex partition no> <edge partition no (optional)>
$1/bin/spark-submit --class "org.apache.spark.offheapstore.examples.bp.RunBP"  --master spark://$2:7077 $JAR $NODES $EDGES 50 2
