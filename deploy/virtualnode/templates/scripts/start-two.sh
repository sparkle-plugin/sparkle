#!/bin/bash
echo "start master...."
./multicore/master/sbin/start-master.sh

sleep 10s 

echo "start worker0...."
export SPARK_IDENT_STRING=worker0
./multicore/worker0/sbin/start-slave.sh spark://{{ansible_hostname}}:7077

echo "start worker1...."
export SPARK_IDENT_STRING=worker1
./multicore/worker1/sbin/start-slave.sh spark://{{ansible_hostname}}:7077



