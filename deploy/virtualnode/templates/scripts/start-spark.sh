#!/bin/bash
echo "start master...."
./multicore/master/sbin/start-master.sh

sleep 10s 

{% for i in range(0, num_workers) %}
echo "start worker{{i}}...."
export SPARK_IDENT_STRING=worker{{i}}
./multicore/worker{{i}}/sbin/start-slave.sh spark://{{ansible_hostname}}:7077

{% endfor %}


