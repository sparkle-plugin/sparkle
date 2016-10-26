#!/bin/bash
echo "to cleanup master log directory"
rm ./multicore/master/logs/*

{% for i in range(0, num_workers) %}
echo "to cleanup worker{{i}} log directory"
rm ./multicore/worker{{i}}/logs/*

{% endfor %}

