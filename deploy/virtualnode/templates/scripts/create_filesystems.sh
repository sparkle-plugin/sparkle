#!/bin/bash
set -e

#for master node 
sudo mkdir -p {{work_dir}}/store-m0
df {{work_dir}}/store-m0 | grep {{work_dir}}/store-m0 > /dev/null && sudo umount {{work_dir}}/store-m0
sudo mkdir -p {{work_dir}}/store-m0
sudo mount -t tmpfs -o noatime,size={{master_tmpfs_size}},mpol=bind:0 tmpfs {{work_dir}}/store-m0
mkdir -p {{work_dir}}/store-m0/eventLog
mkdir -p {{work_dir}}/store-m0/local

{% for i in range(0, num_workers) %}
sudo mkdir -p {{work_dir}}/store-w{{i}}
df {{work_dir}}/store-w{{i}} | grep {{work_dir}}/store-w{{i}} > /dev/null && sudo umount {{work_dir}}/store-w{{i}}
sudo mkdir -p {{work_dir}}/store-w{{i}}
sudo mount -t tmpfs -o noatime,size={{worker_tmpfs_size}},mpol=bind:0 tmpfs {{work_dir}}/store-w{{i}}
mkdir -p {{work_dir}}/store-w{{i}}/eventLog
mkdir -p {{work_dir}}/store-w{{i}}/local
mkdir -p {{work_dir}}/store-w{{i}}/worker

{% endfor %}
