#!/bin/bash
set -e

echo "to umount {{work_dir}}/store-m0...."
sudo umount -f {{work_dir}}/store-m0

{% for i in range(0, num_workers) %}
echo "to umount {{work_dir}}/store-w{{i}}...."
sudo umount -f {{work_dir}}/store-w{{i}}

{% endfor %}

echo "please check whether the tempfs file systems have been unmounted successfully...."
sudo df -h

