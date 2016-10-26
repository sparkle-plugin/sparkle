#!/bin/bash
# Set up ALPS library: create global shared memory region.

set -e

ansible-playbook -b -i inventory.yml alps-config.yml 

