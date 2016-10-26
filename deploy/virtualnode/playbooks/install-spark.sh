#!/bin/bash
# Create pseudo-multi-core SPARK installation

set -e

spark_home=$1

if [ ! -d "$spark_home" ]
then
	echo "Usage: $0 [spark_home_dir]" 1>&2
	exit 1
fi

invoke_dir=$( cd $( dirname $0 ) ; pwd )

if [ -z "$JAVA_HOME" ] ; then
	echo "Variable JAVA_HOME not set"; exit 1
fi
if [ ! -d "$JAVA_HOME" -o ! -x "$JAVA_HOME/bin/java" ] ; then
	echo "Variable JAVA_HOME not correctly set"; exit 1
fi

PARMS="--extra-vars '{\"java_home\": \"${JAVA_HOME}\"}'"

# Read variables from variables.yml
WORK_DIR=$(
	ansible localhost -m include_vars -a 'variables.yml' 2>/dev/null \
	| awk '/"work_dir":/ {print $2}' \
	| sed -e 's/"//g' -e 's/,$//'
)

INSTALL_ROOT=$(
	ansible localhost -m include_vars -a 'variables.yml' 2>/dev/null \
	| awk '/"install_root":/ {print $2}' \
	| sed -e 's/"//g' -e 's/,$//'
)

NUM_WORKERS=$(
	ansible localhost -m include_vars -a 'variables.yml' 2>/dev/null \
	| awk '/"num_workers":/ {print $2}' \
	| sed -e 's/"//g' -e 's/,$//'
)

echo "Welcome to the Spark-HPC installer"
echo
echo "Spark home: $spark_home" 1>&2
echo "install root: <$INSTALL_ROOT> work dir: <$WORK_DIR>" 1>&2

cd $invoke_dir

for dir in "$WORK_DIR" "$INSTALL_ROOT" 
do
	if [ -d "$dir" ]
	then
		echo "Already exists: $dir -- Uninstall first." 1>&2
		exit 1
	fi
done

echo "PRESS ENTER TO INSTALL" ; read SOMEVAR

set -e

sudo rm -f /tmp/spark-hpc-install-worker-*.err
sudo rm -f /tmp/spark-hpc-install-worker-*.out

extra_vars="{\"spark_home_dir\": \"${spark_home}\", \"java_home\": \"${JAVA_HOME}\"}"

ansible-playbook -b --extra-vars "$extra_vars" -i inventory.yml root-dir.yml
ansible-playbook -b --extra-vars "$extra_vars" -i inventory.yml multi-core.yml

# Check if number of CPU's per worker is balanced, or
# if unassigned CPU's remain after partitioning.
echo "Number of workers: $NUM_WORKERS"
../scripts/workers-per-socket.pl --balance $NUM_WORKERS

#echo "Don't forget to copy:"
#echo "  * the Firesteel JAR under $INSTALL_ROOT/lib"
#echo "  * the GroupBy and PageRank JAR files under $INSTALL_ROOT/lib"

