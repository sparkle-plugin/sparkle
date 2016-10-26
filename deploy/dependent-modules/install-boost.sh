#!/bin/bash
# Install boost

set -e

WORK=/tmp/boost

boost_tarball="/tmp/boost_1_56_0.tar.gz"
boost_url="https://sourceforge.net/projects/boost/files/boost/1.56.0/boost_1_56_0.tar.gz"

[ -d "$WORK" ] && rm -fr /tmp/boost

mkdir -p "$WORK"

# Check if the Boost tarball is a valid tar archive
if [ -f "$boost_tarball" ]
then
	if tar ztf "$boost_tarball" > /dev/null
	then
		:
	else
		rm "$boost_tarball"
	fi
fi

[ -f "$boost_tarball" ] || wget -O ${boost_tarball} --no-check-certificate ${boost_url}

cd $WORK

tar -zxf ${boost_tarball} # creates dir boost_1_56_0
cd boost_1_56_0

./bootstrap.sh

./b2 cxxflags='-I/usr/include/python3.4m'

./b2 install

