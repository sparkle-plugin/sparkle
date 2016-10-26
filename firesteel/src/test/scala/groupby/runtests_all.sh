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

#!/bin/bash
# Script that executes test on shared-memory shuffle across Java, JNI and then C++ shuffle engine
 
# parameters of LSH Code
export LD_LIBRARY_PATH=/usr/local/lib:/opt/intel/tbb/lib 
##GLOG_V works!
export GLOG_minloglevel=1
export GLOG_v=0
##export GLOG_vmodule=rmb=3

#for sprof
#export LD_PROFILE_OUTPUT=/tmp
#export LD_PROFILE=libjnishmshuffle.so
# we need to remove the profile that is  from the last compiled shared library
#rm -f $LD_PROFILE_OUTPUT/$LD_PROFILE.profile
##export GLOG_log_dir=/tmp/shmlog
echo "test suite under test is:  $1"
mvn test 




