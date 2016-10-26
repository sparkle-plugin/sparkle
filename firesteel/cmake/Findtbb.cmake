# 
# (c) Copyright 2016 Hewlett Packard Enterprise Development LP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Find the tbb library.
# Output variables:
#  TBB_INCLUDE_DIR : e.g., /usr/include/.
#  TBB_LIBRARY     : Library path of tbb library
#  TBB_FOUND       : True if found.

# for TBB, it is up to the directory
FIND_PATH(TBB_INCLUDE_DIR NAME tbb
  HINTS $ENV{HOME}/local/include /opt/local/include /usr/local/include /usr/include /opt/concurrent/intel/tbb/include)

FIND_LIBRARY(TBB_LIBRARY NAME tbb
  HINTS $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib /opt/concurrent/intel/tbb/lib
)

IF (TBB_INCLUDE_DIR AND TBB_LIBRARY)
    SET(TBB_FOUND TRUE)
    MESSAGE(STATUS "Found tbb library: inc=${TBB_INCLUDE_DIR}, lib=${TBB_LIBRARY}")
ELSE ()
    SET(TBB_FOUND FALSE)
    MESSAGE(STATUS "WARNING: Tbb library not found.")
    MESSAGE(STATUS "Try: 'sudo apt-get install libtbb' (or sudo yum install tbb)")
ENDIF ()
