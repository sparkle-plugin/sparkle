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

# Find the holodeck distaf library.
# Output variables:
#  DISTAF_INCLUDE_DIR : e.g., /usr/include/.
#  DISTAF_LIBRARY     : Library path of distaf library
#  DISTAF_FOUND       : True if found.

find_path(DISTAF_INCLUDE_DIR NAME distaf.h
  HINTS $ENV{HOME}/local/include /opt/local/include /usr/local/include /usr/include ${HOLODECK_DIR}/distaf/include
)

find_library(DISTAF_LIBRARY NAME distaf
  HINTS $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib ${HOLODECK_DIR}/build/distaf/src
)

if (DISTAF_INCLUDE_DIR AND DISTAF_LIBRARY)
    set(DISTAF_FOUND TRUE)
    message(STATUS "Found distaf library: inc=${DISTAF_INCLUDE_DIR}, lib=${DISTAF_LIBRARY}")
else ()
    set(NUMA_FOUND FALSE)
    message(STATUS "WARNING: distaf library not found.")
endif ()
