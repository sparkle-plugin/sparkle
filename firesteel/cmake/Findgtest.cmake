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

# Find the gtest library.
# Output variables:
#  GTEST_INCLUDE_DIR : e.g., /usr/local/include/.
#  GTEST_LIBRARY     : Library path of gtest library
#  GTEST_FOUND       : True if found.

# for GTEST, it is up to the directory
FIND_PATH(GTEST_INCLUDE_DIR NAME gtest
  HINTS $ENV{HOME}/local/include /opt/local/include /usr/local/include /usr/include)

FIND_LIBRARY(GTEST_LIBRARY NAME gtest
  HINTS $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib
)

IF (GTEST_INCLUDE_DIR AND GTEST_LIBRARY)
    SET(GTEST_FOUND TRUE)
    MESSAGE(STATUS "Found gtest library: inc=${GTEST_INCLUDE_DIR}, lib=${GTEST_LIBRARY}")
ELSE ()
    SET(GTEST_FOUND FALSE)
    MESSAGE(STATUS "WARNING: Gtest library not found.")
    MESSAGE(STATUS "Try: 'sudo apt-get install gtest/gtest-devel' (or sudo yum install gtest/gtest-devel)")
ENDIF ()
