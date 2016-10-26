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

# Find the alps library. That is, we will have to have ALPS to be built and then installed first.
# Output variables:
#  ALPS_LIBRARY     : Library path of alps library
#  ALPS_FOUND       : True if found.
#  the include path is assumed to be side-by-side at the repository level.

## FIND_PATH(ALPS_INCLUDE_DIR NAME alps
##   HINTS $ENV{HOME}/local/include /opt/local/include /usr/local/include /usr/include
## )

FIND_LIBRARY(ALPS_LIBRARY NAME alps
  HINTS $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib
)

IF (ALPS_LIBRARY)
    SET(ALPS_FOUND TRUE)
    MESSAGE(STATUS "Found alps library: lib=${ALPS_LIBRARY}")
ELSE ()
    SET(ALPS_FOUND FALSE)
    MESSAGE(STATUS "WARNING: alps library not found.")
    MESSAGE(STATUS "Try: to compile and install ALPS package first")
ENDIF ()
