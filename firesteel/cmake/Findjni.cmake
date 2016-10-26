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

# Find the JNI header files for include.
# the way to find is to rely on the shell environment variable: JAVA_HOME
# Output variables:
#  JNI_INCLUDE_DIR : one general include, and one for linux.
#  JNI_INCLUDE_FOUND       : True if found.

SET(SYSTEM_PLATFORM "")

MESSAGE(STATUS "platform system identified: ${CMAKE_SYSTEM_NAME}")

## only support linux platform at this time
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
   SET(SYSTEM_PLATFORM linux)
   MESSAGE(STATUS "platform system innerloop identified: ${SYSTEM_PLATFORM}")
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

# for the general include
FIND_PATH(JNI_GENERAL_INCLUDE_DIR NAME jni.h
  HINTS $ENV{JAVA_HOME}/include)

# for the platform-specific include
FIND_PATH(JNI_PLATFORM_SPECIFIC_INCLUDE_DIR NAME jni_md.h
  HINTS $ENV{JAVA_HOME}/include/${SYSTEM_PLATFORM}
)

IF (JNI_GENERAL_INCLUDE_DIR AND JNI_PLATFORM_SPECIFIC_INCLUDE_DIR)
    SET(JNI_INCLUDE_FOUND TRUE)
    MESSAGE(STATUS "Found jni include path: inc=${JNI_GENERAL_INCLUDE_DIR}, inc=${JNI_PLATFORM_SPECIFIC_INCLUDE_DIR}")
ELSE ()
    SET(JNI_INCLUDE_FOUND FALSE)
    MESSAGE(STATUS "WARNING: try to find jni include paths, not found.")
    MESSAGE(STATUS "Try: to export JAVA_HOME as shell environment variable")
ENDIF ()
