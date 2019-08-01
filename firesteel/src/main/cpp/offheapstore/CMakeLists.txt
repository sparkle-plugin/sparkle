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

find_package (tbb REQUIRED)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${ALPS_ROOT}/src)
include_directories(${ALPS_ROOT}/include/alps)
include_directories(${ALPS_ROOT}/third-party/yaml-cpp-0.5.2/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../shmmanagement)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../shuffle)

## need to change which package's include, boost?
include_directories(/usr/local/include)

## need to find intel tbb include
IF (TBB_INCLUDE_DIR)
   include_directories(${TBB_INCLUDE_DIR})
ENDIF ()

set_property(GLOBAL APPEND PROPERTY ONLY_OFFHEAP_STORE_SRC
	${CMAKE_CURRENT_SOURCE_DIR}/AttributeHashTable.cc
	${CMAKE_CURRENT_SOURCE_DIR}/OffHeapStoreManager.cc 
)

get_property(TMP_ONLY_OFFHEAPSTORE_SRC GLOBAL PROPERTY ONLY_OFFHEAP_STORE_SRC)
add_library(offheapstore OBJECT ${TMP_ONLY_OFFHEAPSTORE_SRC})


##set_target_properties (offheapstore PROPERTIES VERSION 1.0)

## default to be installed to /usr/local/lib
## install (TARGETS shm_management DESTINATION lib)

## later, to find automatically the following shared libraries.
## target_link_libraries(shuffle alps shm_management tcmalloc ${TBB_LIBRARY} glog rt)
