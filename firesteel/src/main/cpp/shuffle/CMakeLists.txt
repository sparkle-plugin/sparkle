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
include_directories(${ALPS_ROOT}/include)
include_directories(${ALPS_ROOT}/third-party/yaml-cpp-0.5.2/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../shmmanagement)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/longext)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/strext)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/barrext)

## need to change which package's include, boost?
include_directories(/usr/local/include)

## need to find intel tbb include
IF (TBB_INCLUDE_DIR)
   include_directories(${TBB_INCLUDE_DIR})
ENDIF ()

set_property(GLOBAL APPEND PROPERTY ONLY_SHUFFLE_SRC
	${CMAKE_CURRENT_SOURCE_DIR}/ExtensibleByteBuffers.cc
	${CMAKE_CURRENT_SOURCE_DIR}/MapShuffleStoreManager.cc 
        ${CMAKE_CURRENT_SOURCE_DIR}/MergeSortChannelHelper.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/MapShuffleStoreWithIntKeys.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/ReduceShuffleStoreManager.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/GenericReduceChannel.cc 
        ${CMAKE_CURRENT_SOURCE_DIR}/MergeSortReduceChannelWithIntKeys.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/HashMapKeyPositionTracker.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/HashMapReduceChannelWithIntKeys.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/PassThroughReduceChannelWithIntKeys.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/ReduceShuffleStoreWithIntKeys.cc 
        ${CMAKE_CURRENT_SOURCE_DIR}/ShuffleDataSharedMemoryManagerHelper.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/longext/MapShuffleStoreWithLongKeys.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/longext/ReduceShuffleStoreWithLongKeys.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/longext/MergeSortReduceChannelWithLongKeys.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/longext/HashMapReduceChannelWithLongKeys.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/longext/PassThroughReduceChannelWithLongKeys.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/strext/MapShuffleStoreWithStringKeys.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/strext/MergeSortReduceChannelWithStringKeys.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/strext/ReduceShuffleStoreWithStringKeys.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/barrext/MapShuffleStoreWithByteArrayKeys.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/barrext/ReduceShuffleStoreWithByteArrayKeys.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/barrext/MergeSortReduceChannelWithByteArrayKeys.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/barrext/HashMapReduceChannelWithByteArrayKeys.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/barrext/PassThroughReduceChannelWithByteArrayKeys.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/ShuffleStoreManager.cc 
        ${CMAKE_CURRENT_SOURCE_DIR}/ByteBufferPool.cc 
        ${CMAKE_CURRENT_SOURCE_DIR}/ArrayBufferPool.cc
     
)

get_property(TMP_ONLY_SHUFFLE_SRC GLOBAL PROPERTY ONLY_SHUFFLE_SRC)
add_library(shuffle OBJECT ${TMP_ONLY_SHUFFLE_SRC})


##set_target_properties (shuffle PROPERTIES VERSION 1.0)

## default to be installed to /usr/local/lib
## install (TARGETS shm_management DESTINATION lib)

## later, to find automatically the following shared libraries.
## target_link_libraries(shuffle alps shm_management tcmalloc ${TBB_LIBRARY} glog rt)
