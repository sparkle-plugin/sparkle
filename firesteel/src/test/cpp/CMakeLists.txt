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

## gtest is located in alps
set(SHUFFLE_ROOT ${PROJECT_SOURCE_DIR}/src/main/cpp/shuffle)
set(SHM_MANAGEMENT_ROOT ${PROJECT_SOURCE_DIR}/src/main/cpp/shmmanagement)

## need to find intel tbb include
IF (TBB_INCLUDE_DIR)
   include_directories(${TBB_INCLUDE_DIR})
ENDIF ()

find_package (gtest REQUIRED) 

include_directories(${GTEST_INCLUDE_DIR})
include_directories(${ALPS_ROOT}/src)
include_directories(${ALPS_ROOT}/include)
include_directories(${SHUFFLE_ROOT})
include_directories(${SHM_MANAGEMENT_ROOT})
include_directories(${SHM_MANAGEMENT_ROOT})

function (add_firesteel_shuffletest2 targetname srcname binname)
  add_executable(${targetname} ${CMAKE_CURRENT_SOURCE_DIR}/${srcname}.cc $<TARGET_OBJECTS:shuffle>)
  set_target_properties(${targetname} PROPERTIES OUTPUT_NAME ${binname})
  target_link_libraries(${targetname} ${GTEST_LIBRARY})
  add_test(NAME ${targetname} COMMAND ${CMAKE_CURRENT_BINARY_DIR}/${binname})
  install (TARGETS ${targetname} DESTINATION bin/test/unit COMPONENT test)
endfunction()

add_subdirectory(shuffle)
