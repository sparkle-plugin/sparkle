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

include_directories(${PROJECT_SOURCE_DIR}/test/unit)

function (add_alps_test2 targetname srcname binname)
  add_executable(${targetname} ${CMAKE_CURRENT_SOURCE_DIR}/${srcname}.cc)
  set_target_properties(${targetname} PROPERTIES OUTPUT_NAME ${binname})
  target_link_libraries(${targetname} pthread gtest alps)
  target_link_libraries(${targetname} testcommon)
  add_test(NAME ${targetname} COMMAND ${CMAKE_CURRENT_BINARY_DIR}/${binname})
  install (TARGETS ${targetname} DESTINATION bin/test/unit COMPONENT test)
endfunction()

function (add_alps_test targetname)
  add_alps_test2(${targetname} ${targetname} ${targetname})
endfunction()


## Common functionality used by tests 
set(testcommon_src
	${CMAKE_CURRENT_SOURCE_DIR}/test_common.cc
)

add_library(testcommon STATIC ${testcommon_src})
target_link_libraries(testcommon boost_program_options)

add_subdirectory(common)
add_subdirectory(globalheap)
add_subdirectory(pegasus)
