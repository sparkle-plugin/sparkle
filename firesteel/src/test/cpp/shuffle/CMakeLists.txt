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

## gtest is located under third-party external component
function (add_shuffle_test targetname)
  add_firesteel_shuffletest2(${targetname} ${targetname} ${targetname})
endfunction()

function (add_common_test targetname)
  add_shuffle_test(${targetname})
  target_link_libraries(${targetname} shm_management ${ALPS_LIBRARY} ${TBB_LIBRARY} boost_system boost_filesystem glog rt pthread)
endfunction()

add_common_test(bytebufferpool_unitest)
add_common_test(extensiblebytebuffers_unittest)
add_common_test(mapshufflestoremanager_unittest)
add_common_test(shufflestoremanager_unittest)
add_common_test(multipartitionedheap_unititest)

