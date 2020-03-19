# * (c) Copyright 2016 Hewlett Packard Enterprise Development LP
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
#

cmake_minimum_required(VERSION 2.8)
project(firesteel)
set(FIRESTEEL_PROJECT_VERSION 0.2.0)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -march=native -Wall -O3")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3")

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

# third-party libraries
# set(GTEST_ROOT ${PROJECT_SOURCE_DIR}/third-party/gtest)
# add_subdirectory(third-party)

# alps library
find_package (alps REQUIRED)
set(ALPS_ROOT ${PROJECT_SOURCE_DIR}/../alps)

add_subdirectory(src/main/cpp)

## enable unit testing 
#enable_testing()
#add_subdirectory(src/test/cpp)


## packaging later
## 
