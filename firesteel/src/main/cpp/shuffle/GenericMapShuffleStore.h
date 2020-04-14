/*
 * (c) Copyright 2016 Hewlett Packard Enterprise Development LP
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#ifndef GENERIC_MAP_SHUFFLE_STORE_H_
#define GENERIC_MAP_SHUFFLE_STORE_H_

#include "EnumKvTypes.h"

//define a generic map shuffle store to encapsue different map shuffle store manager that has different keys:
//int, float, double, byte-array and arbitrary objects.
class GenericMapShuffleStore {
 public: 
    //to retrieve the type id
    virtual  KValueTypeDefinition getKValueType() =0;
    //to retrieve the value definition
    virtual  VValueTypeDefinition getVValueType() = 0;

    //to specify whether key ordering is required.
    virtual  bool needsOrdering() = 0;

    //for DRAM related resources
    virtual  void stop () =0;
    //for NVRAM related resources
    virtual  void shutdown () =0;
 
    virtual ~GenericMapShuffleStore() {
      //do nothing
    }

    int typeId {-1};
    int shuffleId {-1};
};


#endif  /*GENERIC_MAP_SHUFFLE_STORE_H_*/
