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

#ifndef MAPSHUFFLESTOREMANAGER_H_
#define MAPSHUFFLESTOREMANAGER_H_

#include "EnumKvTypes.h"
#include "GenericMapShuffleStore.h"

//NOTE: to remove cyclic, comment out these include files.
//#include "MapShuffleStoreWithIntKeys.h"
//#include "MapShuffleStoreWithStringKeys.h"

using namespace std;

//NOTE: this now become a factory class. And all of the map store created are managed in
//Java, thus avoid using the concurrent hash map in C++.
class MapShuffleStoreManager {
 public: 
    
    MapShuffleStoreManager() {
       //
    }

    //Item 7: declare a virtual destructor if and only if the class contains at least one virtual function
    ~MapShuffleStoreManager() {
       //
    }

    //create a shuffle store with shuffle id, map id and the specified kvalue type.
    //ordering specify whether we need to have the map side key sorting or not.
    GenericMapShuffleStore  *createStore(int shuffleId, int id, enum KValueTypeId tid, bool ordering);

    //only release the transient DRAM related memory resources
    void stopShuffleStore (GenericMapShuffleStore *store); 

    //release the NVM related memory resources 
    void shutdownShuffleStore (GenericMapShuffleStore *store); 

    //initialize this MapShufflstore 
    void initialize ();

    //shutdown  itself
    void shutdown ();

};


#endif 
