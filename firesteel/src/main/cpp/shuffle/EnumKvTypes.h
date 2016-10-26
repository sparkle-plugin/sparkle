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

#ifndef ENUM_KV_TYPES_H_
#define ENUM_KV_TYPES_H_

#include <stdlib.h>

//enum, corresonding to ShuffleDataModel defined in java
static const size_t TOTAL_PREDEFINED_KVALUETYPE_IDS=8;

enum KValueTypeId {
  Int = 0,
  Long = 1,
  Float = 2,
  Double = 3,
  String = 4,
  ByteArray = 5,
  Object = 6,
  Unknown= 7
};

//to allow object-based Key type definition 
struct KValueTypeDefinition {
  enum KValueTypeId  typeId; 

  int length; //the length of the definition 
  //ultimate owner of the definition will need to free it
  //at the end of the lifetime.
  unsigned char *definition;
 
  KValueTypeDefinition(): 
      typeId(KValueTypeId::Unknown),length(0), definition(nullptr) {
  
  }

  KValueTypeDefinition(enum KValueTypeId tid): 
      typeId(tid), length(0), definition(nullptr) {
  
  }
       
};

//to allow object-based Value type definition 
struct VValueTypeDefinition {
  int length; //the length of the definition
  //ultimate owner of the definition will need to free it
  //at the end of the lifetime.
  unsigned char *definition;

  VValueTypeDefinition(): length(0), definition(nullptr) {
  }
};



#endif /*ENUM_KV_TYPES_H_*/
