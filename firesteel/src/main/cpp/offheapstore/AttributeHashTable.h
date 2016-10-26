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

#ifndef ATTRIBUTEHASHTABLE_H_
#define ATTRIBUTEHASHTABLE_H_
#include <unordered_map>
#include <glog/logging.h>

using namespace std;

class AttributeHashTable {
private:
	unordered_map < long, long >  hashTable;  


	// two tables
	int* arr1stTable;
	long* arr2ndTable;
	size_t bucket_cnt;
	size_t arr2ndSize;

public:
       AttributeHashTable();
       virtual ~AttributeHashTable();

       long getArr1stTable() {
	   return reinterpret_cast<long>(arr1stTable);
       } 

       long getArr2ndTable() {
	   return reinterpret_cast<long>(arr2ndTable);
       } 
       void setAttrTable(int* addr1, long* addr2) {
	   arr1stTable = addr1;
	   arr2ndTable = addr2;
       }

       void setBucketCnt(int cnt) {
	   bucket_cnt = cnt;
       }

       size_t getBucketCnt() {
	  return bucket_cnt;
       }
       size_t getArr2ndSize() {
	 return arr2ndSize;
       } 
       
       long get(long key) {

	   unordered_map < long, long >::hasher fn = hashTable.hash_function();
    	   size_t k = key % bucket_cnt;  // generate key
    	   int pos = arr1stTable[k];
		
	   // key is not found
    	   if(pos == -1) {
		return 0;
    	   }
	
    	   while(arr2ndTable[pos] != -1) {  // while it is not the ending 
		if(arr2ndTable[pos] == key) {
			return arr2ndTable[pos+1]; 
		}
		pos += 2;
	   }

	   return 0;
       	}

       	void put(long key, long addr) {
		   	hashTable[key] = addr;
       	}

       	void create2tables(int regionId);

};

#endif /* ATTRIBUTEHASHTABLE_H_ */

