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

#ifndef _SIMPLE_UTILS_H_
#define _SIMPLE_UTILS_H_

#include  "ExtensibleByteBuffers.h"

#include <random>
#include <functional>
#include <string.h>
#include <string.h>
#include <sstream>
#include <iostream>
#include <iomanip>

#include <sys/time.h>

using namespace std;


//utility class 1
class Rand_int {
	//store the parameter
public:
	Rand_int(int lo, int hi) : p{ lo, hi } { }
	int operator()() const {
		return r();
	};

private:
	uniform_int_distribution<>::param_type p;
	function<int()> r = bind(uniform_int_distribution<>{p}, default_random_engine{});
};

//utilty funtion 1 
class HashUtils {

public: 
	static size_t hash_compute(unsigned char* buffer, int payload_size){
		size_t result = 0;
		size_t prime = 31;
		std::hash<unsigned char> hash_fn;
		for (int i = 0; i < payload_size; i++) {
			result = result *prime + hash_fn(buffer[i]);
		}

		return result;
	}

	static int hash_key(int key, int total_partitions){
		int result = 0;
		int prime = 31;

		result = (key *prime) % total_partitions;
		if (result < 0) {
			result = -result;
		}

		return result;
	}
}; 


class TimeUtil {

 static const long SECOND_IN_NANOSECONDS= 1000000000;

 public: 
 
 static timespec diff(timespec start, timespec end)  {
    timespec temp;
    if ((end.tv_nsec - start.tv_nsec) <0 ) {
      temp.tv_sec = (end.tv_sec - start.tv_sec) -1;
      temp.tv_nsec = SECOND_IN_NANOSECONDS + (end.tv_nsec - start.tv_nsec);
    } else {
      temp.tv_sec = end.tv_sec - start.tv_sec;
      temp.tv_nsec = end.tv_nsec - start.tv_nsec;
    }
    return temp;
  }
};

class ByteArrayUtil {
 public:
 
 static string to_str (const unsigned char *buf, int size) {
     stringstream ss;
     if (buf != nullptr) {
       for (int z =0; z< size; z++) {
	 unsigned char  val = *(buf+z);
	 ss << std::hex << std::uppercase << std::setfill('0') << std::setw(2) << (int)val;
       }
     }
    
     return ss.str();
  }
};

#endif /*#define _SIMPLE_UTILS_H_*/
