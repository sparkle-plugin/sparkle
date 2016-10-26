[//]: # ( (c) Copyright 2016 Hewlett Packard Enterprise Development LP             )
[//]: # (                                                                          )
[//]: # ( Licensed under the Apache License, Version 2.0 (the "License");          )
[//]: # ( you may not use this file except in compliance with the License.         )
[//]: # ( You may obtain a copy of the License at                                  )
[//]: # (                                                                          )
[//]: # (     http://www.apache.org/licenses/LICENSE-2.0                           )
[//]: # (                                                                          )
[//]: # ( Unless required by applicable law or agreed to in writing, software      )
[//]: # ( distributed under the License is distributed on an "AS IS" BASIS,        )
[//]: # ( WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. )
[//]: # ( See the License for the specific language governing permissions and      )
[//]: # ( limitations under the License.                                           )


# ALPINiSM

ALPINiSM (Abstraction Layer for Programming Persistent Shared Memory)
provides a low-level abstraction layer that reliefs the user from the 
details of mapping, addressing, and allocating persistent shared memory 
(also known as fabric-attached memory).
This layer can be used as a building block for building higher level 
abstractions and data structures such as heaps, logs, etc.

## Building ALPINiSM

	$ cd $ALPS
	$ mkdir build
	$ cd build

For Debug build type (default):

	$ cmake .. -DTARGET_ARCH_MEM=NV-NCC-FAM -DCMAKE_BUILD_TYPE=Debug

For Release build type:

	$ cmake .. -DTARGET_ARCH_MEM=NV-NCC-FAM -DCMAKE_BUILD_TYPE=Release

## Installing Dependencies

### L4TM

apt-get install vim git
apt-get install cmake
apt-get install libnuma 
apt-get install libnuma-dev

apt-get install libconfig++-dev

sudo apt-get install libboost-all-dev

OR

sudo apt-get install libboost-dev
sudo apt-get install libboost-serialization-dev

sudo apt-get install libattr1-dev
sudo apt-get install libyaml-cpp-dev

sudo apt-get install python3

### RedHat

sudo yum install attr1


## Example Programs

ALPINiSM comes with several samples in the `examples` directory.

## Running Tests

## Style Guide 

We follow the Google C++ style guide available here:

https://google.github.io/styleguide/cppguide.html

## Frequently Asked Questions

### Why yet another persistent memory allocator?

Several persistent memory allocators already exist. For example, libpmemobj 
is an industrial-strength library developed by Intel that provides a 
transactional object store, including a persistent memory allocator. 
Although, it was likely developed before our allocator, to the best of our 
knowledge, the first stable implementation and public description appeared 
later in mid-June 2015 (http://pmem.io/2015/06/11/type-safety-macros.html),
about the time when we started work on our own memory allocator. 

We considered leveraging libpmemobj for building our allocator given its 
maturity but we eventually decided in favor of building our own. In general,
libpmemobj makes different design assumptions about the underlying hardware 
that would make such a porting exercise a challenging task. In particular:
- libpmemobj has no support for multiple process. Extending it to multiple 
processes would require introducing inter-process synchronization, extend the 
code to deal with in-coherent accesses to allocator metadata between multiple 
processes, and reorganize the heap layout (e.g. shard the heap) to reduce 
expensive fabric synchronization between multiple processes. 
- libpmemobj's allocator appears to be coupled with the transactional object store.
- libpmemobj's type-safe pointer addressing scheme is implemented in C. This is a 
heroic attempt to introduce type-safety to a language that is not type safe, and
not surprisingly the end result suffers from a lot of ugliness. Instead, we choose
to implement smart pointers in C++ and benefit from C++ type system.
- libpmemobj supports only mirroring and provides no flexibility in replication.
- We would have less flexibility and control over the design and implementation 
of the allocator.
