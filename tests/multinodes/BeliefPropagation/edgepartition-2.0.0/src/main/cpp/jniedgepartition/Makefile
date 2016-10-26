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

GCC=/usr/bin/g++

##DEFINED_FLAGS   = -g -DDEBUG -O3 -fPIC
#DEFINED_FLAGS   = -Wall -fPIC -O3 -mavx -Ofast -ffast-math
#DEFINED_FLAGS   = -Wall -fPIC -O3 -mavx 
DEFINED_FLAGS   = -Wall -fPIC -O3 -Ofast -ffast-math
#DEFINED_FLAGS   = -Wall -fPIC -O3 
##we will need the additional includes later
CXXINC  = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux
CPLUSPLUS11_OPTIONS=-std=c++11
CXXOPTS = $(CPLUSPLUS11_OPTIONS) $(DEFINED_FLAGS) $(CXXINC)
CXX     = $(GCC)

# for the linking to produce an executable 
##LDOPTS  = -Wall,-w -L$(BOOCHLIB)
SHMSHUFFLE_LIB_PATH   = ../shuffle/
LSH_LIB   = shmshuffle
##LDOPTS  = -shared -Wl,-soname,$(SHARED_LIB_TO_BUILD) -L$(LSH_LIB_PATH) -l$(LSH_LIB) -ltcmalloc -lrt
LDOPTS  = -shared -Wl,-soname,$(SHARED_LIB_TO_BUILD) -L$(LSH_LIB_PATH) -l$(LSH_LIB) -lglog -lrt

SHARED_LIB_TO_BUILD =libjniedgepartition.so
OTHERLIBS =  
CXXLIBS = $(LIB_TO_BUILD) $(OTHERLIBS)

##CFLAGS = $(CXXINC) -Ae -D_REENTRANT -g
SRCS	= jni_EdgeMessageComputeAggregate.cc 

 
LIB_OBJS    = $(SRCS:.cc=.o)

.SUFFIXES: .cc

.cc.o :
	$(CXX) $(CXXOPTS) -c $<

##NOTE: this needs to be the first rule that is actionable. if we
##want to have "make" to assume "make all"
all : $(SHARED_LIB_TO_BUILD)


$(SHARED_LIB_TO_BUILD) :  $(LIB_OBJS)
	$(CXX) $(LDOPTS) -o $@ $(LIB_OBJS)

clean :
	-rm -f core *.o 
	-rm -f $(SHARED_LIB_TO_BUILD)

clobber : clean
	-rm -f $(SHARED_LIB_TO_BUILD)
