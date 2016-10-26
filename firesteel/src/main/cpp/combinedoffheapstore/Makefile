# $Header: Makefile,v 1.3 95/08/17 14:14:17 hmgr Exp $
# added tcmalloc to replace the default memory allocation in C++.
GCC=/usr/bin/g++

##DEFINED_FLAGS   = -g -DDEBUG -O3 -fPIC
DEFINED_FLAGS   = -march=native -Wall  -fPIC -O3
##for sprof, turned on -g. 08/10/2015
##DEFINED_FLAGS   = -Wall -fPIC -g
##we will need the additional includes later
SHM_SRC_PATH = ../offheapstore
JNI_SRC_PATH = ../jnioffheapstore
SHUFFLE_SRC_PATH = ../shuffle

RMB_INCLUDE = -I../../../../../../alps/src -I../../../../../../alps/third-party/yaml-cpp-0.5.2/include/

CXXINC  = -I$(SHM_SRC_PATH) -I$(SHUFFLE_SRC_PATH) $(RMB_INCLUDE) -I$(JNI_SRC_PATH) -I/usr/local/include -I/usr/java/latest/include -I/usr/java/latest/include/linux
CPLUSPLUS11_OPTIONS=-std=c++11
CXXOPTS = $(CPLUSPLUS11_OPTIONS) $(DEFINED_FLAGS) $(CXXINC)
CXX     = $(GCC)

# for the linking to produce an executable 
##LDOPTS  = -shared -Wl,-soname,$(SHARED_LIB_TO_BUILD) -ltcmalloc -lrt
RMB_SHAREDLIB_PATH  = ../../../../../../alps/build/src
RMB_SHAREDLIB  = alps
LDOPTS  = -shared -Wl,-soname,$(SHARED_LIB_TO_BUILD) -L$(RMB_SHAREDLIB_PATH) -l$(RMB_SHAREDLIB) -ltcmalloc -L/opt/concurrent/intel/tbb/lib -ltbb -lnuma -lglog -lrt 
##LDOPTS  = -shared -Wl,-soname,$(SHARED_LIB_TO_BUILD) -lglog -lrt 

SHARED_LIB_TO_BUILD =libjnioffheapstore.so
OTHERLIBS =  
CXXLIBS = $(LIB_TO_BUILD) $(OTHERLIBS)

##CFLAGS = $(CXXINC) -Ae -D_REENTRANT -g
SRCS	= ../offheapstore/OffHeapStoreManager.cc ../offheapstore/AttributeHashTable.cc \
          ../jnioffheapstore/OffHeapStoreInf.cc 

 
LIB_OBJS    = $(SRCS:.cc=.o)

.SUFFIXES: .cc

.cc.o :
#	$(CXX) $(CXXOPTS) -c $<
	$(CXX) $(CXXOPTS) -c $< -o $@ 


##NOTE: this needs to be the first rule that is actionable. if we
##want to have "make" to assume "make all"
all : $(SHARED_LIB_TO_BUILD)


$(SHARED_LIB_TO_BUILD) :  $(LIB_OBJS) $(C_LIB_OBJS)
	$(CXX) $(LDOPTS) -o $@ $(LIB_OBJS) 

clean :
	-rm -f core $(SHM_SRC_PATH)/*.o $(SHM_SRC_EXTENSION_PATH)/*.o $(JNI_SRC_PATH)/*.o
	-rm -f $(SHARED_LIB_TO_BUILD)

clobber : clean
	-rm -f $(SHARED_LIB_TO_BUILD)
