# $Header: Makefile,v 1.3 95/08/17 14:14:17 hmgr Exp $
# added tcmalloc to replace the default memory allocation in C++.
GCC=/usr/bin/g++

##DEFINED_FLAGS   = -g -DDEBUG -O3 -fPIC
DEFINED_FLAGS   = -Wall -fPIC -O3
##we will need the additional includes later
CXXINC  = -I/usr/java/latest/include -I/usr/java/latest/include/linux -I../offheapstore 
CPLUSPLUS11_OPTIONS=-std=c++11
CXXOPTS = $(CPLUSPLUS11_OPTIONS) $(DEFINED_FLAGS) $(CXXINC)
CXX     = $(GCC)

# for the linking to produce an executable 
##LDOPTS  = -Wall,-w -L$(BOOCHLIB)
SHMSHUFFLE_LIB_PATH   = ../shuffle/
LSH_LIB   = shmshuffle
##LDOPTS  = -shared -Wl,-soname,$(SHARED_LIB_TO_BUILD) -L$(LSH_LIB_PATH) -l$(LSH_LIB) -ltcmalloc -lrt
LDOPTS  = -shared -Wl,-soname,$(SHARED_LIB_TO_BUILD) -L$(LSH_LIB_PATH) -l$(LSH_LIB) -lrt

SHARED_LIB_TO_BUILD =libjnioffheapstore.so
OTHERLIBS =  
CXXLIBS = $(LIB_TO_BUILD) $(OTHERLIBS)

##CFLAGS = $(CXXINC) -Ae -D_REENTRANT -g
SRCS	= OffHeapStoreInf.cc

 
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
