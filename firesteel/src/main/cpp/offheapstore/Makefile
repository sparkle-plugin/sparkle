# $Header: Makefile,v 1.3 95/08/17 14:14:17 hmgr Exp $
#This make file build the library libLSH.
#Changes:
#add -ltr flag to enable clock_monotic functions to execute. Feb 20, 2014
#add -ltcmalloc flag to enable memory allocator of tcmalloc to replace the built-in C++ memory allocation.
GCC=/usr/bin/g++

##DEFINED_FLAGS   = -g -DREAL_FLOAT -DDEBUG -O3 -fPIC
##DEFINED_FLAGS   = -w -fPIC -g -O3
##DEFINED_FLAGS   = -Wall -Wextra -fPIC -g -O3
DEFINED_FLAGS   = -march=native -Wall -fPIC -O3
##DEFINED_FLAGS   = -g -Wall -fPIC -O3
##we will need the additional includes later. 
##added for tcmalloc. 
RMB_INCLUDE = -I../../../../../../alps/src -I../../../../../../alps/third-party/yaml-cpp-0.5.2/include/

CXXINC  = -I/usr/local/include $(RMB_INCLUDE) -I. -I./longext 
CPLUSPLUS11_OPTIONS=-std=c++11
CXXOPTS = $(CPLUSPLUS11_OPTIONS) $(DEFINED_FLAGS) $(CXXINC)
CXX     = $(GCC)

# for the linking to produce an executable 
##LDOPTS  = -Wall,-w -L$(BOOCHLIB)
##LDOPTS  = -shared -Wl,-soname,$(SHARED_LIB_TO_BUILD) -ltbb -lrt -lglog -ltcmalloc
LDOPTS  = -shared -Wl,-soname,$(SHARED_LIB_TO_BUILD) -lrt -lglog

SHARED_LIB_TO_BUILD =liboffheapstore.so
OTHERLIBS = -lm 
CXXLIBS = $(LIB_TO_BUILD) $(OTHERLIBS)

##CFLAGS = $(CXXINC) -Ae -D_REENTRANT -g
SRCS	= OffHeapStoreManager.cc AttributeHashTable.cc 
           

LIB_OBJS    = $(SRCS:.cc=.o)

.SUFFIXES: .cc

.cc.o :
##	$(CXX) $(CXXOPTS) -c $<
	$(CXX) $(CXXOPTS) -c $< -o $@

##NOTE: this needs to be the first rule that is actionable. if we
##want to have "make" to assume "make all"
all : $(SHARED_LIB_TO_BUILD)


$(SHARED_LIB_TO_BUILD) :  $(LIB_OBJS)
	$(CXX) $(LDOPTS) -o $@ $(LIB_OBJS)

clean :
	-rm -f core *.o longext/*.o
	-rm -f $(SHARED_LIB_TO_BUILD)

clobber : clean
	-rm -f $(SHARED_LIB_TO_BUILD)
