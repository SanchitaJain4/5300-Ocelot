# Makefile for Milestone 1
# Authors: Alex Larsen, Yao Yao
# Functonality based on example provided by Kevin Lundeen

CCFLAGS = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c
COURSE = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR = $(COURSE)/lib

# List of compiled object files required to build the executable
OBJS = sql5300.o heap_storage.o

# General rule for compilation
%.o: %.cpp
	g++ -I$(INCLUDE_DIR) $(CCFLAGS) -o "$@" "$<"

# Rule for linking the executable
sql5300: $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $< -ldb_cxx -lsqlparser

sql5300.o: heap_storage.h storage_engine.h
heap_storage.o: heap_storage.h storage_engine.h

# Rule for removing non-source files
clean:
	rm -f sql5300 *.o

