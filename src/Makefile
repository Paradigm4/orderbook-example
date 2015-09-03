ifeq ($(SCIDB),) 
  X := $(shell which scidb 2>/dev/null)
  ifneq ($(X),)
    X := $(shell dirname ${X})
    SCIDB := $(shell dirname ${X})
  endif
endif

CFLAGS=-pedantic -W -Wextra -Wall -Wno-variadic-macros -Wno-strict-aliasing -Wno-long-long -Wno-unused-parameter -fPIC -D_STDC_FORMAT_MACROS -Wno-system-headers -isystem -O2 -g -DNDEBUG -ggdb3  -D_STDC_LIMIT_MACROS
INC=-I. -DPROJECT_ROOT="\"$(SCIDB)\"" -I"$(SCIDB)/3rdparty/boost/include/" -I"$(SCIDB)/include"

LIBS=-shared -Wl,-soname,libsuperfunpack.so  -L. -L"$(SCIDB)/3rdparty/boost/lib" -L"$(SCIDB)/lib" -Wl,-rpath,$(SCIDB)/lib:$(RPATH) -lm

# Compiler settings for SciDB version >= 15.7
ifneq ("$(wildcard /usr/bin/g++-4.9)","")
 CC := "/usr/bin/gcc-4.9"
 CXX := "/usr/bin/g++-4.9"
 CFLAGS+=-std=c++11 -DCPP11
else
 ifneq ("$(wildcard /opt/rh/devtoolset-3/root/usr/bin/gcc)","")
  CC := "/opt/rh/devtoolset-3/root/usr/bin/gcc"
  CXX := "/opt/rh/devtoolset-3/root/usr/bin/g++"
  CFLAGS+=-std=c++11 -DCPP11
 endif
endif

all:
	@if test ! -d "$(SCIDB)"; then echo  "Error. Try:\n\nmake SCIDB=<PATH TO SCIDB INSTALL PATH>"; exit 1; fi
	$(CXX) $(CFLAGS) $(INC) -o liborderbook.so orderbook.cpp $(LIBS)
	@echo "Now copy liborderbook.so to your SciDB lib/scidb/plugins directory and restart SciDB."

clean:
	rm -f *.so *.o
