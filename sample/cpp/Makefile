# Standalone YSQL IN-list client. Uses Homebrew libpq on macOS by default.
#
#   make
#   ./in_list_client create-schema --tablets 8 --rows 10000 --subkeys 1
#   ./in_list_client query --in-list-size 64 --iterations 20 --subkeys 1
#   ./in_list_client explain --in-list-size 64 --subkeys 2 --independent-in
#
#   make bnl_client
#   ./bnl_client create-schema --key-size 32768 --tablets 8 --rows 1024
#   ./bnl_client explain --key-size 32768

LIBPQ_PREFIX ?= /opt/homebrew/opt/libpq
CXX ?= clang++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra
CPPFLAGS += -I$(LIBPQ_PREFIX)/include
LDFLAGS += -L$(LIBPQ_PREFIX)/lib -Wl,-rpath,$(LIBPQ_PREFIX)/lib -lpq

.PHONY: all clean

all: in_list_client bnl_client

in_list_client: in_list_client.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $< -o $@ $(LDFLAGS)

bnl_client: bnl_client.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $< -o $@ $(LDFLAGS)

clean:
	rm -f in_list_client bnl_client
