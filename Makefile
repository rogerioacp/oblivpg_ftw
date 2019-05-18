# contrib/oblivpg_fdw/Makefile

MODULE_big = oblivpg_fdw
OBJS = obliv_utils.o obliv_status.o oblivpg_fdw.o obliv_ocalls.o

ifeq ($(UNSAFE), 1)
	SOE_LIB = -lsoeus
else
	SOE_LIB = -lsoeu $(SGX_LIB) 
endif

SHLIB_LINK = -loram -lcollectc $(ENCLAVE_LIB) $(SOE_LIB)


EXTENSION = oblivpg_fdw

DATA = oblivpg_fdw--1.0.sql
PGFILEDESC = "oblivpg_fdw - foreign data wrapper for oblivious access"

REGRESS = heap_insert_small heap_insert_select_small heap_insert heap_insert_select

#REGRESS_OPTS = --dlpath=/usr/local/lib/soe 

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)


