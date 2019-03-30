# contrib/oblivpg_fdw/Makefile

MODULE_big = oblivpg_fdw
SHLIB_LINK = -loram $(GLIB_LIB)

OBJS = obliv_utils.o obliv_status.o obliv_soe.o obliv_ofile.o oblivpg_fdw.o

EXTENSION = oblivpg_fdw
#DATA =  oblivpg_fdw--unpackaged--1.0.sql

#DATA = oblivpg_fdw--1.0.sql
PGFILEDESC = "oblivpg_fdw - foreign data wrapper for oblivious access"

#PG_CPPFLAGS= -I/usr/local/include

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
