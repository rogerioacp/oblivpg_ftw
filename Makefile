# contrib/oblivpg_fdw/Makefile

MODULE_big = oblivpg_fdw
OBJS = obliv_utils.o obliv_status.o obliv_index.o oblivpg_fdw.o

MODULES = oblivpg_fdw

EXTENSION = oblivpg_fdw
DATA = oblivpg_fdw--1.0.sql
PGFILEDESC = "oblivpg_fdw - foreign data wrapper for oblivious access"


PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
