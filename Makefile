# contrib/oblivpg_fdw/Makefile

MODULES = oblivpg_fdw

EXTENSION = oblipg_fdw
DATA = oblivpg_fdw--1.0.sql
PGFILEDESC = "oblivpg_fdw - foreign data wrapper for oblivious access"


PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
