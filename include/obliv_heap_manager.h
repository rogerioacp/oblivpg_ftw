/*-------------------------------------------------------------------------
 *
 * obliv_index.h
 *	  prototypes for contrib/oblivpg_fdw/index.c.
 *
 *
 * Copyright (c) 2018-2019, HASLab
 *
 * contrib/oblivpg_fdw/include/obliv_index.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OBLIV_INDEX_H
#define OBLIV_INDEX_H

#include "obliv_status.h"

#include "c.h"
#include "utils/relcache.h"


Relation	obliv_table_create(FdwOblivTableStatus status);
Relation	obliv_index_create(FdwOblivTableStatus status);


#endif							/* OBLIV_INDEX_H */
