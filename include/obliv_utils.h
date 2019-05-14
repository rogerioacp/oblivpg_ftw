/*-------------------------------------------------------------------------
 *
 * obliv_utils.h
 *	  prototypes for contrib/oblivpg_fdw/obliv_utils.c.
 *
 *
 * Copyright (c) 2018-2019, HASLab
 *
 * contrib/oblivpg_fdw/include/obliv_utils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OBLIV_UTILS_H
#define OBLIV_UTILS_H

#include "postgres.h"
#include "access/heapam.h"

char        *generateOblivTableName(char *tableName);
Oid			GenerateNewRelFileNode(Oid tableSpaceId, char relpersistance);
HeapTuple   heap_prepare_insert(Relation relation, HeapTuple tup, TransactionId xid, CommandId cid, int options);
#endif							/* OBLIV_UTILS_H */
