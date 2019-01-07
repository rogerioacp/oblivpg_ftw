/*-------------------------------------------------------------------------
 *
 * index.h
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


char	   *generateOblivTableName(char *tableName);
Oid			GenerateNewRelFileNode(Oid tableSpaceId, char relpersistance);

#endif							/* OBLIV_UTILS_H */
