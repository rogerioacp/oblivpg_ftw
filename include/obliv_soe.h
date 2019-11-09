/*-------------------------------------------------------------------------
 *
 * obli_soe.h
 *	  prototypes for contrib/oblivpg_fdw/obliv_soe.c.
 *
 *
 * Copyright (c) 2018-2019, HASLab
 *
 * contrib/oblivpg_fdw/include/obliv_soe.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OBLIV_SOE_H
#define OBLIV_SOE_H


#include "postgres.h"
#include "storage/bufpage.h"
#include "oblivpg_fdw.h"

#define SOE_CONTEXT "SOE_CONTEXT"

void		initSOE(char *relName, size_t nblocks, size_t bucketCapacity);
void		insertTuple(const char *relname, Item item, Size size);
bool		getTuple(OblivScanState *state);


#endif							/* OBLIV_SOE_H */
