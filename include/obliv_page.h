/*-------------------------------------------------------------------------
 *
 * obliv_page.h
 *	  header file for oblivious page special space structure
 *
 * Copyright (c) 2018-2019, HASLab
 *
 * IDENTIFICATION
 *		  contrib/oblivpg_fdw/oblivpg_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef POSTGRESQL_OBLIV_PAGE_H
#define POSTGRESQL_OBLIV_PAGE_H

//Data structure of the contents stored on every oblivious page.
typedef struct OblivPageOpaqueData
{
    int		o_blkno; //original block number. This should be encrypted.

} OblivPageOpaqueData;

typedef OblivPageOpaqueData *OblivPageOpaque;

#endif //POSTGRESQL_OBLIV_PAGE_H
