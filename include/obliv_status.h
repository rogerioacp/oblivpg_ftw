/*-------------------------------------------------------------------------
 *
 * index.h
 *	  prototypes for contrib/oblivpg_fdw/obliv_status.c.
 *
 *
 * Copyright (c) 2018-2019, HASLab
 *
 * contrib/oblivpg_fdw/include/obliv_status.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OBLIV_STATUS_H
#define OBLIV_STATUS_H

#include "postgres.h"

/* human-readable names for addressing columns of the obl_ftw table */
#define Anum_obl_ftw_oid 1
#define Anum_obl_mirror_table_oid 2
#define Anum_obl_mirror_index_oid 3
#define Anum_obl_mirror_index_am 4
#define Anum_obl_ftw_index_relfilenode 5


#define OBLIV_MAPPING_TABLE_NAME "obl_ftw"


#define INVALID_STATUS 0
#define OBLIVIOUS_UNINTIALIZED 1
#define OBLIVIOUS_INITIALIZED 2


typedef unsigned int Ostatus;

typedef struct FdwIndexTableStatus
{

	/**
	 * the mirror table relation Id.
	 **/
	Oid			relMirrorId;

	/**
	 * The index mirror relation Id.
	 **/
	Oid			relIndexMirrorId;

	/**
	 *  relam is the Oid that defines the type of the index.
	 *  It has the same name and follows the same semantics as the variable relam in
	 *  the catalog tablepg_class.h
	 *  This variable is used to check if it is a BTree, Hash ... and use the correct methods.
	 */
	Oid			relam;

	/*
	 * identifier of physical storage file. Is null if it has not been created
	 * yet.
	 */
	Oid			relfilenode;
} FdwIndexTableStatus;


FdwIndexTableStatus getIndexStatus(Oid ftwOid, Oid mappingOid);

Ostatus		validateIndexStatus(FdwIndexTableStatus toValidate);

#endif							/* OBLIV_STATUS_H */
