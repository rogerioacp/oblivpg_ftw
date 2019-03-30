/*-------------------------------------------------------------------------
 *
 * obliv_status.h
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
#include "utils/relcache.h"

/* human-readable names for addressing columns of the obl_ftw table */
#define Anum_obl_ftw_table_relfilenode 1
#define Anum_obl_mirror_table_oid 2
#define Anum_obl_mirror_index_oid 3
#define Anum_obl_mirror_index_am 4
#define Anum_obl_ftw_table_nblocks 5
#define Anum_obl_ftw_index_nblocks 6
#define Anum_obl_ftw_heap_table_relfilenode 7
#define Anum_obl_ftw_index_relfilenode 8
#define Anum_obl_init 9

#define Natts_obliv_mapping 9

#define OBLIV_MAPPING_TABLE_NAME "obl_ftw"


#define INVALID_STATUS 0
#define OBLIVIOUS_UNINTIALIZED 1
#define OBLIVIOUS_INITIALIZED 2


typedef unsigned int Ostatus;

typedef struct FdwOblivTableStatus
{

	/**
	 * the mirror table relation Id.
	 **/
	Oid			tableRelFileNode;

	/**
	 *  The table mirror relation id.
	 **/
	Oid 	relTableMirrorId;

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


	/* number of blocks that the mirror table should have. This number is used by the
	 * ORAM algorithm to calculate the number of blocks to allocate to the oblivious
	 * table*/
	int 		tableNBlocks;

	/* number of blocks that the mirror index should have. This number is used by the
	 * ORAM algorithm to calculate the number of blocks to allocate to the oblivious
	 * index*/
	int 		indexNBlocks;


    /*
     * identifier of the underlying heap table physical storage file.
     * Is null if it has not been created yet.
     */
    Oid			heapTableRelFileNode;


    /*
     * identifier of the index physical storage file.
     * Is null if it has not been created yet.
     */
    Oid			indexRelFileNode;



    // boolean flag defining if the relation and index oblivious file have been initated.
	bool 		filesInitated;
} FdwOblivTableStatus;


typedef struct OblivWriteState
{

	FdwOblivTableStatus indexedTableStatus;
	MemoryContext writeContext;

}			OblivWriteState;

FdwOblivTableStatus getOblivTableStatus(Oid ftwOid, Relation mappingOid);

Ostatus		validateIndexStatus(FdwOblivTableStatus toValidate);

void		setOblivStatusInitated(FdwOblivTableStatus status, Relation mappingRel);

#endif							/* OBLIV_STATUS_H */
