/*-------------------------------------------------------------------------
 *
 * obliv_status.c
 *	  code to manage and access Oblivious status table
 *
 *  Copyright (c) 2018-2019, HASLab
 *
 *
 * IDENTIFICATION
 * contrib/oblivpg_fdw/obliv_status.c
 *
 *
 * INTERFACE ROUTINES
 *		getIndexStatus()			- Obtain information on a given oblivious table.
 *
 *-------------------------------------------------------------------------
 */

#include "include/obliv_status.h"

#include "access/heapam.h"
#include "access/skey.h"
#include "access/stratnum.h"
#include "access/tupdesc.h"
#include "storage/lockdefs.h"
#include "utils/fmgroids.h"
#include "access/htup_details.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

FdwIndexTableStatus
getIndexStatus(Oid ftwOid, Oid mappingOid)
{

	Relation	rel;
	ScanKeyData skey;
	TupleDesc	tupleDesc;
	HeapScanDesc scan;
	Snapshot	snapshot;
	HeapTuple	tuple;
	bool		found;
	FdwIndexTableStatus iStatus;

	iStatus.relMirrorId = InvalidOid;
	iStatus.relIndexMirrorId = InvalidOid;
	iStatus.relam = InvalidOid;
	iStatus.relfilenode = InvalidOid;

	rel = heap_open(mappingOid, AccessShareLock);
	tupleDesc = RelationGetDescr(rel);

	ScanKeyInit(&skey,
				Anum_obl_ftw_oid,
				InvalidStrategy, F_OIDEQ, ObjectIdGetDatum(ftwOid));

	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = heap_beginscan(rel, snapshot, 1, &skey);

	/* There can be at most one matching row */
	tuple = heap_getnext(scan, ForwardScanDirection);
	found = HeapTupleIsValid(tuple);

	if (found)
	{
		bool		isMirrorTableNull,
					isMirrorIndexNull,
					isIndexAmNull,
					isIndexRfnNull;
		Datum		dMirrorTableId,
					dMirrorIndexId,
					dIndexAm,
					dIndexRfn;

		dMirrorTableId = heap_getattr(tuple, Anum_obl_mirror_table_oid, tupleDesc, &isMirrorTableNull);
		dMirrorIndexId = heap_getattr(tuple, Anum_obl_mirror_index_oid, tupleDesc, &isMirrorIndexNull);
		dIndexAm = heap_getattr(tuple, Anum_obl_mirror_index_am, tupleDesc, &isIndexAmNull);
		dIndexRfn = heap_getattr(tuple, Anum_obl_ftw_index_relfilenode, tupleDesc, &isIndexRfnNull);


		if (!isMirrorTableNull)
			iStatus.relMirrorId = DatumGetObjectId(dMirrorTableId);
		if (!isMirrorIndexNull)
			iStatus.relIndexMirrorId = DatumGetObjectId(dMirrorIndexId);
		if (!isIndexAmNull)
			iStatus.relam = DatumGetObjectId(dIndexAm);
		if (!isIndexRfnNull)
			iStatus.relfilenode = DatumGetObjectId(dIndexRfn);
	}
	else
	{
		/**
		 * Throw error to warn user that there must be a table to store the index.
		 * Or we can assume that there are no indexes.
		 * In the current implementation we simply warn the user.
		 **/
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("No valid record found in %s", OBLIV_MAPPING_TABLE_NAME)));

	}

	/* Check if iStatus is in a consistent state. */
	heap_endscan(scan);
	heap_close(rel, AccessShareLock);
	UnregisterSnapshot(snapshot);

	return iStatus;

}


Ostatus
validateIndexStatus(FdwIndexTableStatus toValidate)
{

	Ostatus		result = INVALID_STATUS;

	if (toValidate.relMirrorId == InvalidOid)
	{
		/**
		 * Throw error to warn user that there must be a valid Oid for the Mirror relation.
		 **/
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("Oid of mirror relation is not valid on table %s", OBLIV_MAPPING_TABLE_NAME)));
		return result;
	}

	if (toValidate.relIndexMirrorId == InvalidOid)
	{
		/**
		 * Throw error to warn user that there must be a valid Oid for the Mirror Index.
		 **/
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("Oid of mirror Index is not valid on table %s", OBLIV_MAPPING_TABLE_NAME)));
		return result;
	}

	if (toValidate.relam == InvalidOid)
	{
		/**
         * Throw error to warn user that there must be a valid index access method for the Mirror Index.
         **/
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("Oid of access method of mirror Index is not valid on table %s", OBLIV_MAPPING_TABLE_NAME)));
		return result;

	}

	if (toValidate.relfilenode == InvalidOid)
	{

		return OBLIVIOUS_UNINTIALIZED;

	}
	else
	{
		return OBLIVIOUS_INITIALIZED;
	}

}
