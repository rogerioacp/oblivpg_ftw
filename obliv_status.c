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

#include "c.h"
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

FdwOblivTableStatus
getOblivTableStatus(Oid ftwOid, Relation rel)
{

	ScanKeyData skey;
	TupleDesc	tupleDesc;
	HeapScanDesc scan;
	Snapshot	snapshot;
	HeapTuple	tuple;
	bool		found;
	FdwOblivTableStatus iStatus;

	iStatus.tableRelFileNode = InvalidOid;
	iStatus.relIndexMirrorId = InvalidOid;
	iStatus.tableNBlocks = 0;
	iStatus.indexNBlocks = 0;
	iStatus.filesInitated = false;

	tupleDesc = RelationGetDescr(rel);

	ScanKeyInit(&skey,
				Anum_obl_ftw_table_relfilenode,
				InvalidStrategy, F_OIDEQ, ObjectIdGetDatum(ftwOid));
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = heap_beginscan(rel, snapshot, 1, &skey);

	/* There can be at most one matching row */
	tuple = heap_getnext(scan, ForwardScanDirection);
	found = HeapTupleIsValid(tuple);

	if (found)
	{
		bool        isMirrorTableNull,
					isMirrorIndexNull,
		            isTableNBlocksNull,
		            isIndexNBlocksNull,
		            isInitNull;


		Datum       dMirrorTableId,
					dMirrorIndexId,
					dTableNBlocks,
					dIndexNBlocks,
					dInit;

		int tableNBlocks = 0;
		int indexNBlocks = 0;

		dMirrorTableId = heap_getattr(tuple, Anum_obl_mirror_table_oid, tupleDesc, &isMirrorTableNull);
		dMirrorIndexId = heap_getattr(tuple, Anum_obl_mirror_index_oid, tupleDesc, &isMirrorIndexNull);
		dTableNBlocks = heap_getattr(tuple, Anum_obl_ftw_table_nblocks, tupleDesc, &isTableNBlocksNull);
		dIndexNBlocks = heap_getattr(tuple, Anum_obl_ftw_index_nblocks, tupleDesc, &isIndexNBlocksNull);
        dInit = heap_getattr(tuple,  Anum_obl_init, tupleDesc, &isInitNull);

		if (!isMirrorTableNull)
			iStatus.relTableMirrorId = DatumGetObjectId(dMirrorTableId);
		if (!isMirrorIndexNull)
			iStatus.relIndexMirrorId = DatumGetObjectId(dMirrorIndexId);
		if(!isTableNBlocksNull) {
			//iStatus.tableNBlocks = DatumGetInt32(dTableNBlocks);
			tableNBlocks = DatumGetInt32(dTableNBlocks);
			//elog(DEBUG1, "number of table blocks %d", tableNBlocks);
			iStatus.tableNBlocks = tableNBlocks;
		}
		if(!isIndexNBlocksNull){
			indexNBlocks = DatumGetInt32(dIndexNBlocks);
			//elog(DEBUG1, "number of index blocks %d", indexNBlocks);
			iStatus.indexNBlocks = indexNBlocks;
		}
		if(!isInitNull)
			iStatus.filesInitated = DatumGetBool(dInit);
	}
	else
	{
		heap_endscan(scan);
		UnregisterSnapshot(snapshot);
		/**
		 * Throw error to warn user that there must be a valid tuple with information about the oblivous foreign table.
		 **/
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("No valid record found in %s", OBLIV_MAPPING_TABLE_NAME)));

	}

	/* Check if iStatus is in a consistent state. */
	heap_endscan(scan);
	UnregisterSnapshot(snapshot);

	return iStatus;

}


Ostatus
validateIndexStatus(FdwOblivTableStatus toValidate)
{

	Ostatus		result = INVALID_STATUS;

	if (toValidate.tableRelFileNode == InvalidOid)
	{
		/**
         * Throw error to warn user that there must be a valid index access method for the Mirror Index.
         **/
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("Oid of FDW table is not valid on table %s", OBLIV_MAPPING_TABLE_NAME)));
		return result;

	}


	if (toValidate.relTableMirrorId == InvalidOid)
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


	if(toValidate.tableNBlocks <= 0 ){
        /**
         * Throw error to warn user that there must be a valid index access method for the Mirror Index.
         **/
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Number of blocks %d for oblivious simulation of table %s is not valid", toValidate.tableNBlocks,  OBLIV_MAPPING_TABLE_NAME)));
        return result;
	}

    if(toValidate.indexNBlocks <= 0 ){
        /**
         * Throw error to warn user that there must be a valid index access method for the Mirror Index.
         **/
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Number of blocks %d for oblivious simulation of table index %s is not valid", toValidate.indexNBlocks, OBLIV_MAPPING_TABLE_NAME)));
        return result;
    }


    if (toValidate.filesInitated)
	{
        return OBLIVIOUS_INITIALIZED;
	}

    return OBLIVIOUS_UNINTIALIZED;
}

void setOblivStatusInitated(FdwOblivTableStatus status, Relation mappingRel){


	ScanKeyData skey;
	HeapScanDesc scan;
	Snapshot	snapshot;
	HeapTuple	oldTuple;
	HeapTuple   newTuple;
	bool		found;
	Datum		new_record[Natts_obliv_mapping];
	bool		new_record_nulls[Natts_obliv_mapping];
	bool		new_record_repl[Natts_obliv_mapping];


	ScanKeyInit(&skey,
				Anum_obl_ftw_table_relfilenode,
				InvalidStrategy, F_OIDEQ, ObjectIdGetDatum(status.tableRelFileNode));
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = heap_beginscan(mappingRel, snapshot, 1, &skey);

	/* There can be at most one matching row */
	oldTuple = heap_getnext(scan, ForwardScanDirection);
	found = HeapTupleIsValid(oldTuple);

	if (found)
	{

		MemSet(new_record, 0, sizeof(new_record));
		MemSet(new_record_nulls, false, sizeof(new_record_nulls));
		MemSet(new_record_repl, false, sizeof(new_record_repl));
		new_record[Anum_obl_init-1] = BoolGetDatum(true);
		new_record_repl[Anum_obl_init-1] = true;


		newTuple = heap_modify_tuple(oldTuple, RelationGetDescr(mappingRel),
					new_record, new_record_nulls, new_record_repl);

		simple_heap_update(mappingRel, &oldTuple->t_self,newTuple);
		//heap_freetuple(newTuple);

	}else{
		/**
		 *  Something went really wrong, somehow the tuple that existed in the first scan on the function getIndexStatus
		 *  disapeared. This function has a pointer to the same relation that was used on the function getIndexStatus
		 *  and the relation should have  a RowExclusiveLock.
		 **/
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg(" updateOblivIndexStatus can not find the valid record on the table %s", OBLIV_MAPPING_TABLE_NAME)));

	}

	/* Check if iStatus is in a consistent state. */
	heap_endscan(scan);
	UnregisterSnapshot(snapshot);
}