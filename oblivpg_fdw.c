/*-------------------------------------------------------------------------
 *
 * file_fdw.c
 *		  foreign-data wrapper for server-side zero leakage indexes and heap.
 *
 * Copyright (c) 2018-2019, HASLab
 *
 * IDENTIFICATION
 *		  contrib/oblivpg_fdw/oblivpg_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

//Include for makeNode?
//#include "nodes/makefuncs.h"

#include "foreign/fdwapi.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "utils/fmgroids.h"


#define Anum_obl_ftw_oid 1
#define Anum_obl_ftw_table_oid 2
#define Anum_obl_ftw_index_oid 3



/**
 * Postgres macro to ensure that the compiled object file is not loaded to
 * an incompatible server.
 */
#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/**
 * Postgres initialization function which is called immediately after the an 
 * extension is loaded. This function can latter be used to initialize SGX 
 * enclaves and set-up Remote attestation.
 */
void
_PG_init ()
{

}

/**
 * Postgres cleaning function which is called just before an extension is
 * unloaded from a server. This function can latter be used to close SGX 
 * enclaves and clean the final context.
 */
void
_PG_fini ()
{

}




PG_FUNCTION_INFO_V1 (oblivpg_fdw_handler);


/*
 * FDW callback routines
 */

/* Functions for scanning oblivious index and table */
static void obliviousGetForeignRelSize (PlannerInfo * root,
					RelOptInfo * baserel,
					Oid foreigntableid);
static void obliviousGetForeignPaths (PlannerInfo * root,
				      RelOptInfo * baserel,
				      Oid foreigntableid);
static ForeignScan *obliviousGetForeignPlan (PlannerInfo * root,
					     RelOptInfo * baserel,
					     Oid foreigntableid,
					     ForeignPath * best_path,
					     List * tlist,
					     List * scan_clauses,
					     Plan * outer_plan);
static void obliviousExplainForeignScan (ForeignScanState * node,
					 ExplainState * es);
static void obliviousBeginForeignScan (ForeignScanState * node, int eflags);
static TupleTableSlot *obliviousIterateForeignScan (ForeignScanState * node);
static void obliviousReScanForeignScan (ForeignScanState * node);
static void obliviousEndForeignScan (ForeignScanState * node);
static bool obliviousAnalyzeForeignTable (Relation relation,
					  AcquireSampleRowsFunc * func,
					  BlockNumber * totalpages);
static bool obliviousIsForeignScanParallelSafe (PlannerInfo * root,
						RelOptInfo * rel,
						RangeTblEntry * rte);


/* Functions for updating foreign tables */


/**
 * Function used by postgres before issuing a table update. This function is 
 * used to initialize the necessary resources to have an oblivious heap 
 * and oblivious table
 */
void obliviousBeginForeignModify (ModifyTableState * mtstate,
				  ResultRelInfo * rinfo, List * fdw_private,
				  int subplan_index, int eflags);

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to callback routines.
 */
Datum
oblivpg_fdw_handler (PG_FUNCTION_ARGS)
{
  FdwRoutine *fdwroutine = makeNode (FdwRoutine);

  // Oblivious table scan functions

  fdwroutine->GetForeignRelSize = obliviousGetForeignRelSize;
  fdwroutine->GetForeignPaths = obliviousGetForeignPaths;
  fdwroutine->GetForeignPlan = obliviousGetForeignPlan;
  fdwroutine->ExplainForeignScan = obliviousExplainForeignScan;
  fdwroutine->BeginForeignScan = obliviousBeginForeignScan;
  fdwroutine->IterateForeignScan = obliviousIterateForeignScan;
  fdwroutine->ReScanForeignScan = obliviousReScanForeignScan;
  fdwroutine->EndForeignScan = obliviousEndForeignScan;
  fdwroutine->AnalyzeForeignTable = obliviousAnalyzeForeignTable;
  fdwroutine->IsForeignScanParallelSafe = obliviousIsForeignScanParallelSafe;

  //  Oblivious insertion, update, deletion table functions
  fdwroutine->BeginForeignModify = obliviousBeginForeignModify;

  PG_RETURN_POINTER (fdwroutine);
}



void getOblivOids(Oid ftw_Oid, Oid ftw_map_Oid){

	Relation rel;
	ScanKeyData skey;
	TupleDesc tupleDesc;
	rel = heap_open(ftw_Oid, AccessShareLock);
	tupleDesc = RelationGetDescr(rel);
	HeapScanDesc scanDesc;

	ScanKeyInit(&skey,
				Anum_obl_ftw_oid,
				NULL, F_NAMEEQ, CStringGetDatum('obl_ftw'));

	heap_close(rel, AccessShareLock);
	scanDesc = heap_beginscan(rel, )
	//Number of columns in the obl_ftw table.


}

Oid getOblivFtwMappingTable(Oid ftw_Oid){
	Relation	rel;
	ScanKeyData skey;
	bool		found;
	SysScanDesc conscan;
	HeapTuple tuple;

	rel = heap_open(RelationRelationId, AccessShareLock);

	ScanKeyInit(&skey,
				Anum_pg_class_relname,
				NULL, F_OIDEQ, CStringGetDatum('obl_ftw'));

	/**
	 * Force heap scan since the pg_class catalog table does not have an index on the
	 * relname column.
	 **/
	conscan = systable_beginscan(rel, NULL, false, NULL, 1, skey);

	/* There can be at most one matching row */
	tuple = systable_getnext(conscan);
	found = HeapTupleIsValid(tuple);

	if(found){
		/**
		 * Do a table select to find the indexed columns.
		 *
		 **/

		Oid ftw_indexes_oid = HeapTupleGetOid(tuple);
		systable_endscan(conscan);
		heap_close(rel, AccessShareLock);
		//Search for the table oid and index oid that match the ftw_relation


	}else{
		/**
		 * Throw error to warn user that there must be a table to store the index.
		 * Or we can assume that there are no indexes.
		 * In the current implementation we simply warn the user.
		 **/
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("Index table for  oblivious foreign tables")));

		systable_endscan(conscan);
		heap_close(rel, AccessShareLock);
	}


}

void obliviousBeginForeignModify (ModifyTableState * mtstate,
			     ResultRelInfo * rinfo, List * fdw_private,
			     int subplan_index, int eflags)
{
	Relation	rel = rinfo->ri_RelationDesc;
	findForeignTableIndex(rel->rd_id, RelationGetRelatioName(rel));

}
