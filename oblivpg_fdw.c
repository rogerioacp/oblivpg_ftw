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


#include <string.h>

#include "include/obliv_status.h"
#include "include/obliv_index.h"

#include "postgres.h"
#include "catalog/pg_namespace_d.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"


/**
 *
 * The current postgres version does not support table indexes on foreign tables.
 * To overcome this limitation without modifying the server core source code, this extension assumes that the
 * database has a standard postgres table which mirrors the foreign table. Furthermore, this mirror table has at most
 * an index on a single column. This mirror table and its index will enable this extension to know what type of index and
 * which column should be indexed. Furthermore, the mirror table and mirror indexcan be used for functional testing purposes and benchmarking.
 *
 * The last part for this hack to work is the table OBLIV_MAPPING_TABLE_NAME that must be created by the user and
 * which will map foreign table Oids to its mirror tables Oids. From this information, the extension can visit the
 * catalog tables to learn the necessary information about the indexes.
 *
 * To read, write or modify the physical files of a table or an index the extension leverages the internal postgres
 * API. To open a descriptor of the files it is necessary the to have an associated Relation for either the index or the heap file.
 * This relation can be created with the method heap_create on the src/backend/catalog/heap.c file.
 * The relation itself requires a varying number of parameters, most of which can be obtained from the foreign table relation.
 * However, to create an index transparently for the foreign table, it is necessary to have a relation Oid (relid) which can be obtained
 * by the function GetNewRelFileNode. To have an idea on how to create the index transparently it is helpful to follow the
 * standard postgres flow to create a normal table index.
 *
 * It can be considered that the index creation flow of a postgres table starts in the IndexCmds source file in the function DefineIndex.
 * The next relevant function is on the src/backend/catalog/index.c file, index_create function.
 * From these functions, most of the necessary logic can be used and it just might be possible to build on top of it.
 *
 *
 */


/***
 *
 * The logic of a default insert operation starts on the executor node  nodeMofifyTable function ExecInsert.
 * Read this code to understand how insertions are done.
 *
 *
 */


/**
 * Postgres macro to ensure that the compiled object file is not loaded to
 * an incompatible server.
 */
#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Function declarations for extension loading and unloading */

extern void _PG_init(void);
extern void _PG_fini(void);

PG_FUNCTION_INFO_V1(oblivpg_fdw_handler);
PG_FUNCTION_INFO_V1(oblivpg_fdw_validator);
/**
 * Postgres initialization function which is called immediately after the an
 * extension is loaded. This function can latter be used to initialize SGX
 * enclaves and set-up Remote attestation.
 */
void
_PG_init()
{
	elog(DEBUG1, "In _PG_init");
}

/**
 * Postgres cleaning function which is called just before an extension is
 * unloaded from a server. This function can latter be used to close SGX
 * enclaves and clean the final context.
 */
void
_PG_fini()
{
	elog(DEBUG1, "In _PG_fini");

}






/*
 * FDW callback routines
 */

/* Functions for scanning oblivious index and table */
static void obliviousGetForeignRelSize(PlannerInfo * root,
						   RelOptInfo * baserel,
						   Oid foreigntableid);
static void obliviousGetForeignPaths(PlannerInfo * root,
						 RelOptInfo * baserel,
						 Oid foreigntableid);
static ForeignScan * obliviousGetForeignPlan(PlannerInfo * root,
											 RelOptInfo * baserel,
											 Oid foreigntableid,
											 ForeignPath * best_path,
											 List * tlist,
											 List * scan_clauses,
											 Plan * outer_plan);
static void obliviousExplainForeignScan(ForeignScanState * scanState, ExplainState * explainState);
static void obliviousBeginForeignScan(ForeignScanState * node, int eflags);
static TupleTableSlot * obliviousIterateForeignScan(ForeignScanState * node);
static void obliviousReScanForeignScan(ForeignScanState * node);
static void obliviousEndForeignScan(ForeignScanState * node);
static bool obliviousAnalyzeForeignTable(Relation relation,
							 AcquireSampleRowsFunc * func,
							 BlockNumber * totalpages);
static bool obliviousIsForeignScanParallelSafe(PlannerInfo * root,
								   RelOptInfo * rel,
								   RangeTblEntry * rte);




/* Functions for updating foreign tables */


/**
 * Function used by postgres before issuing a table update. This function is
 * used to initialize the necessary resources to have an oblivious heap
 * and oblivious table
 */
static void obliviousBeginForeignModify(ModifyTableState * mtstate,
							ResultRelInfo * rinfo, List * fdw_private,
							int subplan_index, int eflags);

static TupleTableSlot *obliviousExecForeignInsert(EState * estate,
						   ResultRelInfo * rinfo,
						   TupleTableSlot * slot,
						   TupleTableSlot * planSlot);

/*
 * Foreign-data wrapper handler function: return a structure with pointers
 * to callback routines.
 */
Datum
oblivpg_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	/* Oblivious table scan functions */
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

	/* Oblivious insertion, update, deletion table functions */
	fdwroutine->BeginForeignModify = obliviousBeginForeignModify;
	fdwroutine->ExecForeignInsert = obliviousExecForeignInsert;

	PG_RETURN_POINTER(fdwroutine);
}


/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses file_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 *
 * This function has to be completed, follow the example on file_fdw.c.
 */
Datum
oblivpg_fdw_validator(PG_FUNCTION_ARGS)
{
	/* To Complete */
	PG_RETURN_VOID();
}








static void
obliviousGetForeignRelSize(PlannerInfo * root,
						   RelOptInfo * baserel,
						   Oid foreigntableid)
{

	/* To complete */
}

static void
obliviousGetForeignPaths(PlannerInfo * root,
						 RelOptInfo * baserel,
						 Oid foreigntableid)
{

	/* To complete */
}

static ForeignScan *
obliviousGetForeignPlan(PlannerInfo * root,
						RelOptInfo * baserel,
						Oid foreigntableid,
						ForeignPath * best_path,
						List * tlist,
						List * scan_clauses,
						Plan * outer_plan)
{
	/* To complete */

	return NULL;
}

static void
obliviousExplainForeignScan(ForeignScanState * node,
							ExplainState * es)
{
	/* To complete */
}

static void
obliviousBeginForeignScan(ForeignScanState * node, int eflags)
{
	/* To complete */
}

static TupleTableSlot *
obliviousIterateForeignScan(ForeignScanState * node)
{
	/* To complete */
	return NULL;
}

static void
obliviousReScanForeignScan(ForeignScanState * node)
{
	/* To complete */
}
static void
obliviousEndForeignScan(ForeignScanState * node)
{
	/* To complete */
}
static bool
obliviousAnalyzeForeignTable(Relation relation,
							 AcquireSampleRowsFunc * func,
							 BlockNumber * totalpages)
{
	/* To complete */
	return false;
}
static bool
obliviousIsForeignScanParallelSafe(PlannerInfo * root,
								   RelOptInfo * rel,
								   RangeTblEntry * rte)
{
	return false;
}



static void
obliviousBeginForeignModify(ModifyTableState * mtstate,
							ResultRelInfo * rinfo, List * fdw_private,
							int subplan_index, int eflags)
{

	elog(DEBUG1, "In obliviousBeginForeignModify");
	Oid			mappingOid;
	Ostatus		obliv_status;


	Relation	rel = rinfo->ri_RelationDesc;

	mappingOid = get_relname_relid(OBLIV_MAPPING_TABLE_NAME, PG_PUBLIC_NAMESPACE);
	FdwIndexTableStatus iStatus;

	Relation	indexRelation;

	if (mappingOid != InvalidOid)
	{
		iStatus = getIndexStatus(rel->rd_id, mappingOid);
		obliv_status = validateIndexStatus(iStatus);

		if (obliv_status == OBLIVIOUS_UNINTIALIZED)
		{
			elog(DEBUG1, "Index has not been created");

			/* indexRelation = obliv_index_create(iStatus); */

		}
		else if (obliv_status == OBLIVIOUS_INITIALIZED)
		{
			elog(DEBUG1, "Index has already been created");
			/* Index has been created. */
		}
		/**
		 *  If none of the above cases is valid, the record stored in
		 *  OBLIV_MAPPING_TABLE_NAME is invalid and an error message
		 *  has already been show to the user by the function
		 *  validateIndexStatus.
		 * */

	}
	else
	{
		/*
		 * The database administrator should create a Mapping table which maps
		 * the oid of the foreign table to its mirror table counterpart. The
		 * mirror table is used by this extension to find a matching index and
		 * simulate it.
		 *
		 */
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("Mapping table %s does not exist in the database!",
						OBLIV_MAPPING_TABLE_NAME)));
	}

}

static TupleTableSlot *
obliviousExecForeignInsert(EState * estate,
						   ResultRelInfo * rinfo,
						   TupleTableSlot * slot,
						   TupleTableSlot * planSlot)
{

	elog(DEBUG1, "In obliviousExecForeignInsert");
	return NULL;
}
