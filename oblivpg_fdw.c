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
#include "include/obliv_utils.h"
#include "include/oblivpg_fdw.h"
#include "include/obliv_ocalls.h"

#include "access/htup.h"
#include "access/htup_details.h"
#include "access/tuptoaster.h"
#include "catalog/catalog.h"

#include "postgres.h"
#include "access/xact.h"
#include "catalog/pg_namespace_d.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "executor/tuptable.h"

/**
 * SGX includes
 */

// Needed to create enclave and do ecall.
#ifndef UNSAFE
#include "sgx_urts.h"
#include "Enclave_u.h"
#else
#include "Enclave_dt.h"
#endif


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
 * The logic of a default insert operation starts on the executor node nodeMofifyTable function ExecInsert.
 * Read this code to understand how insertions are done.
 */


/**
 * Postgres macro to ensure that the compiled object file is not loaded to
 * an incompatible server.
 */
#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(oblivpg_fdw_handler);
PG_FUNCTION_INFO_V1(oblivpg_fdw_validator);


/* Function declarations for extension loading and unloading */

extern void _PG_init(void);
extern void _PG_fini(void);




PG_FUNCTION_INFO_V1(init_soe);
PG_FUNCTION_INFO_V1(log_special_pointer);
PG_FUNCTION_INFO_V1(open_enclave);
PG_FUNCTION_INFO_V1(set_next);
PG_FUNCTION_INFO_V1(close_enclave);


/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST	100.0

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_OBLIV_FDW_TOTAL_COST	100.0

/* Predefined max tuple size for sgx to copy the real tuple to*/
#define MAX_TUPLE_SIZE 200

#define ENCLAVE_LIB "/usr/local/lib/soe/libsoe.signed.so"

#define HEAP_ACCESS_TEST 1
#define INDEX_ACCESS_TEST 2

int test;
char* key = NULL;

#ifndef UNSAFE
sgx_enclave_id_t  enclave_id = 0;
#endif


/**
 * Postgres initialization function which is called immediately after the an
 * extension is loaded. This function can latter be used to initialize SGX
 * enclaves and set-up Remote attestation.
 *
 * This function is only called when an extension is first created. It is not
 * used for every database initialization.
 */
void
_PG_init()
{
}

/**
 * Postgres cleaning function which is called just before an extension is
 * unloaded from a server. This function can latter be used to close SGX
 * enclaves and clean the final context.
 */
void
_PG_fini(){}


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


Datum init_soe(PG_FUNCTION_ARGS){

    Oid oid;
    Oid	mappingOid;
	sgx_status_t status;

    MemoryContext mappingMemoryContext;
    MemoryContext oldContext;

    Relation oblivMappingRel;
    Relation mirrorHeapTable;
    Relation mirrorIndexTable;

    FdwOblivTableStatus oStatus;
    char* mirrorTableRelationName;
    char* mirrorIndexRelationName;

 	oid = PG_GETARG_OID(0); //Foreign table wrapper oid
    test = PG_GETARG_UINT32(1); // Test run or deployment

    status = SGX_SUCCESS;
    mappingMemoryContext = AllocSetContextCreate(CurrentMemoryContext, "Obliv Mapping Table",  ALLOCSET_DEFAULT_SIZES);
    oldContext = MemoryContextSwitchTo(mappingMemoryContext);
    mappingOid = get_relname_relid(OBLIV_MAPPING_TABLE_NAME, PG_PUBLIC_NAMESPACE);

    if (mappingOid != InvalidOid) {

        oblivMappingRel = heap_open(mappingOid, RowShareLock);
        elog(DEBUG1, "GetOblivTableStatus");
        
        oStatus = getOblivTableStatus(oid, oblivMappingRel);
        elog(DEBUG1, "Open Mirror relation table");

        mirrorHeapTable = heap_open(oStatus.relTableMirrorId, NoLock);
        mirrorTableRelationName = RelationGetRelationName(mirrorHeapTable);
        elog(DEBUG1, "Open Mirror index table");

        mirrorIndexTable = index_open(oStatus.relIndexMirrorId, NoLock);
        elog(DEBUG1, "Get index Name");
        mirrorIndexRelationName = RelationGetRelationName(mirrorIndexTable);

        setupOblivStatus(oStatus, mirrorTableRelationName, mirrorIndexRelationName);
        elog(DEBUG1, "Initializing SOE in enclave");

        #ifndef UNSAFE
        status = initSOE(enclave_id, 
        		mirrorTableRelationName, 
        		mirrorIndexRelationName, 
        		oStatus.tableNBlocks, 
        		oStatus.indexNBlocks, 
        		oStatus.relTableMirrorId, 
        		oStatus.relIndexMirrorId);
        #else
        	initSOE(mirrorTableRelationName, 
        		mirrorIndexRelationName, 
        		oStatus.tableNBlocks, 
        		oStatus.indexNBlocks, 
        		oStatus.relTableMirrorId, 
        		oStatus.relIndexMirrorId);
        #endif
        if(status != SGX_SUCCESS){
        	elog(DEBUG1, "SOE initialization failed");
        }

        heap_close(mirrorHeapTable, NoLock);
        index_close(mirrorIndexTable, NoLock);
        heap_close(oblivMappingRel, RowShareLock);

    }
    MemoryContextSwitchTo(oldContext);
    MemoryContextDelete(mappingMemoryContext);
    PG_RETURN_INT32(0);

}



Datum log_special_pointer(PG_FUNCTION_ARGS) {
	PG_RETURN_INT32(0);
}

Datum open_enclave(PG_FUNCTION_ARGS) {
	#ifndef UNSAFE
		sgx_status_t status;
		int token_update;
		sgx_launch_token_t token;

		token_update = 0;

		memset(&token, 0, sizeof(sgx_launch_token_t));

		status = sgx_create_enclave(ENCLAVE_LIB,
		 						  SGX_DEBUG_FLAG,
		 						  &token,
		 						  &token_update,
	                              &enclave_id, NULL);

	    if(SGX_SUCCESS != status)
	    {
	    	elog(DEBUG1, "Enclave was not created. Return error %d", status);
	    	sgx_destroy_enclave(enclave_id);
	    	PG_RETURN_INT32(status);

	    }

	    elog(DEBUG1, "Enclave successfully created");
	    PG_RETURN_INT32(status);
	#else
    	PG_RETURN_INT32(0);
    #endif
}


Datum set_next(PG_FUNCTION_ARGS) {
	
	key = (char*) palloc(strlen("NEXT")+1);
	memcpy(key, "NEXT", strlen("NEXT")+1);

	PG_RETURN_VOID();

}

Datum close_enclave(PG_FUNCTION_ARGS) {

	#ifndef UNSAFE
		sgx_status_t status;
		status = sgx_destroy_enclave(enclave_id);

		if(SGX_SUCCESS != status){
			elog(DEBUG1, "Enclave was not destroyed. Return error %d", status);
			PG_RETURN_INT32(status);
		}

		PG_RETURN_INT32(status);
   #else
		closeSoe();
		PG_RETURN_INT32(0);
   #endif
	closeOblivStatus();
	elog(DEBUG1, "Enclave destroyed");

}


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
	Path *path = NULL;
	Cost startup_cost = DEFAULT_FDW_STARTUP_COST;
	Cost total_cost = DEFAULT_OBLIV_FDW_TOTAL_COST;

	path = (Path *) create_foreignscan_path(root, baserel,
								   NULL,	/* default pathtarget */
								   baserel->rows,
								   startup_cost,
								   total_cost,
								   NIL, /* no pathkeys */
								   NULL,	/* no outer rel either */
								   NULL,	/* no extra plan */
								   NIL);	/* no fdw_private list */

	add_path(baserel, path);



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
	ForeignScan *foreignScan = NULL;

	foreignScan = make_foreignscan(tlist,  NIL, baserel->relid, NIL,NIL, NIL, NIL, NULL) ;

	return foreignScan;

}

static void
obliviousBeginForeignScan(ForeignScanState * node, int eflags)
{
	/**On the stream execution, this function should check if the necessary resources are initiated
	 *
	 *  enclave
	 *  constant rate thread
	 *
	 *
	 *  For now, this code follows the same logic as the sequential scan on the postgres code
	 *  (nodeSeqscan.c -> ExecInitSeqScan).
	 */

	OblivScanState *fsstate;
	Relation oblivFDWTable;
	Ostatus		obliv_status;

	FdwOblivTableStatus oStatus;
	Relation oblivMappingRel;
	Oid			mappingOid;
	//char* relationName;


	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	oblivFDWTable = node->ss.ss_currentRelation;

	mappingOid = get_relname_relid(OBLIV_MAPPING_TABLE_NAME, PG_PUBLIC_NAMESPACE);

	if (mappingOid != InvalidOid){
		oblivMappingRel = heap_open(mappingOid, AccessShareLock);
		oStatus = getOblivTableStatus(oblivFDWTable->rd_id, oblivMappingRel);
		oStatus.tableRelFileNode = oblivFDWTable->rd_id;
		obliv_status = validateIndexStatus(oStatus);

		//if(obliv_status == OBLIVIOUS_INITIALIZED){
		    elog(DEBUG1, "initializing fsstate %d", obliv_status);
			fsstate = (OblivScanState *) palloc0(sizeof(OblivScanState));
            fsstate->mirrorIndex = NULL;
            fsstate->mirrorTable = NULL;
            fsstate->indexTupdesc = NULL;
            fsstate->query = NULL;
            fsstate->working_cxt = NULL;
            fsstate->tupleHeader = (HeapTupleHeader) palloc(MAX_TUPLE_SIZE);
            memset(fsstate->tupleHeader, 0, MAX_TUPLE_SIZE);
            node->fdw_state = (void *) fsstate;
			//fsstate->table = heap_open(oStatus.relTableMirrorId,  AccessShareLock);
			//fsstate->index = heap_open(oStatus.indexRelFileNode, AccessShareLock);
			//fsstate->tableTupdesc = RelationGetDescr(node->ss.ss_currentRelation);
		//}
		heap_close(oblivMappingRel, AccessShareLock);
		/*relationName = RelationGetRelationName(oblivFDWTable);
		initSOE(relationName, (size_t) oStatus.tableNBlocks, 1);
		pfree(relationName);*/


	}
}

static TupleTableSlot *
obliviousIterateForeignScan(ForeignScanState * node)
{
	OblivScanState* fsstate = (OblivScanState *) node->fdw_state;
    TupleTableSlot *tupleSlot;

    int len;
	int rowFound = 0;
	

	tupleSlot = node->ss.ss_ScanTupleSlot;
	len = 5;


	elog(DEBUG1, "Going to read tuple in function getTuple");
	if(test == HEAP_ACCESS_TEST){

		/**
		* The real tuple header size is set inside of the enclave on the
		* HeapTupleData strut in the field t_len;
		*/
		#ifndef UNSAFE
		getTuple(enclave_id, &rowFound, key, len , (char*) &(fsstate->tuple), sizeof(HeapTupleData), (char*) fsstate->tupleHeader, MAX_TUPLE_SIZE);
		#else
		rowFound = getTuple(key, len, (char*) &(fsstate->tuple), sizeof(HeapTupleData), (char*) fsstate->tupleHeader, MAX_TUPLE_SIZE);
		#endif
		fsstate->tuple.t_data = fsstate->tupleHeader;
	}

	if (rowFound == 0)
	{
		ExecStoreTuple(&(fsstate->tuple), tupleSlot, InvalidBuffer, false);
	}else{
		//Reached the end of available tuples
        return ExecClearTuple(tupleSlot);
	}

	return tupleSlot;
}



static void
obliviousEndForeignScan(ForeignScanState * node)
{
    OblivScanState *fsstate;

    fsstate = (OblivScanState *) node->fdw_state;
    pfree(fsstate->tupleHeader);
    pfree(fsstate);
}


static void
obliviousExplainForeignScan(ForeignScanState * node,
							ExplainState * es)
{
	/* To complete */
}



static void
obliviousReScanForeignScan(ForeignScanState * node)
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

static void obliviousBeginForeignModify(ModifyTableState * mtstate,
                                        ResultRelInfo * rinfo, List * fdw_private,
                                        int subplan_index, int eflags)
                                        {

   // init_soe(rinfo->ri_RelationDesc->rd_id);
}

/**
 *  The logic of this function of accessing the relation, tuple and other information to store a tuple was
 *  obtained from the function  ExecInsert in nodeModifyTable.c
 */
static TupleTableSlot *
obliviousExecForeignInsert(EState * estate,
						   ResultRelInfo * rinfo,
						   TupleTableSlot * slot,
						   TupleTableSlot * planSlot)
{



	ResultRelInfo* resultRelInfo;
	Relation	resultRelationDesc;
	HeapTuple	tuple;
	TransactionId xid;
	sgx_status_t status;
	status = SGX_SUCCESS;
	resultRelInfo = NULL;
	resultRelationDesc = NULL;

	elog(DEBUG1, "In obliviousExecForeignInsert");

	/*
     * get the heap tuple out of the tuple table slot, making sure we have a
     * writable copy
     */
	tuple = ExecMaterializeSlot(slot);

	resultRelInfo = estate->es_result_relation_info;
	resultRelationDesc = resultRelInfo->ri_RelationDesc;
	xid = GetCurrentTransactionId();

    //The function heap_prepar_insert is copied from heapam.c as it is a private function.
	tuple = heap_prepare_insert(resultRelationDesc, tuple, xid, estate->es_output_cid, 0);

    //elog(DEBUG1, "Inserting tuple with ecall that has size %d", tuple->t_len);
    if(test == HEAP_ACCESS_TEST){

    	//elog(DEBUG1, "Heap Access Test with tuple of size %d", tuple->t_len);

    	#ifdef UNSAFE
    	   insertHeap((char*) tuple->t_data, tuple->t_len);
    	#else
    		status = insertHeap(enclave_id, (char*) tuple->t_data, tuple->t_len);
    	#endif

    	if(status != SGX_SUCCESS){
    		elog(DEBUG1, "tuple insertion on heap was not successful!");
    	}

    }else{

    	#ifdef UNSAFE
    		insert((char*) tuple->t_data, tuple->t_len);
    	#else
    		insert(enclave_id, (char*) tuple->t_data, tuple->t_len);

    	#endif

    }
    /*insertTuple(RelationGetRelationName(resultRelationDesc), (Item) tuple->t_data, tuple->t_len);*/

	elog(DEBUG1, "out of obliviousExecForeignInsert");
	return slot;
}