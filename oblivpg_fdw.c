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
#include "access/nbtree.h"
#include "catalog/catalog.h"

#include "postgres.h"
#include "access/xact.h"
#include "catalog/pg_namespace_d.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "executor/tuptable.h"
#include "nodes/nodes.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/restrictinfo.h"

#include <collectc/queue.h>

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

#include "ops.h"


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
PG_FUNCTION_INFO_V1(close_enclave);
PG_FUNCTION_INFO_V1(init_fsoe);
PG_FUNCTION_INFO_V1(load_blocks);
//PG_FUNCTION_INFO_V1(transverse_tree);


/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST	100.0

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_OBLIV_FDW_TOTAL_COST	100.0

/* Predefined max tuple size for sgx to copy the real tuple to*/
#define MAX_TUPLE_SIZE 300

#define ENCLAVE_LIB "/usr/local/lib/soe/libsoe.signed.so"


int opmode;

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


typedef struct BTQueueData{
	unsigned int level;
	BlockNumber bts_parent_blkno;
	OffsetNumber bts_offnum;
	BlockNumber bts_bn_entry;
} BTQueueData;

typedef BTQueueData *BTQData;

typedef struct TreeConfig{
	unsigned int levels;
	int* fanouts;
}TreeConfig;

typedef TreeConfig *TConfig;

//Assuming a default tree hight to allocate to fanouts. This is reallocated for trees with more levels.
#define DTHeight 3




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

//Helper function
static  int getindexColumn(Oid oTable);

static TConfig transverse_tree(Oid indexOID);

static void load_blocks_tree(Oid indexOid);
static void load_blocks_heap(Oid heapOid);


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

    Oid hashFunctionOID;
    Oid indexHandlerOID;
    //unsigned int indexedColumn;

    TupleDesc indexTupleDesc;
	FormData_pg_attribute attrDesc;

	//unsigned int tupleDescLength;
	unsigned int attrDescLength;

 	oid = PG_GETARG_OID(0); //Foreign table wrapper oid
    opmode = PG_GETARG_UINT32(1); // Test run or deployment

    status = SGX_SUCCESS;
    mappingMemoryContext = AllocSetContextCreate(CurrentMemoryContext, "Obliv Mapping Table",  ALLOCSET_DEFAULT_SIZES);
    oldContext = MemoryContextSwitchTo(mappingMemoryContext);
    mappingOid = get_relname_relid(OBLIV_MAPPING_TABLE_NAME, PG_PUBLIC_NAMESPACE);

    if (mappingOid != InvalidOid) {

        oblivMappingRel = heap_open(mappingOid, RowShareLock);
        
        oStatus = getOblivTableStatus(oid, oblivMappingRel);

        mirrorHeapTable = heap_open(oStatus.relTableMirrorId, NoLock);
        mirrorTableRelationName = RelationGetRelationName(mirrorHeapTable);

        mirrorIndexTable = index_open(oStatus.relIndexMirrorId, NoLock);
        mirrorIndexRelationName = RelationGetRelationName(mirrorIndexTable);


        /* Fetch the oid of the functions that manipulate the indexed 
        * columns data types. In the current prototype this is the
        * function used to hash a given value. The system's default functions
        * for the database datatypes are defined in fmgroids.h. This values
        * are also in the catalog table pg_proc.
        */

        //Only works with hash indexes with a single column.
        hashFunctionOID = mirrorIndexTable->rd_support[0];
        indexTupleDesc = RelationGetDescr(mirrorIndexTable);
        attrDesc = indexTupleDesc->attrs[0];
        //tupleDescLength = sizeof(struct tupleDesc);
        attrDescLength = sizeof(FormData_pg_attribute);
        indexHandlerOID = mirrorIndexTable->rd_amhandler;
        elog(DEBUG1, "rd_amhandler is %d ", indexHandlerOID);
        //Fetch the column number of the indexed tuple
        //indexedColumn = mirrorIndexTable->rd_index->indkey.values[0];

        setupOblivStatus(oStatus, mirrorTableRelationName, mirrorIndexRelationName, indexHandlerOID);

        elog(DEBUG1, "Initializing SOE");

        #ifndef UNSAFE
        status = initSOE(enclave_id, 
        		mirrorTableRelationName, 
        		mirrorIndexRelationName, 
        		oStatus.tableNBlocks, 
        		oStatus.indexNBlocks, 
        		oStatus.relTableMirrorId, 
        		oStatus.relIndexMirrorId,
        		(unsigned int) hashFunctionOID,
        		(unsigned int) indexHandlerOID,
        		(char*) &attrDesc,
        		attrDescLength);
        #else
        	initSOE(mirrorTableRelationName, 
        		mirrorIndexRelationName, 
        		oStatus.tableNBlocks, 
        		oStatus.indexNBlocks, 
        		oStatus.relTableMirrorId, 
        		oStatus.relIndexMirrorId,
        		(unsigned int) hashFunctionOID,
        		(unsigned int) indexHandlerOID,
        		(char*) &attrDesc,
        		attrDescLength);
        #endif
        if(status != SGX_SUCCESS){
        	elog(ERROR, "SOE initialization failed %d ", status);
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
	    	elog(ERROR, "Enclave was not created. Return error %d", status);
	    	sgx_destroy_enclave(enclave_id);
	    	PG_RETURN_INT32(status);

	    }

	    elog(DEBUG1, "Enclave successfully created");
	    PG_RETURN_INT32(status);
	#else
    	PG_RETURN_INT32(0);
    #endif
}


TConfig transverse_tree(Oid indexOID) {
	Relation irel;
	BTQData queue_data = NULL;
	void *qblock;
	Buffer bufp;
	int queue_stat;
	Queue  *queue;
	bool isroot = true;
	unsigned int max_height = 0;
	unsigned int cb_height = 0;
	unsigned int nblocks_level = 0;
	unsigned int level_offset = 0;
	unsigned int nblocks_level_next = 0;
	TConfig result;
	//int *levels = (int*) malloc(sizeof(int));

	result = (TConfig) palloc(sizeof(struct TreeConfig));
	result->fanouts = (int*) palloc(sizeof(int)*DTHeight);


	irel = index_open(indexOID, ExclusiveLock);

	//elog(DEBUG1, "Going to create queue");

	queue_stat = queue_new(&queue);

	if(queue_stat != CC_OK){
		// TODO: Log error and abort.
        elog(ERROR, " queue initialization failed");
	} 
	//elog(DEBUG1, "Going to get root oid %d", indexOID);
	/*Get the root page to start with */
	bufp = _bt_getroot(irel, BT_READ);

	//elog(DEBUG1, "Root is in buffer %d", bufp);
	//The three has not been created and does not have a root
	//if(!BufferIsValid(*bufp))
		/**/


	queue_data = (BTQData) palloc(sizeof(BTQueueData));
	queue_data->bts_parent_blkno = InvalidBlockNumber; //IS ROOT
	// the root is not the offset of any other block.
	queue_data->bts_offnum = InvalidOffsetNumber; 
	queue_data->bts_bn_entry = 0;// We consider root to be on the first block.
	queue_data->level = 0;

	queue_enqueue(queue, queue_data);


	//Breadth first tree transversal
	while(queue_poll(queue, &qblock) != CC_ERR_OUT_OF_RANGE){
		Page page;
		//current queue (cq) data
		BTQData cq_data = NULL;
		BTPageOpaque opaque;
		OffsetNumber offnum;
		ItemId itemid;
		IndexTuple itup;
		BlockNumber blkno;
		BlockNumber par_blkno;
		OffsetNumber low,
					 high;


		BTQData cblock = (BTQData) qblock;

		blkno = cblock->bts_bn_entry;

		//ITS NOT A ROOT BLOCK
		if(!isroot){
			bufp = ReadBuffer(irel, cblock->bts_bn_entry);
		}

		page = BufferGetPage(bufp);
		opaque = (BTPageOpaque) PageGetSpecialPointer(page);
		blkno = BufferGetBlockNumber(bufp);
		low = P_FIRSTDATAKEY(opaque);
		high = PageGetMaxOffsetNumber(page);

		//elog(DEBUG1, "Page in blkno %d is root %d and next is %d", blkno, P_ISROOT(opaque), opaque->btpo_next);
		//elog(DEBUG1, "Page in blkno %d is leaf %d", blkno, P_ISLEAF(opaque));
		//elog(DEBUG1, "low is %d and high is %d", low, high);
		/*print the number of items in the page*/
		// If there are no keys on the page, meaning there are tuples.
		//if(high < low){
			/*TODO*/
		//}
		//height+=1;
		
		par_blkno = BufferGetBlockNumber(bufp);
		offnum = low;
		if(!P_ISLEAF(opaque)){
			while(offnum <= high){
				//push elements to the stack  to be transversed on the next loop iteration.
				// Get page offset on disk.

				itemid = PageGetItemId(page, offnum);
				itup = (IndexTuple) PageGetItem(page, itemid);
				blkno = BTreeInnerTupleGetDownLink(itup);

				cq_data = (BTQData) palloc(sizeof(BTQueueData));
				cq_data->bts_offnum = offnum;
				cq_data->bts_bn_entry = blkno;
				cq_data->bts_parent_blkno = par_blkno;
				//elog(DEBUG1, "Parent %d - child offset %d with blkno %d", par_blkno, offnum, blkno);
				//cq_data->level = height;
				queue_enqueue(queue, cq_data);
				offnum = OffsetNumberNext(offnum);
			}
		}
		if(P_ISROOT(opaque)){
			nblocks_level = high-low+1;
			level_offset = 0;
			cb_height +=1;
			isroot = false;
			max_height = Max(max_height, cb_height);
			result->fanouts[0] = nblocks_level;

		}else{
			//elog(DEBUG1, "1-level offset is  %d, nblocks_level %d, nblocks_level_next %d", level_offset, nblocks_level, nblocks_level_next);
			if(level_offset == nblocks_level-1){
				if(!P_ISLEAF(opaque)){
					nblocks_level_next += (high-low+1);
					if(cb_height > DTHeight){
						result->fanouts = (int*) realloc(result->fanouts, sizeof(int)*cb_height);
					}
					result->fanouts[cb_height] = nblocks_level_next;
				}
				nblocks_level = nblocks_level_next;
				nblocks_level_next = 0;
				cb_height += 1;
				max_height = Max(max_height, cb_height);
				level_offset = 0;
			}else{
				level_offset++;
				if(!P_ISLEAF(opaque)){
					nblocks_level_next += (high-low+1);
				}
			}			
			//elog(DEBUG1, "2-level offset is  %d, nblocks_level %d, nblocks_level_next %d", level_offset, nblocks_level, nblocks_level_next);

		}
		//Selog(DEBUG1, "transver_tree Height is %d ",max_height);

		pfree(qblock);
		ReleaseBuffer(bufp);
	}

	queue_destroy(queue);
	index_close(irel, ExclusiveLock);
	result->levels = max_height-1;
	return result;
}


Datum init_fsoe(PG_FUNCTION_ARGS){

	Oid oid;
	Oid mappingOid;
	Oid realIndexOid;
	sgx_status_t status;

	MemoryContext mappingMemoryContext;
    MemoryContext oldContext;

    Relation oblivMappingRel;
    Relation mirrorHeapTable;
    Relation mirrorIndexTable;

    FdwOblivTableStatus oStatus;
    char* mirrorTableRelationName;
    char* mirrorIndexRelationName;

//    Oid hashFunctionOID;
    Oid indexHandlerOID;

    TupleDesc indexTupleDesc;
	FormData_pg_attribute attrDesc;

	unsigned int attrDescLength;

	unsigned int tci;
	TConfig config;

 	oid = PG_GETARG_OID(0); //Foreign table wrapper oid
    opmode = PG_GETARG_UINT32(1); // Test run or deployment
    realIndexOid = PG_GETARG_OID(2); // The Oid of the real index to load the blocks to the mirror tables.

    status = SGX_SUCCESS;
    //mappingMemoryContext = AllocSetContextCreate(CurrentMemoryContext, "Obliv Mapping Table",  ALLOCSET_DEFAULT_SIZES);
    //oldContext = MemoryContextSwitchTo(mappingMemoryContext);
    mappingOid = get_relname_relid(OBLIV_MAPPING_TABLE_NAME, PG_PUBLIC_NAMESPACE);

	if (mappingOid != InvalidOid) {

        oblivMappingRel = heap_open(mappingOid, RowShareLock);
        
        oStatus = getOblivTableStatus(oid, oblivMappingRel);

        mirrorHeapTable = heap_open(oStatus.relTableMirrorId, NoLock);
        mirrorTableRelationName = RelationGetRelationName(mirrorHeapTable);

        mirrorIndexTable = index_open(oStatus.relIndexMirrorId, NoLock);
        mirrorIndexRelationName = RelationGetRelationName(mirrorIndexTable);


        /* Fetch the oid of the functions that manipulate the indexed 
        * columns data types. In the current prototype this is the
        * function used to hash a given value. The system's default functions
        * for the database datatypes are defined in fmgroids.h. This values
        * are also in the catalog table pg_proc.
        */

        //Only works with hash indexes with a single column.
//        hashFunctionOID = mirrorIndexTable->rd_support[0];
        indexTupleDesc = RelationGetDescr(mirrorIndexTable);
        attrDesc = indexTupleDesc->attrs[0];
        //tupleDescLength = sizeof(struct tupleDesc);
        attrDescLength = sizeof(FormData_pg_attribute);
        indexHandlerOID = mirrorIndexTable->rd_amhandler;
        //elog(DEBUG1, "rd_amhandler is %d ", indexHandlerOID);
        //Fetch the column number of the indexed tuple
        //indexedColumn = mirrorIndexTable->rd_index->indkey.values[0];

        setupOblivStatus(oStatus, mirrorTableRelationName, mirrorIndexRelationName, indexHandlerOID);

        //elog(DEBUG1, "Initializing SOE");
        config = transverse_tree(realIndexOid);
        /*elog(DEBUG1, "TConfig has nlevels %d",config->levels);
        for(tci = 0; tci < config->levels; tci++){
        	elog(DEBUG1, "level %d has fanout %d", tci, config->fanouts[tci]);
        }*/
        #ifndef UNSAFE
        status = initFSOE(enclave_id, 
        		mirrorTableRelationName, 
        		mirrorIndexRelationName, 
        		oStatus.tableNBlocks, 
        		config->fanouts,
        		config->levels,
        		oStatus.relTableMirrorId, 
        		oStatus.relIndexMirrorId,
        		(char*) &attrDesc,
        		attrDescLength);
        #else
        	initFSOE(mirrorTableRelationName, 
        		mirrorIndexRelationName, 
        		oStatus.tableNBlocks, 
        		config->fanouts,
        		config->levels,
        		oStatus.relTableMirrorId, 
        		oStatus.relIndexMirrorId,
        		(char*) &attrDesc,
        		attrDescLength);
        #endif
        if(status != SGX_SUCCESS){
        	elog(ERROR, "SOE initialization failed %d ", status);
        }

        heap_close(mirrorHeapTable, NoLock);
        index_close(mirrorIndexTable, NoLock);
        heap_close(oblivMappingRel, RowShareLock);

    }
    //MemoryContextSwitchTo(oldContext);
    //MemoryContextDelete(mappingMemoryContext);
    //print_status();

    PG_RETURN_INT32(0);
}

Datum load_blocks(PG_FUNCTION_ARGS){
	//print_status();
	Oid ioid = PG_GETARG_OID(0);
	Oid toid = PG_GETARG_OID(1);
	//elog(DEBUG1, "Requested to load blocks for index %d and table %d", ioid, toid);
	//print_status();
	//elog(DEBUG1,"Initializing oblivious tree construction");
	load_blocks_tree(ioid);
	//elog(DEBUG1, "Initializing oblivious heap table");
	load_blocks_heap(toid);
	PG_RETURN_INT32(0);
}


void load_blocks_heap(Oid toid){
	Relation rel;
	BlockNumber npages;
	BlockNumber blkno;
	Buffer buffer;
	Page page;
	//uint16 ps_size;
	PageHeader	phdr;

	rel = heap_open(toid, ExclusiveLock);
	npages = RelationGetNumberOfBlocks(rel);
	for(blkno = 0; blkno < npages;blkno++){
		buffer = ReadBuffer(rel, blkno);
		if(BufferIsValid(buffer)){
			page = BufferGetPage(buffer);
			phdr = (PageHeader) page;

			//ps_size = PageGetSpecialSize(page);
			//elog(DEBUG1, "Page special size is %d", phdr->pd_prune_xid);
			// storing on blkno on page header as it is not used by postgres engine.
			phdr->pd_prune_xid = blkno;
			//elog(DEBUG1, "Page special is now %d", phdr->pd_prune_xid);

			addHeapBlock(page, BLCKSZ, blkno);
		}else{
			elog(ERROR, "Buffer is invalid %d", blkno);
		}
		ReleaseBuffer(buffer);
	}
	heap_close(rel, ExclusiveLock);
}


void load_blocks_tree(Oid indexOID){
	Relation irel;
	BTQData queue_data = NULL;
	void *qblock;
	Buffer bufp;
	int queue_stat;
	Queue  *queue;
	bool isroot = true;
	unsigned int max_height = 0;
	unsigned int cb_height = 0;
	unsigned int nblocks_level = 0;
	unsigned int level_offset = 0;
	unsigned int nblocks_level_next = 0;

	irel = index_open(indexOID, ExclusiveLock);

	//elog(DEBUG1, "Going to create queue");

	queue_stat = queue_new(&queue);

	if(queue_stat != CC_OK){
		// TODO: Log error and abort.
        elog(ERROR, " queue initialization failed");
	} 
	//elog(DEBUG1, "Going to get root oid %d", indexOID);
	/*Get the root page to start with */
	bufp = _bt_getroot(irel, BT_READ);

	//elog(DEBUG1, "Root is in buffer %d", bufp);
	//The three has not been created and does not have a root
	//if(!BufferIsValid(*bufp))
		/**/


	queue_data = (BTQData) palloc(sizeof(BTQueueData));
	queue_data->bts_parent_blkno = InvalidBlockNumber; //IS ROOT
	// the root is not the offset of any other block.
	queue_data->bts_offnum = InvalidOffsetNumber; 
	queue_data->bts_bn_entry = 0;// We consider root to be on the first block.
	queue_data->level = 0;

	queue_enqueue(queue, queue_data);


	//Breadth first tree transversal
	while(queue_poll(queue, &qblock) != CC_ERR_OUT_OF_RANGE){
		Page page;
		//current queue (cq) data
		BTQData cq_data = NULL;
		BTPageOpaque opaque;
		OffsetNumber offnum;
		ItemId itemid;
		IndexTuple itup;
		BlockNumber blkno;
		BlockNumber par_blkno;
		OffsetNumber low,
					 high;
		//target block
		BlockNumber tblock = nblocks_level_next;


		BTQData cblock = (BTQData) qblock;

		blkno = cblock->bts_bn_entry;

		//ITS NOT A ROOT BLOCK
		if(!isroot){
			bufp = ReadBuffer(irel, cblock->bts_bn_entry);
		}

		page = BufferGetPage(bufp);
		opaque = (BTPageOpaque) PageGetSpecialPointer(page);
		blkno = BufferGetBlockNumber(bufp);
		low = P_FIRSTDATAKEY(opaque);
		high = PageGetMaxOffsetNumber(page);
		
		//elog(DEBUG1, "Page in blkno %d is root %d and  prev is %d and next is %d", blkno, P_ISROOT(opaque),opaque->btpo_prev, opaque->btpo_next);
		if(opaque->btpo_prev != P_NONE){
			opaque->btpo_prev = level_offset-1;
		}

		if(opaque->btpo_next != P_NONE){
			opaque->btpo_next = level_offset+1;
		}

		//elog(DEBUG1, "Page in blkno %d is root %d and  prev is %d and next is %d", blkno, P_ISROOT(opaque),opaque->btpo_prev, opaque->btpo_next);
		//elog(DEBUG1, "Page in blkno %d is root %d and  prev is %d and next is %d", blkno, P_ISROOT(opaque),opaque->btpo_prev, opaque->btpo_next);
		//elog(DEBUG1, "Page in blkno %d is leaf %d", blkno, P_ISLEAF(opaque));
		//elog(DEBUG1, "low is %d and high is %d", low, high);
		/*print the number of items in the page*/
		// If there are no keys on the page, meaning there are tuples.
		//if(high < low){
			/*TODO*/
		//}
		//height+=1;
		
		par_blkno = BufferGetBlockNumber(bufp);
		offnum = low;
		if(!P_ISLEAF(opaque)){
			while(offnum <= high){
				//push elements to the stack  to be transversed on the next loop iteration.
				// Get page offset on disk.

				itemid = PageGetItemId(page, offnum);
				itup = (IndexTuple) PageGetItem(page, itemid);
				blkno = BTreeInnerTupleGetDownLink(itup);
				//elog(DEBUG1, "updating chiild pointer from block %d to block number %d", blkno, tblock);
				BTreeInnerTupleSetDownLink(itup, tblock);
				cq_data = (BTQData) palloc(sizeof(BTQueueData));
				cq_data->bts_offnum = offnum;
				cq_data->bts_bn_entry = blkno;
				cq_data->bts_parent_blkno = par_blkno;
				//elog(DEBUG1, "Parent %d - child offset %d with blkno %d updated to %d", par_blkno, offnum, blkno, tblock);
				//cq_data->level = height;
				queue_enqueue(queue, cq_data);
				offnum = OffsetNumberNext(offnum);
				tblock +=1; 
				
			}
		}
		//elog(DEBUG1, "Moving block %d to height %d and offset %d", par_blkno, max_height, level_offset);
		addIndexBlock(page, BLCKSZ, level_offset, max_height);
		if(P_ISROOT(opaque)){
			nblocks_level = high-low+1;
			level_offset = 0;
			cb_height +=1;
			isroot = false;
			max_height = Max(max_height, cb_height);

		}else{
			//elog(DEBUG1, "1-level offset is  %d, nblocks_level %d, nblocks_level_next %d", level_offset, nblocks_level, nblocks_level_next);
			if(level_offset == nblocks_level-1){
				if(!P_ISLEAF(opaque)){
					nblocks_level_next += (high-low+1);
				}
				nblocks_level = nblocks_level_next;
				nblocks_level_next = 0;
				cb_height += 1;
				max_height = Max(max_height, cb_height);
				level_offset = 0;
			}else{
				level_offset++;
				if(!P_ISLEAF(opaque)){
					nblocks_level_next += (high-low+1);
				}
			}			
			//elog(DEBUG1, "2-level offset is  %d, nblocks_level %d, nblocks_level_next %d", level_offset, nblocks_level, nblocks_level_next);

		}
		//elog(DEBUG1, "Height is %d", cb_height);
		pfree(qblock);
		ReleaseBuffer(bufp);
	}


	queue_destroy(queue);
	index_close(irel, ExclusiveLock);
}

Datum close_enclave(PG_FUNCTION_ARGS) {

	#ifndef UNSAFE
		sgx_status_t status;
		status = sgx_destroy_enclave(enclave_id);

		if(SGX_SUCCESS != status){
			elog(ERROR, "Enclave was not destroyed. Return error %d", status);
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
	/*
	* TODO: A future implementation might iterate over the scan_clauses 
	* list and filter any clause that is not going to be processed by the fdw.
	* The current prototype assumes simple queries with a single clause with
	* the following syntax:
	*  ... where colname op value
	*/
	scan_clauses = extract_actual_clauses(scan_clauses,
										 false); /* extract regular clauses */
	
	foreignScan = make_foreignscan(tlist, scan_clauses, baserel->relid, NIL,NIL, NIL, NIL, NULL) ;

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
	 *  For now, this code follows a similar logic as the sequential scan on the postgres code
	 *  (nodeSeqscan.c -> ExecInitSeqScan).
	 */

	OblivScanState *fsstate;
	Relation oblivFDWTable;
	//Ostatus	obliv_status;

	FdwOblivTableStatus oStatus;
	Relation oblivMappingRel;
	Oid mappingOid;
	List* scan_clauses;


	ListCell *l;
	Oid	opno;
	Datum scanValue;
	Expr *clause;
	Expr *leftop;		/* expr on lhs of operator */
	Expr *rightop;	/* expr on rhs ... */
	//AttrNumber	varattno;	/* att number used in scan */

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	oblivFDWTable = node->ss.ss_currentRelation;

	mappingOid = get_relname_relid(OBLIV_MAPPING_TABLE_NAME, PG_PUBLIC_NAMESPACE);

	if (mappingOid != InvalidOid){
		//List of qualifier that will be evaluated by the fdw.
		scan_clauses = ((ForeignScan *) node->ss.ps.plan)->scan.plan.qual;

		fsstate = (OblivScanState *) palloc0(sizeof(OblivScanState));

		/**
		 * The logic to parse and obtain the necessary scan clauses values follows
		 * the function create_indescan_plan(createplan.c) and the 
		 * ExecIndexBuildScanKeys(nodeIndexscan.c).
		 *
		**/
		//For the prototype we are assuming a where clause with a single operator.
		foreach(l, scan_clauses){
			clause = lfirst(l);
			if(IsA(clause, OpExpr)){
				//elog(DEBUG1, "Operation expression");
				opno = ((OpExpr *) clause)->opno;
				//opfuncid = ((OpExpr *) clause)->opfuncid;

				leftop = (Expr *) get_leftop(clause);
				
				if (leftop && IsA(leftop, RelabelType))
					leftop = ((RelabelType *) leftop)->arg;

				//varattno = ((Var *) leftop)->varattno;

				rightop = (Expr *) get_rightop(clause);

				if(IsA(rightop, Const)){
	 				scanValue = ((Const *) rightop)->constvalue;
	 				fsstate->searchValue = VARDATA_ANY(DatumGetBpCharPP(scanValue));
					fsstate->searchValueSize = bpchartruelen(VARDATA_ANY(DatumGetBpCharPP(scanValue)), VARSIZE_ANY_EXHDR(DatumGetBpCharPP(scanValue)));
				}
				fsstate->opno = opno;
			}else{
		    	elog(ERROR, "Expression not supported");

			}
		}


		oblivMappingRel = heap_open(mappingOid, AccessShareLock);
		oStatus = getOblivTableStatus(oblivFDWTable->rd_id, oblivMappingRel);
		oStatus.tableRelFileNode = oblivFDWTable->rd_id;
		validateIndexStatus(oStatus);

		//elog(DEBUG1, "initializing fsstate %d", obliv_status);

		node->fdw_state = (void *) fsstate;
		fsstate->tupleHeader = (HeapTupleHeader) palloc(MAX_TUPLE_SIZE);
		memset(fsstate->tupleHeader, 0, MAX_TUPLE_SIZE);
		fsstate->mirrorTable = heap_open(oStatus.relTableMirrorId,  AccessShareLock);
		fsstate->tableTupdesc = RelationGetDescr(fsstate->mirrorTable);
		heap_close(oblivMappingRel, AccessShareLock);
	}
}

static TupleTableSlot *
obliviousIterateForeignScan(ForeignScanState * node)
{
	int len;
	char* key;
	int rowFound;
	//int indexedColumn;
	OblivScanState* fsstate;
    TupleTableSlot *tupleSlot;

	fsstate = (OblivScanState *) node->fdw_state;;
	tupleSlot = node->ss.ss_ScanTupleSlot;

	//elog(DEBUG1, "Going to read tuple in function getTuple");
	if(opmode == PRODUCTION_MODE){
		key = fsstate->searchValue;
		len = fsstate->searchValueSize;
	}else{ 
		
		/* In test mode the key is not used, and the SOE does a 
		* sequential scan on the heap relation 
		*/
		key = NULL;
		len = 0;
	}

	//elog(DEBUG1, "Going to search key %s with size %d", key, len);
	/**
	* The real tuple header size is set inside of the enclave on the
	* HeapTupleData strut in the field t_len;
	*/

	#ifdef UNSAFE
		rowFound = getTupleOST(opmode, fsstate->opno, key, len, (char*) &(fsstate->tuple), sizeof(HeapTupleData), (char*) fsstate->tupleHeader, MAX_TUPLE_SIZE);
	#else
		getTupleOST(enclave_id, &rowFound, opmode, fsstate->opno, key, len , (char*) &(fsstate->tuple), sizeof(HeapTupleData), (char*) fsstate->tupleHeader, MAX_TUPLE_SIZE);
	#endif
	fsstate->tuple.t_data = fsstate->tupleHeader;

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
    heap_close(fsstate->mirrorTable, AccessShareLock);
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

}


int getindexColumn(Oid oTable){
	Oid	mappingOid;
    FdwOblivTableStatus oStatus;

    Relation oblivMappingRel;
    Relation  mirrorIndexTable;

    int indexedColumn;

	mappingOid = get_relname_relid(OBLIV_MAPPING_TABLE_NAME, PG_PUBLIC_NAMESPACE);

   oblivMappingRel = heap_open(mappingOid, RowShareLock);

	oStatus = getOblivTableStatus(oTable, oblivMappingRel);

    mirrorIndexTable = index_open(oStatus.relIndexMirrorId, NoLock);

    // the current prototype assumes a single indexed column
    indexedColumn = mirrorIndexTable->rd_index->indkey.values[0];

    index_close(mirrorIndexTable, NoLock);
    heap_close(oblivMappingRel, RowShareLock);

    return indexedColumn;
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

    int indexedColumn;
    Datum indexedValueDatum;
    bool isColumnNull;
    char* indexValue;
    int indexValueSize;

	resultRelInfo = NULL;
	resultRelationDesc = NULL;
	status = SGX_SUCCESS;

	//elog(DEBUG1, "In obliviousExecForeignInsert");

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

	

    if(opmode == TEST_MODE){

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

    	indexedColumn = getindexColumn(resultRelationDesc->rd_id);


		indexedValueDatum = heap_getattr(tuple, indexedColumn, RelationGetDescr(resultRelationDesc), &isColumnNull);

		/**
		 * Currently, for development, we are assuming that the indexed attribute 
		 * is a fixed size char (e.g.: char(50)). when data is encrypted on 
		 * the client side and sent to the server, its going to be a binary
		 * data type.  for the binary data type look to the functions 
		 * toast_raw_datum_size and byteane to understand how to handle and 
		 * get the size of the binary array.
		 */

		/* LER! TODO!
			Funções de calcular a hash para diferentes Data Types.

			O data type bytes calcula a hash na função hashvarlena.

			O data type varlen calcula a hash com o hashtext.

			O data type char de tamanho fixo (e.g. char(50)) calcula a hash com a função hashbpchar.

		*/

		indexValue = VARDATA_ANY(DatumGetBpCharPP(indexedValueDatum));
		indexValueSize = bpchartruelen(VARDATA_ANY(DatumGetBpCharPP(indexedValueDatum)), VARSIZE_ANY_EXHDR(DatumGetBpCharPP(indexedValueDatum)));
		//indexValue = DatumGetCString(indexedValueDatum);
		//indexValueSize =  strlen(indexValue)+1;

		//elog(DEBUG1, "Datum to index is %s and has size %d", indexValue, indexValueSize);
    	#ifdef UNSAFE
    		insert((char*) tuple->t_data, tuple->t_len, indexValue, indexValueSize);
    	#else
    		insert(enclave_id, (char*) tuple->t_data, tuple->t_len, indexValue, indexValueSize);

    	#endif

    }
    /*insertTuple(RelationGetRelationName(resultRelationDesc), (Item) tuple->t_data, tuple->t_len);*/

	//elog(DEBUG1, "out of obliviousExecForeignInsert");
	return slot;
}


