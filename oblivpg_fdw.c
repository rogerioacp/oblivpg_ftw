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

#include <string.h>

#include "access/amapi.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/tupdesc.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index_d.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "commands/explain.h"
#include "commands/tablespace.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#define Anum_obl_ftw_oid 1
#define Anum_obl_mirror_table_oid 2
#define Anum_obl_mirror_index_oid 3
#define Anum_obl_mirror_index_am 4
#define Anum_obl_ftw_index_relfilenode 5


#define OBLIV_MAPPING_TABLE_NAME "obl_ftw"



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



typedef struct FdwIndexTableStatus{

	/**
	 * the mirror table relation Id.
	 **/
	Oid relMirrorId;

	/**
	 * The index mirror relation Id.
	 **/
	Oid relIndexMirrorId;

	/**
	 *  relam is the Oid that defines the type of the index.
	 *  It has the same name and follows the same semantics as the variable relam in
	 *  the catalog tablepg_class.h
	 *  This variable is used to check if it is a BTree, Hash ... and use the correct methods.
	 */
	Oid relam;
	/* identifier of physical storage file. Is null if it has not been created yet.*/
	Oid relfilenode;
}FdwIndexTableStatus;

/**
 * Postgres initialization function which is called immediately after the an 
 * extension is loaded. This function can latter be used to initialize SGX 
 * enclaves and set-up Remote attestation.
 */
/*void
_PG_init ()
{

}*/

/**
 * Postgres cleaning function which is called just before an extension is
 * unloaded from a server. This function can latter be used to close SGX 
 * enclaves and clean the final context.
 */
/*void
_PG_fini ()
{

}*/




PG_FUNCTION_INFO_V1 (oblivpg_fdw_handler);
PG_FUNCTION_INFO_V1 (oblivpg_fdw_validator);


/*
 * FDW callback routines
 */

/* Functions for scanning oblivious index and table */
static void obliviousGetForeignRelSize (PlannerInfo * root,
					RelOptInfo * baserel,
					Oid foreigntableid);
static void obliviousGetForeignPaths(PlannerInfo * root,
				      RelOptInfo * baserel,
				      Oid foreigntableid);
static ForeignScan *obliviousGetForeignPlan(PlannerInfo * root,
					     RelOptInfo * baserel,
					     Oid foreigntableid,
					     ForeignPath * best_path,
					     List * tlist,
					     List * scan_clauses,
					     Plan * outer_plan);
static void obliviousExplainForeignScan(ForeignScanState *scanState, ExplainState *explainState);
static void obliviousBeginForeignScan(ForeignScanState * node, int eflags);
static TupleTableSlot *obliviousIterateForeignScan(ForeignScanState * node);
static void obliviousReScanForeignScan(ForeignScanState * node);
static void obliviousEndForeignScan(ForeignScanState * node);
static bool obliviousAnalyzeForeignTable(Relation relation,
					  AcquireSampleRowsFunc * func,
					  BlockNumber * totalpages);
static bool obliviousIsForeignScanParallelSafe(PlannerInfo * root,
						RelOptInfo * rel,
						RangeTblEntry * rte);



/*Internal functions declarations*/
Oid getIndexType(Oid index_oid);
FdwIndexTableStatus getIndexStatus(Oid ftwOid, Oid mappingOid);
Oid GenerateNewRelFileNode(Oid tableSpaceId, char relpersistance);
Oid get_rel_relam(Oid relid);
char* generateOblivTableName(char* tableName);
TupleDesc createIndexTupleDescriptor(Relation mirrorHeapRelation, Relation mirrorIndexRelation, FdwIndexTableStatus status);
Relation obliv_index_create(FdwIndexTableStatus status);
List* ConstructIndexColNames(Oid mirrorIndexOid);
Oid* get_index_oidvector(Oid mirrorIndex, Oid column);

static TupleDesc
CustomConstructTupleDescriptor(Relation heapRelation,
						 IndexInfo *indexInfo,
						 List *indexColNames,
						 Oid accessMethodObjectId,
						 Oid *collationObjectId,
						 Oid *classObjectId);
/* Functions for updating foreign tables */


/**
 * Function used by postgres before issuing a table update. This function is 
 * used to initialize the necessary resources to have an oblivious heap 
 * and oblivious table
 */
static void obliviousBeginForeignModify (ModifyTableState * mtstate,
				  ResultRelInfo * rinfo, List * fdw_private,
				  int subplan_index, int eflags);

/*
 * Foreign-data wrapper handler function: return a structure with pointers
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
	//To Complete
	PG_RETURN_VOID();
}



Oid getIndexType(Oid index_oid){

	Relation	rel;
	Oid accessMethodObjectId;

	rel = heap_open(index_oid, AccessShareLock);
    accessMethodObjectId = rel->rd_rel->relam;

    heap_close(rel, AccessShareLock);

	return accessMethodObjectId;
}

FdwIndexTableStatus getIndexStatus(Oid ftwOid, Oid mappingOid){

	Relation rel;
	ScanKeyData skey;
	TupleDesc tupleDesc;
	HeapScanDesc scan;
	Snapshot snapshot;
	HeapTuple tuple;
	bool found;
	FdwIndexTableStatus  iStatus;
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

	if(found)
	{
		bool isMirrorTableNull, isMirrorIndexNull, isIndexAmNull, isIndexRfnNull;
		Datum dMirrorTableId, dMirrorIndexId, dIndexAm, dIndexRfn;

		dMirrorTableId = heap_getattr(tuple, Anum_obl_mirror_table_oid, tupleDesc, &isMirrorTableNull);
		dMirrorIndexId = heap_getattr(tuple, Anum_obl_mirror_index_oid, tupleDesc, &isMirrorIndexNull);
		dIndexAm = heap_getattr(tuple, Anum_obl_mirror_index_am, tupleDesc, &isIndexAmNull);
		dIndexRfn = heap_getattr(tuple, Anum_obl_ftw_index_relfilenode, tupleDesc, &isIndexRfnNull);


		if(!isMirrorTableNull)
			iStatus.relMirrorId = DatumGetObjectId(dMirrorTableId);
		if(!isMirrorIndexNull)
			iStatus.relIndexMirrorId = DatumGetObjectId(dMirrorIndexId);
		if(!isIndexAmNull)
			iStatus.relam = DatumGetObjectId(dIndexAm);
		if(!isIndexRfnNull)
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
				 errmsg("Index table for  oblivious foreign tables")));

	}


	heap_close(rel, AccessShareLock);
	UnregisterSnapshot(snapshot);

	return iStatus;

}


Relation
obliv_index_create(FdwIndexTableStatus status){

	/**
	 * obtain an unique file Oid in the database to use as the
	 * name and pointer of the physical index file.
	 *
	 */

	Relation mirrorHeapRelation;
	Relation mirrorIndexRelation;
	Relation result;

	Oid tableSpaceId;
	Oid relFileNode;
	Oid indexRelationId;
	Oid mirrorNameSpace;
	Oid mirrorTableSpaceId;

	TupleDesc tupleDescription;

	char relKind;
	char relpersistence;

	char* mirrorIndexRelationName;
	char* oblivIndexRelationName;

	bool shared_relation;
	bool mapped_relation;

	/*
	 * The index table are going to be considered unlogged as it is not relevant for the prototype
	 * to recover from crashes. This is something to think about in the future.
	 * Furthermore, the relperstence option is irrelevant for this extension, it only modifies the
	 * behavior of the function for temporary table which is not the case being considered.
	 */
	char relpersistance = RELPERSISTENCE_UNLOGGED;


	/**
	 * Tables can be stored in different directories or disk partitions to either increase the
	 * database size or take advantage of faster (RAMDisks) or slower disks. For now, oblivious
	 * tables and indexes are stored on the default table space, but can latter be changed to
	 * something user-defined.
	 *
	 * The relation persistence is a necessary argument, but is only relevant for temporary tables.
	 *
	 **/
	tableSpaceId = GetDefaultTablespace(relpersistance);

	indexRelationId = GenerateNewRelFileNode(tableSpaceId, relpersistance);

	/**
	 * RelFileNode is used in corner cases on the standard postgres code to assign physical
	 * storage Oid different from the relation  Oid. This happens when a user wants to move a table
	 * for instance. Thus, for the default case, it can be left unspecified.
	 *
	 * This parameter has nothing to do with the tables forks or segments.
	 *
	 * Forks are the names given to additional files that store metadata of a relation.
	 * Segments are the division of a relation in multiple files of 1GB each.
	 *
	 * Both of this cases are handled internally by the postgres storage manager.
	 * On the file md.c there are several places where these concepts are used.
	 * In particular, the internal function _mdfd_getseg handles the creation and access of
	 * a relation segments.
	 *
	 * More information about forks and segments can be found in https://www.postgresql.org/docs/current/storage-file-layout.html.
	 * https://www.postgresql.org/docs/current/storage-file-layout.html
	 **/

	 relFileNode = InvalidOid;

	mirrorHeapRelation = heap_open(status.relMirrorId, AccessShareLock);
	mirrorIndexRelation = index_open(status.relIndexMirrorId, AccessShareLock);

	mirrorIndexRelationName = get_rel_name(status.relIndexMirrorId);
	oblivIndexRelationName = generateOblivTableName(mirrorIndexRelationName);


	mirrorNameSpace = RelationGetNamespace(mirrorIndexRelation);
	tupleDescription = createIndexTupleDescriptor(mirrorHeapRelation, mirrorIndexRelation, status);
	relKind = RELKIND_INDEX;

	mirrorTableSpaceId = mirrorIndexRelation->rd_rel->reltablespace;
	relpersistence = mirrorIndexRelation->rd_rel->relpersistence;

	shared_relation = mirrorIndexRelation->rd_rel->relisshared;
	mapped_relation = RelationIsMapped(mirrorIndexRelation);


	result = heap_create(oblivIndexRelationName,
						 mirrorNameSpace,
						 tableSpaceId,
						 indexRelationId,
						 relFileNode,
						 tupleDescription,
						 relKind,
						 relpersistence,
						 shared_relation,
						 mapped_relation,
						 false);

	heap_close(mirrorHeapRelation, AccessShareLock);
	index_close(mirrorIndexRelation, AccessShareLock);
	pfree(oblivIndexRelationName);

	return result;
}


Oid GenerateNewRelFileNode(Oid tableSpaceId, char relpersistance){

	Relation pg_class;
	Oid result;


	pg_class = heap_open(RelationRelationId, RowExclusiveLock);

	result = GetNewRelFileNode(tableSpaceId, pg_class,  relpersistance);

	heap_close(pg_class, RowExclusiveLock);

	return result;
}

char* generateOblivTableName(char* tableName){
	char * resultingName;
	size_t nameLen = strlen(tableName);

	resultingName = palloc(sizeof(char)*(6 + nameLen + 1));
	memcpy(resultingName, "obliv_",6);
	memcpy(&resultingName[6], tableName, nameLen+1);

	return resultingName;
}



/**
 * This function is a copy from the non-exported function ConstructTupleDescriptor
 * on the file index.c It should only be used to create TupleDesc of index heap
 * relations. The copy should be kept consistent with the server code or find a
 * way of using the function defined on the server.
 *
 * For the creation of a TupleDesc for normal heap relations, follow
 * read the function DefineRelation on the file tablecmds.c
 *
 **/
static TupleDesc
CustomConstructTupleDescriptor(Relation heapRelation,
						 IndexInfo *indexInfo,
						 List *indexColNames,
						 Oid accessMethodObjectId,
						 Oid *collationObjectId,
						 Oid *classObjectId)
{
	int			numatts = indexInfo->ii_NumIndexAttrs;
	int			numkeyatts = indexInfo->ii_NumIndexKeyAttrs;
	ListCell   *colnames_item = list_head(indexColNames);
	ListCell   *indexpr_item = list_head(indexInfo->ii_Expressions);
	IndexAmRoutine *amroutine;
	TupleDesc	heapTupDesc;
	TupleDesc	indexTupDesc;
	int			natts;			/* #atts in heap rel --- for error checks */
	int			i;

	/* We need access to the index AM's API struct */
	amroutine = GetIndexAmRoutineByAmId(accessMethodObjectId, false);

	/* ... and to the table's tuple descriptor */
	heapTupDesc = RelationGetDescr(heapRelation);
	natts = RelationGetForm(heapRelation)->relnatts;

	/*
	 * allocate the new tuple descriptor
	 */
	indexTupDesc = CreateTemplateTupleDesc(numatts, false);

	/*
	 * For simple index columns, we copy the pg_attribute row from the parent
	 * relation and modify it as necessary.  For expressions we have to cons
	 * up a pg_attribute row the hard way.
	 */
	for (i = 0; i < numatts; i++)
	{
		AttrNumber	atnum = indexInfo->ii_IndexAttrNumbers[i];
		Form_pg_attribute to = TupleDescAttr(indexTupDesc, i);
		HeapTuple	tuple;
		Form_pg_type typeTup;
		Form_pg_opclass opclassTup;
		Oid			keyType;

		if (atnum != 0)
		{
			/* Simple index column */
			Form_pg_attribute from;

			if (atnum < 0)
			{
				/*
				 * here we are indexing on a system attribute (-1...-n)
				 */
				from = SystemAttributeDefinition(atnum,
												 heapRelation->rd_rel->relhasoids);
			}
			else
			{
				/*
				 * here we are indexing on a normal attribute (1...n)
				 */
				if (atnum > natts)	/* safety check */
					elog(ERROR, "invalid column number %d", atnum);
				from = TupleDescAttr(heapTupDesc,
									 AttrNumberGetAttrOffset(atnum));
			}

			/*
			 * now that we've determined the "from", let's copy the tuple desc
			 * data...
			 */
			memcpy(to, from, ATTRIBUTE_FIXED_PART_SIZE);

			/*
			 * Fix the stuff that should not be the same as the underlying
			 * attr
			 */
			to->attnum = i + 1;

			to->attstattarget = -1;
			to->attcacheoff = -1;
			to->attnotnull = false;
			to->atthasdef = false;
			to->atthasmissing = false;
			to->attidentity = '\0';
			to->attislocal = true;
			to->attinhcount = 0;
			to->attcollation = (i < numkeyatts) ?
				collationObjectId[i] : InvalidOid;
		}
		else
		{
			/* Expressional index */
			Node	   *indexkey;

			MemSet(to, 0, ATTRIBUTE_FIXED_PART_SIZE);

			if (indexpr_item == NULL)	/* shouldn't happen */
				elog(ERROR, "too few entries in indexprs list");
			indexkey = (Node *) lfirst(indexpr_item);
			indexpr_item = lnext(indexpr_item);

			/*
			 * Lookup the expression type in pg_type for the type length etc.
			 */
			keyType = exprType(indexkey);
			tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(keyType));
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for type %u", keyType);
			typeTup = (Form_pg_type) GETSTRUCT(tuple);

			/*
			 * Assign some of the attributes values. Leave the rest as 0.
			 */
			to->attnum = i + 1;
			to->atttypid = keyType;
			to->attlen = typeTup->typlen;
			to->attbyval = typeTup->typbyval;
			to->attstorage = typeTup->typstorage;
			to->attalign = typeTup->typalign;
			to->attstattarget = -1;
			to->attcacheoff = -1;
			to->atttypmod = exprTypmod(indexkey);
			to->attislocal = true;
			to->attcollation = (i < numkeyatts) ?
				collationObjectId[i] : InvalidOid;

			ReleaseSysCache(tuple);

			/*
			 * Make sure the expression yields a type that's safe to store in
			 * an index.  We need this defense because we have index opclasses
			 * for pseudo-types such as "record", and the actually stored type
			 * had better be safe; eg, a named composite type is okay, an
			 * anonymous record type is not.  The test is the same as for
			 * whether a table column is of a safe type (which is why we
			 * needn't check for the non-expression case).
			 */
			CheckAttributeType(NameStr(to->attname),
							   to->atttypid, to->attcollation,
							   NIL, false);
		}

		/*
		 * We do not yet have the correct relation OID for the index, so just
		 * set it invalid for now.  InitializeAttributeOids() will fix it
		 * later.
		 */
		to->attrelid = InvalidOid;

		/*
		 * Set the attribute name as specified by caller.
		 */
		if (colnames_item == NULL)	/* shouldn't happen */
			elog(ERROR, "too few entries in colnames list");
		namestrcpy(&to->attname, (const char *) lfirst(colnames_item));
		colnames_item = lnext(colnames_item);

		/*
		 * Check the opclass and index AM to see if either provides a keytype
		 * (overriding the attribute type).  Opclass (if exists) takes
		 * precedence.
		 */
		keyType = amroutine->amkeytype;

		/*
		 * Code below is concerned to the opclasses which are not used with
		 * the included columns.
		 */
		if (i < indexInfo->ii_NumIndexKeyAttrs)
		{
			tuple = SearchSysCache1(CLAOID, ObjectIdGetDatum(classObjectId[i]));
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for opclass %u",
					 classObjectId[i]);
			opclassTup = (Form_pg_opclass) GETSTRUCT(tuple);
			if (OidIsValid(opclassTup->opckeytype))
				keyType = opclassTup->opckeytype;

			/*
			 * If keytype is specified as ANYELEMENT, and opcintype is
			 * ANYARRAY, then the attribute type must be an array (else it'd
			 * not have matched this opclass); use its element type.
			 */
			if (keyType == ANYELEMENTOID && opclassTup->opcintype == ANYARRAYOID)
			{
				keyType = get_base_element_type(to->atttypid);
				if (!OidIsValid(keyType))
					elog(ERROR, "could not get element type of array type %u",
						 to->atttypid);
			}

			ReleaseSysCache(tuple);
		}

		/*
		 * If a key type different from the heap value is specified, update
		 * the type-related fields in the index tupdesc.
		 */
		if (OidIsValid(keyType) && keyType != to->atttypid)
		{
			tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(keyType));
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for type %u", keyType);
			typeTup = (Form_pg_type) GETSTRUCT(tuple);

			to->atttypid = keyType;
			to->atttypmod = -1;
			to->attlen = typeTup->typlen;
			to->attbyval = typeTup->typbyval;
			to->attalign = typeTup->typalign;
			to->attstorage = typeTup->typstorage;

			ReleaseSysCache(tuple);
		}
	}

	pfree(amroutine);

	return indexTupDesc;
}

/**
 *
 * An example on how to construct a TupleDesc  can be found on the file index.c function index_create.
 * However, most of the parameters for the function are given by parsing process of issuing a query.
 * In this case the necessary arguments can be fetched from the catalog tables that store the information
 * of the mirror index and table.
 *
 **/
TupleDesc createIndexTupleDescriptor(Relation mirrorHeapRelation, Relation mirrorIndexRelation, FdwIndexTableStatus status){

	IndexInfo* mirrorIndexInfo;
	List* colNamesInfo;
	Oid accessMethodObjectId;
	Oid* collationsIds;
	Oid* operatorsIds;
	TupleDesc result;

	colNamesInfo = ConstructIndexColNames(status.relIndexMirrorId);
	accessMethodObjectId = get_rel_relam(status.relIndexMirrorId);
	collationsIds = get_index_oidvector(status.relIndexMirrorId, Anum_pg_index_indcollation);
	operatorsIds = get_index_oidvector(status.relIndexMirrorId, Anum_pg_index_indclass);

	mirrorIndexInfo = BuildIndexInfo(mirrorIndexRelation);

	result = CustomConstructTupleDescriptor(mirrorHeapRelation, mirrorIndexInfo, colNamesInfo, accessMethodObjectId, collationsIds, operatorsIds);

	list_free_deep(colNamesInfo);

	return result;

}


/**
 * The names of the columns of any relation (tables, indexes) are stored on
 * the postgres catalog table pg_attribute.
 * This catalog table has the column **attrelid** which is a pointer to a table Oid and
 * that column attname that contains the respective column name.
 *
 * This function has to itereate over every column name of an index given its oid and store it
 * in a List to be returned as the resulting output.
 *
 *
 * This function allocates space for the resulting list and the string elements inside it.
 * The caller is responsible for freeing the elements allocated to prevent memory leakage.
 * The lists, its cells and elements can all be freed by the function list_free_deep defined
 * in list.c.
 *
 * It might be necessary to create Memory contexts, the code needs to be tested to be sure.
 * The file cstore_reader. has some examples that can be used to guide the creation of a memory context.
 **/
List* ConstructIndexColNames(Oid mirrorIndexOid){

	List *result = NIL;
	Relation rel;
	ScanKeyData skey;
	SysScanDesc scanDesc;
	HeapTuple tuple;
	TupleDesc tupleDesc;
	Snapshot snapshot;

	rel = heap_open(AttributeRelationId, AccessShareLock);
	tupleDesc = RelationGetDescr(rel);
	ScanKeyInit(&skey, Anum_pg_attribute_attrelid, BTEqualStrategyNumber, F_OIDEQ,  ObjectIdGetDatum(mirrorIndexOid));
	/**
	 *  The pg_attribute catalog table has a composed btree index on the columns attrelid, attname.
	 *  This index can be used to iterate over the column names instead of forcing a full heap scan.
	 *
	 *  Catalog tables indexes Oids are defined on the indexing.h file. For this
	 *  case we need the index defined by the macro AttributeRelidNameIndexId
	 *
	 *
	 *  ATENTION: Test if scanning a multicolumn index with a key that contains a single value is
	 *  correct.
	 *  It seems to be possible, the code in the file comment.c  function DeleteComments does something
	 *  similar.
	 **/
	snapshot = RegisterSnapshot(GetLatestSnapshot());

	scanDesc = systable_beginscan(rel, AttributeRelidNameIndexId, true, snapshot, 1, &skey);

	while(HeapTupleIsValid(tuple = systable_getnext(scanDesc))){

		bool isColumnNameNull;
		Datum dColumnName;

		dColumnName = heap_getattr(tuple, Anum_pg_attribute_attname, tupleDesc, &isColumnNameNull);

		if(!isColumnNameNull)
			result = lappend(result, pstrdup(DatumGetCString(dColumnName)));

	}

	heap_close(rel, AccessShareLock);
	return result;

}



/**
 *
 * This code follows the same pattern as internal postgres functions that access a catalog cache to obtain
 * information about a relation.
 *
 * Some examples are the functions d get_relname or get_rel_namespace defined in the file lsyscache.
 *
 * All of these functions search on a internal cache that stores tuples of catalog tables to lower the overhead
 * of going to search on disk. If the requested value is not found, than it searches on disk and stores on the
 * cache for latter requests.
 *
 * This code must be tested to see if it works correctly.
 ***/
Oid get_rel_relam(Oid relid){

	HeapTuple	tp;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
		Oid			result;

		result = reltup->relam;
		ReleaseSysCache(tp);
		return result;
	}
	else
		return InvalidOid;
}



/**
 *
 * The ConstructTupleDescriptor function requires the collation oids and operator classes for
 * the index columns. This information is stored on the collation table pg_index for the mirror index
 * relation as vector of oids.
 *
 * This function generalizes the access to both values by accepting as argument the number of the columns
 * in the pg_index table.
 *
 * Possible arguments for input column are Anum_pg_index_indcollation and Anum_pg_index_indclass
 *
 * Test if returning Oids are valid.
 **/
Oid* get_index_oidvector(Oid mirrorIndex, Oid column){

	Relation rel;
	ScanKeyData skey;
	bool found;
	SysScanDesc scanDesc;
	HeapTuple tuple;
	TupleDesc tupleDesc;
	bool isOidVectorNull;
	Datum dOidVector;
	Oid* results = NULL;
	Snapshot snapshot;


	if(column != Anum_pg_index_indcollation && column != Anum_pg_index_indclass){
			return results;
	}

	rel = heap_open(IndexRelationId, AccessShareLock);
	tupleDesc = RelationGetDescr(rel);
	ScanKeyInit(&skey, Anum_pg_index_indexrelid, BTEqualStrategyNumber, F_OIDEQ,  ObjectIdGetDatum(mirrorIndex));
	/**
	 *  The pg_index catalog table has a btree index on the columns indexrelid.
	 *  This index can be used to iterate over the column names instead of forcing a full heap scan.
	 */

	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scanDesc = systable_beginscan(rel, IndexIndrelidIndexId, true, snapshot, 1, &skey);
	tuple = systable_getnext(scanDesc);
	found = HeapTupleIsValid(tuple);
	heap_close(rel, AccessShareLock);

	if(found){
		dOidVector = heap_getattr(tuple, column, tupleDesc, &isOidVectorNull);
		if(!isOidVectorNull){
			/*
			 * The file heap.c, function StorePartitionKey writes the opclass and collation
			 * oids on the proper catalog table.
			 *
			 * This information seems to be accessed by the server source code when necessary.
			 * The file gistutil.c, function gistproperty is an example on how this access can be made.
			 *
			 *
			 * TODO: Review the following code to ensure that it correctly accesses the information
			 * and if it can be optimized by  using the server internal cache. The file gistutil.c seems
			 * to prefer to use the internal cache.
			 *
			 */
			oidvector* vector = (oidvector*) DatumGetPointer(dOidVector);
			results = vector->values;
		}
	}

	return results;

}

static void
obliviousGetForeignRelSize(PlannerInfo *root,
					       RelOptInfo *baserel,
					       Oid foreigntableid)
{

	//To complete
}

static void
obliviousGetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid)
{

	//To complete
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
	//To complete

	return NULL;
}

static void 
obliviousExplainForeignScan(ForeignScanState * node,
					 ExplainState * es)
{
	//To complete
}

static void 
obliviousBeginForeignScan(ForeignScanState * node, int eflags)
{
	//To complete
}

static TupleTableSlot *
obliviousIterateForeignScan(ForeignScanState * node)
{
	//To complete
	return NULL;
}

static void 
obliviousReScanForeignScan(ForeignScanState * node)
{
	//To complete
}
static void 
obliviousEndForeignScan(ForeignScanState * node)
{
	//To complete
}
static bool 
obliviousAnalyzeForeignTable(Relation relation,
					  AcquireSampleRowsFunc * func,
					  BlockNumber * totalpages)
{
	//To complete
	return false;	
}
static bool obliviousIsForeignScanParallelSafe(PlannerInfo * root,
						RelOptInfo * rel,
						RangeTblEntry * rte)
{
	return  false;
}



static void
obliviousBeginForeignModify (ModifyTableState * mtstate,
			     ResultRelInfo * rinfo, List * fdw_private,
			     int subplan_index, int eflags)
{
	Oid mappingOid;


	Relation rel = rinfo->ri_RelationDesc;
	mappingOid = get_relname_relid(OBLIV_MAPPING_TABLE_NAME, PG_CATALOG_NAMESPACE);
	FdwIndexTableStatus iStatus;

	Relation indexRelation;

	if(mappingOid != InvalidOid)
	{
		iStatus = getIndexStatus(rel->rd_id, mappingOid);
		//Index has not been created yet.
		if(iStatus.relfilenode == InvalidOid){

			indexRelation = obliv_index_create(iStatus);

		}else{
			//Index has been created.
		}

	}
	else
	{
		/*
		 * The database administrator should create a Mapping table which maps the oid of the
		 * foreign table to its mirror table counterpart. The mirror table is used by
		 * this extension to find a matching index and simulate it.
		 *
		 */
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("Mapping table %s does not exist in the database!",
								 OBLIV_MAPPING_TABLE_NAME)));
	}

}


