/*-------------------------------------------------------------------------
 *
 * obliv_index.c
 *	  code to create Oblivious POSTGRES index relations
 *
 *  Copyright (c) 2018-2019, HASLab
 *
 *
 * IDENTIFICATION
 * contrib/oblivpg_fdw/obliv_index.c
 *
 *
 * INTERFACE ROUTINES
 *		obliv_index_create()			- Create a cataloged index relation
 *
 *-------------------------------------------------------------------------
 */



#include "include/obliv_index.h"
#include "include/obliv_utils.h"

#include "access/amapi.h"
#include "access/htup_details.h"
#include "access/stratnum.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_class_d.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_d.h"
#include "commands/tablespace.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "storage/lockdefs.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"



/* non-export function prototypes */

static TupleDesc CustomConstructTupleDescriptor(Relation heapRelation,
							   IndexInfo * indexInfo,
							   List * indexColNames,
							   Oid accessMethodObjectId,
							   Oid * collationObjectId,
							   Oid * classObjectId);

Oid		   *get_index_oidvector(Oid mirrorIndex, Oid column);
List	   *ConstructIndexColNames(Oid mirrorIndexOid);
Oid			get_rel_relam(Oid relid);
TupleDesc	createIndexTupleDescriptor(Relation mirrorHeapRelation, Relation mirrorIndexRelation, FdwIndexTableStatus status);
Oid			getIndexType(Oid index_oid);

Relation
obliv_index_create(FdwIndexTableStatus status)
{

	/**
	 * obtain an unique file Oid in the database to use as the
	 * name and pointer of the physical index file.
	 *
	 */

	Relation	mirrorHeapRelation;
	Relation	mirrorIndexRelation;
	Relation	result;

	Oid			tableSpaceId;
	Oid			relFileNode;
	Oid			indexRelationId;
	Oid			mirrorNameSpace;

	TupleDesc	tupleDescription;

	char		relKind;
	char		relpersistence;

	char	   *mirrorIndexRelationName;
	char	   *oblivIndexRelationName;

	bool		shared_relation;
	bool		mapped_relation;

	/*
	 * The index table are going to be considered unlogged as it is not
	 * relevant for the prototype to recover from crashes. This is something
	 * to think about in the future. Furthermore, the relperstence option is
	 * irrelevant for this extension, it only modifies the behavior of the
	 * function for temporary table which is not the case being considered.
	 */
	char		relpersistance = RELPERSISTENCE_UNLOGGED;


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
	oelog(DEBUG1, "The Relation file node for the index is %d", indexRelationId);

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

	heap_close(result, NoLock);
	heap_close(mirrorHeapRelation, AccessShareLock);
	index_close(mirrorIndexRelation, AccessShareLock);
	pfree(oblivIndexRelationName);

	return result;
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
							   IndexInfo * indexInfo,
							   List * indexColNames,
							   Oid accessMethodObjectId,
							   Oid * collationObjectId,
							   Oid * classObjectId)
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
Oid *
get_index_oidvector(Oid mirrorIndex, Oid column)
{

	Relation	rel;
	ScanKeyData skey;
	bool		found;
	SysScanDesc scanDesc;
	HeapTuple	tuple;
	TupleDesc	tupleDesc;
	bool		isOidVectorNull;
	Datum		dOidVector;
	Oid		   *results = NULL;
	Snapshot	snapshot;


	if (column != Anum_pg_index_indcollation && column != Anum_pg_index_indclass)
	{
		return results;
	}

	rel = heap_open(IndexRelationId, AccessShareLock);
	tupleDesc = RelationGetDescr(rel);
	ScanKeyInit(&skey, Anum_pg_index_indexrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(mirrorIndex));
	/**
	 *  The pg_index catalog table has a btree index on the columns indexrelid.
	 *  This index can be used to iterate over the column names instead of forcing a full heap scan.
	 */

	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scanDesc = systable_beginscan(rel, IndexRelidIndexId, true, snapshot, 1, &skey);
	tuple = systable_getnext(scanDesc);
	found = HeapTupleIsValid(tuple);

	if (found)
	{
		dOidVector = heap_getattr(tuple, column, tupleDesc, &isOidVectorNull);
		if (!isOidVectorNull)
		{
			/*
			 * The file heap.c, function StorePartitionKey writes the opclass
			 * and collation oids on the proper catalog table.
			 *
			 * This information seems to be accessed by the server source code
			 * when necessary. The file gistutil.c, function gistproperty is
			 * an example on how this access can be made.
			 *
			 *
			 * TODO: Review the following code to ensure that it correctly
			 * accesses the information and if it can be optimized by  using
			 * the server internal cache. The file gistutil.c seems to prefer
			 * to use the internal cache.
			 *
			 */
			oidvector  *vector = (oidvector *) DatumGetPointer(dOidVector);

			results = vector->values;
		}
	}
	systable_endscan(scanDesc);
	heap_close(rel, AccessShareLock);
	UnregisterSnapshot(snapshot);
	return results;
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
List *
ConstructIndexColNames(Oid mirrorIndexOid)
{

	List	   *result = NIL;
	Relation	rel;
	ScanKeyData skey;
	SysScanDesc scanDesc;
	HeapTuple	tuple;
	TupleDesc	tupleDesc;
	Snapshot	snapshot;

	rel = heap_open(AttributeRelationId, AccessShareLock);
	tupleDesc = RelationGetDescr(rel);
	ScanKeyInit(&skey, Anum_pg_attribute_attrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(mirrorIndexOid));
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

	while (HeapTupleIsValid(tuple = systable_getnext(scanDesc)))
	{

		bool		isColumnNameNull;
		Datum		dColumnName;

		dColumnName = heap_getattr(tuple, Anum_pg_attribute_attname, tupleDesc, &isColumnNameNull);

		if (!isColumnNameNull)
			result = lappend(result, pstrdup(DatumGetCString(dColumnName)));

	}
	systable_endscan(scanDesc);
	heap_close(rel, AccessShareLock);
	UnregisterSnapshot(snapshot);
	return result;

}

/**
 *
 * This code follows the same pattern as internal postgres functions that access a catalog cache to obtain
 * information about a relation.
 *
 * Some examples are the functions get_relname or get_rel_namespace defined in the file lsyscache.
 *
 * All of these functions search on a internal cache that stores tuples of catalog tables to lower the overhead
 * of going to search on disk. If the requested value is not found, than it searches on disk and stores on the
 * cache for latter requests.
 *
 * This code must be tested to see if it works correctly.
 ***/
Oid
get_rel_relam(Oid relid)
{

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

/*This function should do the same as get_rel_relam. Test which is is faster*/
Oid
getIndexType(Oid index_oid)
{

	Relation	rel;
	Oid			accessMethodObjectId;

	rel = heap_open(index_oid, AccessShareLock);
	accessMethodObjectId = rel->rd_rel->relam;

	heap_close(rel, AccessShareLock);

	return accessMethodObjectId;
}


/**
 *
 * An example on how to construct a TupleDesc  can be found on the file index.c function index_create.
 * However, most of the parameters for the function are given by parsing process of issuing a query.
 * In this case the necessary arguments can be fetched from the catalog tables that store the information
 * of the mirror index and table.
 *
 **/
TupleDesc
createIndexTupleDescriptor(Relation mirrorHeapRelation, Relation mirrorIndexRelation, FdwIndexTableStatus status)
{

	IndexInfo  *mirrorIndexInfo;
	List	   *colNamesInfo;
	Oid			accessMethodObjectId;
	Oid		   *collationsIds;
	Oid		   *operatorsIds;
	TupleDesc	result;

	colNamesInfo = ConstructIndexColNames(status.relIndexMirrorId);
	accessMethodObjectId = get_rel_relam(status.relIndexMirrorId);
	collationsIds = get_index_oidvector(status.relIndexMirrorId, Anum_pg_index_indcollation);
	operatorsIds = get_index_oidvector(status.relIndexMirrorId, Anum_pg_index_indclass);

	mirrorIndexInfo = BuildIndexInfo(mirrorIndexRelation);

	result = CustomConstructTupleDescriptor(mirrorHeapRelation, mirrorIndexInfo, colNamesInfo, accessMethodObjectId, collationsIds, operatorsIds);

	list_free_deep(colNamesInfo);

	return result;

}
