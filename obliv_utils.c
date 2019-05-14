/*-------------------------------------------------------------------------
 *
 * obliv_utils.c
 *	 utility code for the Oblivious POSTGRES table access
 *
 *  Copyright (c) 2018-2019, HASLab
 *
 *
 * IDENTIFICATION
 * contrib/oblivpg_fdw/obliv_utils.c
 *
 *
 * INTERFACE ROUTINES
 *		generateOblivTableName()			- generates a new table name
 *      GenerateNewRelFileNode()			- generates a new file node
 *-------------------------------------------------------------------------
 */



#include "include/obliv_utils.h"

#include "catalog/catalog.h"
#include "catalog/pg_class_d.h"
#include "storage/lockdefs.h"
#include "access/htup_details.h"
#include "utils/rel.h"
#include "access/tuptoaster.h"
#include "access/parallel.h"

/* The table name needs to be freed once it is not used. */
char *
generateOblivTableName(char *tableName)
{
	char	   *resultingName;
	size_t		nameLen = strlen(tableName);

	resultingName = palloc(sizeof(char) * (6 + nameLen + 1));
	memcpy(resultingName, "obliv_", 6);
	memcpy(&resultingName[6], tableName, nameLen + 1);

	return resultingName;
}



Oid
GenerateNewRelFileNode(Oid tableSpaceId, char relpersistance)
{

	Relation	pg_class;
	Oid			result;


	pg_class = heap_open(RelationRelationId, RowExclusiveLock);

	result = GetNewRelFileNode(tableSpaceId, pg_class, relpersistance);

	heap_close(pg_class, RowExclusiveLock);

	return result;
}


/*
 * Subroutine for heap_insert(). Prepares a tuple for insertion. This sets the
 * tuple header fields, assigns an OID, and toasts the tuple if necessary.
 * Returns a toasted version of the tuple if it was toasted, or the original
 * tuple if not. Note that in any case, the header fields are also set in
 * the original tuple.
 */
HeapTuple
heap_prepare_insert(Relation relation, HeapTuple tup, TransactionId xid, CommandId cid, int options){
	/*
     * Parallel operations are required to be strictly read-only in a parallel
     * worker.  Parallel inserts are not safe even in the leader in the
     * general case, because group locking means that heavyweight locks for
     * relation extension or GIN page locks will not conflict between members
     * of a lock group, but we don't prohibit that case here because there are
     * useful special cases that we can safely allow, such as CREATE TABLE AS.
     */
	if (IsParallelWorker())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
						errmsg("cannot insert tuples in a parallel worker")));

	if (relation->rd_rel->relhasoids)
	{
#ifdef NOT_USED
		/* this is redundant with an Assert in HeapTupleSetOid */
		Assert(tup->t_data->t_infomask & HEAP_HASOID);
#endif

		/*
         * If the object id of this tuple has already been assigned, trust the
         * caller.  There are a couple of ways this can happen.  At initial db
         * creation, the backend program sets oids for tuples. When we define
         * an index, we set the oid.  Finally, in the future, we may allow
         * users to set their own object ids in order to support a persistent
         * object store (objects need to contain pointers to one another).
         */
		if (!OidIsValid(HeapTupleGetOid(tup)))
			HeapTupleSetOid(tup, GetNewOid(relation));
	}
	else
	{
		/* check there is not space for an OID */
		Assert(!(tup->t_data->t_infomask & HEAP_HASOID));
	}

	tup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
	tup->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
	tup->t_data->t_infomask |= HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tup->t_data, xid);
	if (options & HEAP_INSERT_FROZEN)
		HeapTupleHeaderSetXminFrozen(tup->t_data);

	HeapTupleHeaderSetCmin(tup->t_data, cid);
	HeapTupleHeaderSetXmax(tup->t_data, 0); /* for cleanliness */
	tup->t_tableOid = RelationGetRelid(relation);

	/*
     * If the new tuple is too big for storage or contains already toasted
     * out-of-line attributes from some other relation, invoke the toaster.
     */
	if (relation->rd_rel->relkind != RELKIND_RELATION &&
		relation->rd_rel->relkind != RELKIND_MATVIEW)
	{
		/* toast table entries should never be recursively toasted */
		Assert(!HeapTupleHasExternal(tup));
		return tup;
	}
	else if (HeapTupleHasExternal(tup) || tup->t_len > TOAST_TUPLE_THRESHOLD)
		return toast_insert_or_update(relation, tup, NULL, options);
	else
		return tup;
}
