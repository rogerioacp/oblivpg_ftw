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


#include "access/heapam.h"
#include "catalog/catalog.h"
#include "catalog/pg_class_d.h"
#include "storage/lockdefs.h"

char* generateOblivTableName(char* tableName){
	char * resultingName;
	size_t nameLen = strlen(tableName);

	resultingName = palloc(sizeof(char)*(6 + nameLen + 1));
	memcpy(resultingName, "obliv_",6);
	memcpy(&resultingName[6], tableName, nameLen+1);

	return resultingName;
}



Oid GenerateNewRelFileNode(Oid tableSpaceId, char relpersistance){

	Relation pg_class;
	Oid result;


	pg_class = heap_open(RelationRelationId, RowExclusiveLock);

	result = GetNewRelFileNode(tableSpaceId, pg_class,  relpersistance);

	heap_close(pg_class, RowExclusiveLock);

	return result;
}
