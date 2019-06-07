//
// Created by Rogério Pontes on 2019-03-26.
//

#ifndef OBLIVPG_FDW_H
#define OBLIVPG_FDW_H

#include "access/tupdesc.h"
#include "utils/rel.h"
#include "access/htup_details.h"


/*
 * Execution state of a foreign scan using postgres_fdw.
 *
 * The attributes of this data structure follow the example of postgres_fdw.c
 */
typedef struct OblivScanState{


    Relation mirrorTable; /*relchache entry for the mirror table*/
    TupleDesc tableTupdesc; /* table tuple descriptor for scan */


    /* for storing result tuples
     *
     * In the future oblivpg might have a batch of resuts to return to the client
     **/
    HeapTupleData tuple;			/* array of currently-retrieved tuples */
    HeapTupleHeader tupleHeader;    /* Temporary Tuple Header location*/

	char* searchValue;  //currently we are assuming saerchees over char types. Encrypted blocks
	int   searchValueSize; //The size of the encryptedBlock.

    Oid   opno;

} OblivScanState;

#endif //OBLIVPG_FDW_H
