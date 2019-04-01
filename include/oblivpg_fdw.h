//
// Created by Rogério Pontes on 2019-03-26.
//

#ifndef OBLIVPG_FDW_H
#define OBLIVPG_FDW_H

#include "access/tupdesc.h"
#include "utils/rel.h"

/*
 * Execution state of a foreign scan using postgres_fdw.
 *
 * The attributes of this data structure follow the example of postgres_fdw.c
 */
typedef struct OblivScanState{


    Relation mirrorTable; /*relchache entry for the mirror table*/
    Relation mirrorIndex; /*relcache entry for the mirror index*/

    TupleDesc tableTupdesc; /* table tuple descriptor for scan */
    TupleDesc indexTupdesc; /* index tuple descriptor for scan*/

    char      *query; /* text of SELECT command */


   // HeapScanDesc ss_currentScanDesc;
   // TupleTableSlot *ss_ScanTupleSlot;

    /* for storing result tuples
     *
     * In the future oblivpg might have a batch of resuts to return to the client
     **/
    HeapTupleData tuple;			/* array of currently-retrieved tuples */
    //int			num_tuples;		/* # of tuples in array */
    //int			next_tuple;		/* index of next one to return */


    /* working memory context */
    MemoryContext working_cxt;	    /*Possible memory context that wil be used */


} OblivScanState;

#endif //OBLIVPG_FDW_H
