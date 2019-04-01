#include "oram/ofile.h"
#include "oram/plblock.h"

#include "include/obliv_page.h"
#include "include/obliv_status.h"

#include "postgres.h"
#include "access/heapam.h"
#include "catalog/pg_namespace_d.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"

FdwOblivTableStatus status;
int totalBlocks;

static void fileInit(const char *fileName, unsigned int totalNodes, unsigned int blockSize);

static void fileRead(PLBlock block, const char *fileName, const BlockNumber ob_blkno);

static void fileWrite(const PLBlock block, const char *fileName, const BlockNumber ob_blkno);

static void fileClose(const char *filename);



void setupOblivStatus(FdwOblivTableStatus instatus){
    elog(DEBUG1, "setup obliv status");
    status.relTableMirrorId = instatus.relTableMirrorId;
    status.relIndexMirrorId = instatus.relIndexMirrorId;
    status.tableRelFileNode = instatus.tableRelFileNode;
    status.filesInitated = instatus.filesInitated;
    status.indexNBlocks = instatus.indexNBlocks;
    status.tableNBlocks = instatus.tableNBlocks;
}



void logSpecialPointerData(){

    int index;
    Relation rel;
    Buffer buffer = 0;
    Page page = NULL;
    OblivPageOpaque oopaque;
    rel =  heap_open(status.relTableMirrorId, RowExclusiveLock);
    for(index = 0; index < totalBlocks; index++){
        buffer = ReadBuffer(rel, index);
        page =  BufferGetPage(buffer);
        oopaque = (OblivPageOpaque) PageGetSpecialPointer(page);
        ReleaseBuffer(buffer);
        elog(DEBUG1, "Block number %d  has special pointer value %d ", index, oopaque->o_blkno);

    }
    heap_close(rel, RowExclusiveLock);

}





/**
 *
 * This function follows a logic similar to the function RelationAddExtraBlocks in hio.c which  pre-extend a
 * relation by a calculated amount aof blocks.  The idea in this funciton (fileInit) is to initate every page in
 * the oram relation so that future read or write requests don't have to worry about this. Furthermore, since we know the
 * exact number of blocks the relation must have, we can allocate the space once and never worry about this again.
 * */
void fileInit(const char *filename, unsigned int nblocks, unsigned int blocksize) {
	//Oid relOid;
    //Oid	mappingOid;
    Relation rel;
    int offset = 0;
    Page page = NULL;
    Buffer buffer = 0;
    OblivPageOpaque oopaque;
    totalBlocks = nblocks;
    //Relation oblivMappingRel;
    //FdwOblivTableStatus oStatus;


    /*relOid = get_relname_relid(filename, PG_PUBLIC_NAMESPACE);
    mappingOid = get_relname_relid(OBLIV_MAPPING_TABLE_NAME, PG_PUBLIC_NAMESPACE);

    if (mappingOid != InvalidOid) {
        oblivMappingRel = heap_open(mappingOid, AccessShareLock);
        oStatus = getOblivTableStatus(relOid, oblivMappingRel);
        heap_close(oblivMappingRel, AccessShareLock);
    }*/



    elog(DEBUG1, "Initializing oblivious file for relation %s, heap OID %u, with a total of %u blocks  of size %u bytes", filename, status.relTableMirrorId, nblocks, blocksize);

	if(status.relTableMirrorId != InvalidOid){

	    rel =  heap_open(status.relTableMirrorId, RowExclusiveLock);
        do
            {
                buffer = ReadBuffer(rel, P_NEW);
                /**
                 *  Buffers are not being locked as this extension is not considering concurrent accesses to the
                 *  relations. It might raise some unexpected errors if the postgres implementation checks if buffers
                 *  have pins or locks associated.
                 *
                 **/

                elog(DEBUG1, "Buffer block number is %d", BufferGetBlockNumber(buffer));
                page =  BufferGetPage(buffer);

                 if (!PageIsNew(page))
                     elog(ERROR, "page %u of relation \"%s\" should be empty but is not",
                         BufferGetBlockNumber(buffer),
                         RelationGetRelationName(rel));

                 PageInit(page, blocksize, sizeof(OblivPageOpaqueData));
                 oopaque = (OblivPageOpaque) PageGetSpecialPointer(page);
                 oopaque->o_blkno = DUMMY_BLOCK;

                /*
                * We mark all the new buffers dirty, but do nothing to write them
                * out; they'll probably get used soon, and even if they are not, a
                * crash will leave an okay all-zeroes page on disk.
                */
                MarkBufferDirty(buffer);
                /**
                 *  The original function RelationAdddExtraBlocks updates the free space map of the
                 *  relation but this function does not. The free space map is not updated for now and
                 *  must be considered if it can be used at all since it keeps track in plaintext how much
                 *  space is free in each relation block. Only use the fsm if it's really necessary for the
                 *  prototype.
                 *  */
                ReleaseBuffer(buffer);


           }while(offset++ < nblocks);
        heap_close(rel, RowExclusiveLock);

    }else{
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Oblivious table with name %s does not exist in the database",
                               filename)));
	}
}

void fileRead(PLBlock block, const char *filename, const BlockNumber ob_blkno) {
   // Oid relOid;
   // Oid mappingOid;

    Relation rel;
    Page page = NULL;
    Buffer buffer = 0;
    unsigned int pagesize;
    OblivPageOpaque oopaque;
    int page_size = 0;
    BlockNumber targetBlock;
   /* Relation oblivMappingRel;
    FdwOblivTableStatus oStatus;*/


    //elog(DEBUG1, "fileRead %s block %u", filename, ob_blkno);

   /* relOid = get_relname_relid(filename, PG_PUBLIC_NAMESPACE);
    mappingOid = get_relname_relid(OBLIV_MAPPING_TABLE_NAME, PG_PUBLIC_NAMESPACE);

    if (mappingOid != InvalidOid) {
        oblivMappingRel = heap_open(mappingOid, AccessShareLock);
        oStatus = getOblivTableStatus(relOid, oblivMappingRel);
        heap_close(oblivMappingRel, AccessShareLock);
    }*/


    if(status.relTableMirrorId != InvalidOid){
        rel =  heap_open(status.relTableMirrorId, RowExclusiveLock);
       // elog(DEBUG1, "Going to read buffer for block %d", ob_blkno);
        //targetBlock =  RelationGetTargetBlock(rel);
        //elog(DEBUG1, "Relation get target block %d", targetBlock);
        buffer = ReadBuffer(rel,  ob_blkno);

        //elog(DEBUG1, "Buffer read block number is %d", BufferGetBlockNumber(buffer));

        /**
         *
         * No locks are being used by this code as it assumes that the execution is single threaded. Might
         * create some problems if posgres implementation requires any locks.
         *
         * */
        // elog(DEBUG1, "Getting page from buffer");

        page = BufferGetPage(buffer);
        ///pagesize =  BufferGetPageSize(buffer);
        oopaque = (OblivPageOpaque) PageGetSpecialPointer(page);
        elog(DEBUG1, " fileRead oblivious blockNumber %d that is real block %d", ob_blkno, oopaque->o_blkno);
        page_size = BufferGetPageSize(buffer);
        block->block = (void*) malloc(page_size);
        //elog(DEBUG1, "copy postgres block to block %u ", pagesize);
        memcpy(block->block, (char*) page, page_size);
        block->blkno = oopaque->o_blkno;
        block->size = page_size;
        ReleaseBuffer(buffer);

        //elog(DEBUG1, "fileRead closing relation");

        heap_close(rel, RowExclusiveLock);

    }else{
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Oblivious table with name %s does not exist in the database",
                               filename)));
    }

}

void fileWrite(const PLBlock block, const char *filename, const BlockNumber ob_blkno) {
  //  Oid relOid;
   // Oid mappingOid;

    Relation rel;
    Page page = NULL;
    Buffer buffer = 0;
    OblivPageOpaque oopaque;

    // Relation oblivMappingRel;
    //FdwOblivTableStatus oStatus;
    //elog(DEBUG1, "fileWrite %s block %u", filename, ob_blkno);

    // relOid = get_relname_relid(filename, PG_PUBLIC_NAMESPACE);
    //mappingOid = get_relname_relid(OBLIV_MAPPING_TABLE_NAME, PG_PUBLIC_NAMESPACE);

   /* if (mappingOid != InvalidOid) {
        oblivMappingRel = heap_open(mappingOid, AccessShareLock);
        oStatus = getOblivTableStatus(relOid, oblivMappingRel);
        heap_close(oblivMappingRel, AccessShareLock);
    }*/

    if(status.relTableMirrorId != InvalidOid){
        rel =  heap_open(status.relTableMirrorId, RowExclusiveLock);
        buffer = ReadBuffer(rel,  ob_blkno);

        //elog(DEBUG1, "Buffer read block number is %d", BufferGetBlockNumber(buffer));

        /**
         *
         * No locks are being used by this code as it assumes that the execution is single threaded. Might
         * create some problems if posgres implementation requires any locks.
         *
         * */
        page = BufferGetPage(buffer);

        memcpy(page, block->block, block->size);

        oopaque = (OblivPageOpaque) PageGetSpecialPointer(page);
        if(block->blkno == DUMMY_BLOCK){
            oopaque->o_blkno = DUMMY_BLOCK;
        }
        elog(DEBUG1, " fileWrite oblivious blockNumber %d that is real block %d", ob_blkno, oopaque->o_blkno);

        MarkBufferDirty(buffer);

        ReleaseBuffer(buffer);
        heap_close(rel, RowExclusiveLock);
       // elog(DEBUG1, "complete fileWrite %s block %u", filename, ob_blkno);

    }else{
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Oblivious table with name %s does not exist in the database",
                               filename)));
    }
}


void fileClose(const char * filename) {
}



AMOFile *ofileCreate(){

    AMOFile *file = (AMOFile *) malloc(sizeof(AMOFile));
    file->ofileinit = &fileInit;
    file->ofileread = &fileRead;
    file->ofilewrite = &fileWrite;
    file->ofileclose = &fileClose;
    return file;
}