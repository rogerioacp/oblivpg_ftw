#include "postgres.h"

#include "access/htup_details.h"
#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"

#include "storage/smgr.h"
#include "storage/bufpage.h"

#include "include/obliv_page.h"
#include "include/oblivpg_fdw.h"
#include "include/obliv_status.h"
#include "include/obliv_ocalls.h"

#include "utils/fmgroids.h"

#ifndef UNSAFE
#include "Enclave_u.h"
#else
#include "Enclave_dt.h"
#endif

#include "oram/plblock.h"

typedef struct SoeHashPageOpaqueData
{
	BlockNumber hasho_prevblkno;	/* see above */
	BlockNumber hasho_nextblkno;	/* see above */
	Bucket		hasho_bucket;	/* bucket number this pg belongs to */
	uint16		hasho_flag;		/* page type code + flag bits, see above */
	uint16		hasho_page_id;	/* for identification of hash indexes */
	int			o_blkno;		/* real block number or Dummy Block */
}			SoeHashPageOpaqueData;

typedef SoeHashPageOpaqueData * SoeHashPageOpaque;



#define SOE_CONTEXT "SOE_CONTEXT"

FdwOblivTableStatus status;
char	   *tableName = "mirror_usertable";
char	   *indexName = "mirror_usertable_key";
Oid			ihOID;

void
oc_logger(const char *str)
{
	elog(DEBUG1, "%s", str);
}


void
print_status()
{
	elog(DEBUG1, "tableName is %s and indexName is %s ", tableName, indexName);
}

void
setupOblivStatus(FdwOblivTableStatus instatus, const char *tbName, const char *idName, Oid indexHandlerOID)
{
	status.relTableMirrorId = instatus.relTableMirrorId;
	status.relIndexMirrorId = instatus.relIndexMirrorId;
	status.tableRelFileNode = instatus.tableRelFileNode;
	status.filesInitated = instatus.filesInitated;
	status.indexNBlocks = instatus.indexNBlocks;
	status.tableNBlocks = instatus.tableNBlocks;

	ihOID = indexHandlerOID;
	//tableName = (char *) palloc(strlen(tbName) + 1);
	//indexName = (char *) palloc(strlen(idName) + 1);
    //memcpy(tableName, tbName, strlen(tbName) + 1);
	//memcpy(indexName, idName, strlen(idName) + 1);
}
void
closeOblivStatus()
{
	pfree(tableName);
	pfree(indexName);
}

/**
* The initialization follows the underlying hash index relation follows
* the logic of the function _hash_alloc_buckets in the hashpage.c file.
*
* The idea of this initialization procedure is to create the requested
* number of blocks (nblocks) as empty pages in the index relation file,
* including the metapage which was initialized by the database when the index
* is created.
*
* With this procedure, the hash index functions in the enclave can ask for any
* page on storage within the range of the nblocks and initialize the page as it
* sees fit.  The hash within the enclave works with a virtual index file
* abstraction that maps the enclave pages to the index relation pages
* pre-allocated in this procedure.
*/
void
initIndex(const char *filename, const char *pages, unsigned int nblocks, unsigned int blockSize, int initOffset)
{

	Relation	rel;
	int			offset = 0;
	Buffer		buffer = 0;
	Page		page = NULL;


    //elog(DEBUG1, "Requested to init index  %s for tableName %s and index  name %s", filename, tableName, indexName);


	if (status.relIndexMirrorId != InvalidOid)
	{

		rel = index_open(status.relIndexMirrorId, ExclusiveLock);

		do
		{

			/**
	         * when the index is initialized by the database the first four blocks
	         * of an Hash index already exist and have some defined data.
	         * we override this blocks
	         * to be initialized by the soe blocks.
	         **/
			if (ihOID == F_HASHHANDLER && (initOffset + offset) < 4)
			{
				buffer = ReadBuffer(rel, offset);
			}
			else if (ihOID == F_BTHANDLER && (initOffset + offset) == 0)
			{
				/**
		         *  When the btree index is created, block 0 is initiated and
		         *  has content that must be deleted.
		         */
				buffer = ReadBuffer(rel, offset);
			}
			else
			{
				buffer = ReadBuffer(rel, P_NEW);
			}
			/**
	         * Buffers are not being locked as this extension is not
	         * considering concurrent accesses to the
	         * relations. It might raise some unexpected errors if the
	         * postgres implementation checks if buffers
	         * have pins or locks associated.
	         **/

			page = BufferGetPage(buffer);

			memcpy(page, pages + (offset * BLCKSZ), blockSize);

			/*
			 * We mark all the new buffers dirty, but do nothing to write them
			 * out; they'll probably get used soon, and even if they are not,
			 * a crash will leave an okay all-zeroes page on disk.
			 */
			MarkBufferDirty(buffer);

			/**
	         * The original function RelationAdddExtraBlocks updates the
	         * free space map of the relation but this function does not.
	         * The free space map is not updated for now and
	         *  must be considered if it can be used at all since it keeps
	         * track in plaintext how much
	         * space is free in each relation block. Only use the fsm if
	         * it's really necessary for the prototype.
	         */
			ReleaseBuffer(buffer);

			offset += 1;
		} while (offset < nblocks);

		index_close(rel, ExclusiveLock);

	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("Oblivious table with name %s does not exist in the database",
						filename)));
	}

}

/**
 * This function follows a logic similar to the function
 * RelationAddExtraBlocks in hio.c which  pre-extend a
 * relation by a calculated amount of blocks.  The idea in this
 * function (fileInit) is to initiate every page in
 * the ORAM relation so that future read or write requests don't have to
 * worry about this. Furthermore, since we know the
 * exact number of blocks the relation must have, we can allocate
 * the space once and never worry about this again.
 **/
void
initRelation(const char *filename, const char *pages, unsigned int nblocks, unsigned int blockSize)
{

	Relation	rel;
	Buffer		buffer = 0;
	int			offset = 0;
	Page		page = NULL;
   
    //elog(DEBUG1, "Initializing heap relation %s with nblocks %d of size %d", filename, nblocks, blockSize);
   	
    if (status.relTableMirrorId != InvalidOid)
	{

		rel = heap_open(status.relTableMirrorId, ExclusiveLock);
		do
		{
			buffer = ReadBuffer(rel, P_NEW);

			/**
             * Buffers are not being locked as this extension is not
             * considering concurrent accesses to the
             * relations. It might raise some unexpected errors if the
             * postgres implementation checks if buffers
             * have pins or locks associated.
             **/

			page = BufferGetPage(buffer);
            if(!PageIsVerified(pages + (offset * BLCKSZ), BufferGetBlockNumber(buffer))){
                elog(ERROR, "Page is not verified when init relation. block %d", offset);
            };

            memcpy(page, (pages + (offset * BLCKSZ)), blockSize);

			/*
			 * We mark all the new buffers dirty, but do nothing to write them
			 * out; they'll probably get used soon, and even if they are not,
			 * a crash will leave an okay all-zeroes page on disk.
			 */
			MarkBufferDirty(buffer);

			/**
             * The original function RelationAdddExtraBlocks updates the
             * free space map of the relation but this function does not.
             * The free space map is not updated for now and
             *  must be considered if it can be used at all since it keeps
             * track in plaintext how much
             * space is free in each relation block. Only use the fsm if
             * it's really necessary for the prototype.
             */
			ReleaseBuffer(buffer);

			offset += 1;
		} while (offset < nblocks);
		heap_close(rel, ExclusiveLock);

	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("Oblivious table with name %s does not exist in the database",
						filename)));
	}

}


#ifndef UNSAFE
void
#else
sgx_status_t
#endif
outFileInit(const char *filename, const char *pages, unsigned int nblocks, unsigned int blocksize, int pageSize, int initOffset)
{

	if (strcmp(filename, tableName) == 0)
	{
		initRelation(filename, pages, nblocks, blocksize);

	}
	else if (strcmp(filename, indexName) == 0)
	{
		initIndex(filename, pages, nblocks, blocksize, initOffset);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("Enclave requested a file initialization for %s, %s, %s that is not supported",
						filename, tableName, indexName)));
	}

#ifdef UNSAFE
	return SGX_SUCCESS;
#endif
}



#ifndef UNSAFE
void
#else
sgx_status_t
#endif
outFileRead(const char *filename, const char *pages, int pagesSize, int  *blknos, int posSize)
{
	Relation	rel;
	Buffer		buffer;
	Page		heapPage;
	bool		isIndex;
	Oid			targetTable;
    char*       page;
    int         offset, blkno = 0;
    int         nblocks = posSize / sizeof(int);

    //elog(DEBUG1, "outFileRead relation %s blocknum %d and size %d", filename, blkno, pageSize);
	
    if (strcmp(filename, tableName) == 0)
	{
		isIndex = false;
		targetTable = status.relTableMirrorId;

	}
	else if (strcmp(filename, indexName) == 0)
	{
		isIndex = true;
		targetTable = status.relIndexMirrorId;
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("Enclave requested a file read for %s, %s, %s that is not supported",
						filename, tableName, indexName)));
	}

	if (targetTable != InvalidOid)
	{
        
    
		if (isIndex)
		{
			rel = index_open(targetTable, RowExclusiveLock);
		}
		else
		{
			rel = heap_open(targetTable, RowExclusiveLock);
		}
        
        for(offset=0; offset < nblocks; offset++){
            blkno = blknos[offset];
            page = (char*) &pages[offset*BLCKSZ];

			/**
	        * Buffers are not being locked as this extension is not
	        * considering concurrent accesses to the
	        * relations. It might raise some unexpected errors if the
	        * postgres implementation checks if buffers
	        * have pins or locks associated.
	        **/
			buffer = ReadBuffer(rel, blkno);
			heapPage = BufferGetPage(buffer);

			memcpy(page, heapPage, BLCKSZ);


			ReleaseBuffer(buffer);
	    }

		if (isIndex)
		{
			index_close(rel, RowExclusiveLock);
		}
		else
		{
			heap_close(rel, RowExclusiveLock);
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("Oblivious table with name %s does not exist in the database",
						filename)));
	}

#ifdef UNSAFE
	return SGX_SUCCESS;
#endif

}

#ifndef UNSAFE
void
#else
sgx_status_t
#endif
outFileWrite(const char *filename, const char *pages, int pagesSize, int *blknos, int posSize)
{

	Relation	rel;
	Page		heapPage;
	Buffer		buffer;
	bool		isIndex;
	Oid			targetTable;
    char        *page;
    int         offset, blkno = 0;
    int         nblocks = posSize / sizeof(int); 
    //elog(DEBUG1, "outFileWrite relation %s blocknum %d and size %d", filename, blkno, pageSize);

	buffer = 0;

	if (strcmp(filename, tableName) == 0)
    {
	
		isIndex = false;
		targetTable = status.relTableMirrorId;

	}
	else if (strcmp(filename, indexName) == 0)
	{
		isIndex = true;
		targetTable = status.relIndexMirrorId;
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("Enclave requested a outFileWrite for %s that is not supported",
						filename)));
	}


	if (status.relTableMirrorId != InvalidOid)
	{

		if (isIndex)
		{
			rel = index_open(targetTable, RowExclusiveLock);
		}
		else
		{
			rel = heap_open(targetTable, RowExclusiveLock);
		}

        for(offset = 0; offset < nblocks; offset++){
        	blkno = blknos[offset];
        	page = (char*) &pages[offset*BLCKSZ];

			buffer = ReadBuffer(rel, blkno);
			/**
	         * Buffers are not being locked as this extension is not
	         * considering concurrent accesses to the
	         * relations. It might raise some unexpected errors if the
	         * postgres implementation checks if buffers
	         * have pins or locks associated.
	         **/
			heapPage = BufferGetPage(buffer);

	        memcpy(heapPage, page, BLCKSZ);

			MarkBufferDirty(buffer);
			ReleaseBuffer(buffer);
        }

		if (isIndex)
		{
			index_close(rel, RowExclusiveLock);
		}
		else
		{
			heap_close(rel, RowExclusiveLock);
		}

	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("Oblivious table with name %s does not exist in the database",
						filename)));
	}

#ifdef UNSAFE
	return SGX_SUCCESS;
#endif

}


#ifndef UNSAFE
void
#else
sgx_status_t
#endif
outFileClose(const char *filename)
{
	elog(DEBUG1, "OutFileClose invoked");
#ifdef UNSAFE
	return SGX_SUCCESS;
#endif
}
