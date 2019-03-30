#include "oram/plblock.h"
#include "oram/oram.h"
#include "oram/stash.h"
#include "oram/pmap.h"
#include "oram/ofile.h"

#include "include/obliv_soe.h"
#include "include/obliv_page.h"
#include "include/oblivpg_fdw.h"


#include "access/htup_details.h"



//This file is for now simullating the calls to the enclave and the code that needs to be processed securely on SGX.
//It keeps track of the index and table relationships, the blocks for each relation and the free space on each block.
//It sends the oblivious requests to the relation blocks and sends a final result to the client.

//Includes can only contain some selected postgres components


ORAMState stateTable = NULL;

int currentBlock;



void initSOE(char* relName, size_t nblocks, size_t bucketCapacity) {


    /** A postgres relation file, a single segment, has a default size of 1GB.
     * Each block has a default size of 8kb, 8192 bytes ( BLCKSZ defined in pg_config.h), which means
     * that each segment has 131072 blocks.
     *
     * this function input nblocks defines the maximum number of blocks that 
     * the original file should have. Given this input, this function can 
     * calculate the maximum file size since the oram block size will be the
     * same as the oram block size.
     */

    if(stateTable == NULL){

        //First invocation of init SOE and must define the global variable.
        //Otherwise nothign needs to be done.

        elog(DEBUG1, "Initate SOE for the first time");
        /**
         * calculate the fileSize
         * This blocksize include the size used by the page header and not just the data, not sure if the size has to be
         * adjusted to account for that if the header is not encrypted.
         */
        size_t fileSize = nblocks * BLCKSZ;
        AMStash *stash = NULL;
        AMPMap *pmap = NULL;
        AMOFile *ofile = NULL;
        Amgr* amgr = (Amgr*) malloc(sizeof(Amgr));


        stash = stashCreate();
        pmap = pmapCreate();
        ofile = ofileCreate();

        amgr->am_stash = stash;
        amgr->am_pmap = pmap;
        amgr->am_ofile = ofile;
        stateTable = init(relName, fileSize, BLCKSZ, bucketCapacity, amgr);
        currentBlock =  0;
        elog(DEBUG1, "SOE initialized for table %s", relName);
        //int teste =  amgr->am_pmap->pmget(relName, 0);
        //elog(DEBUG1, "PMAP get position for block %d in pos %d",0, teste);

    }

}




//This function follows the flow of inserting a tuple from RelationPutHeapTuple(hio.c) and PageAddItem(bufpage.c)
void insertTuple(const char* relname, Item item, Size size)
{
    elog(DEBUG1, "gettuple %d", currentBlock);

    /**
     * When an insert operation is received on the enclave, the enclave has to index the inserted value and
     * store the tuple on the table relation.
     *
     * For now we are not indexing the value and are just inserting it on a block in the relationship file that has
     * free space.
     **/
    char* page;
    int result;
    OblivPageOpaque oopaque;


    elog(DEBUG1, "Reading block %d from oram file", currentBlock);
    result = read(&page, (BlockNumber) currentBlock, stateTable);

    if(result == DUMMY_BLOCK){

        /**
         * When the read returns a DUMMY_BLOCK page  it means its the first time the page is read from the disk.
         * As such, its going to be the first time a tuple is going to be written to the page and the special space of
         * the page has to be tagged with the real block block number so future accesses know that it's no longer a
         * dummy block.
         * As such, a new page needs to be allocated and initialized so the tuple can be added.
         **/
        elog(DEBUG1, "First time PAGE is Read. Going to initialize a new one.");
        page = (char*) malloc(BLCKSZ);
        //When this code is in the Enclave, PageInit will have be an internal function or an OCALL.
        PageInit(page, BLCKSZ, sizeof(OblivPageOpaqueData));
        oopaque = (OblivPageOpaque) PageGetSpecialPointer(page);
        oopaque->o_blkno = currentBlock;
        elog(DEBUG1, "Page allocated and initalized.");
    }


    elog(DEBUG1, "Page from block %d read", currentBlock);

    OffsetNumber limit;
    OffsetNumber offsetNumber;
    ItemId itemId;
    int	lower;
    int upper;
    Size alignedSize;

    PageHeader phdr = (PageHeader) page;
    int flags = true; //Defined on the invocation of PageAddItem in RelationPutHeapTuple

    /*
     * Be wary about corrupted page pointers
     */
    if (phdr->pd_lower < SizeOfPageHeaderData ||
        phdr->pd_lower > phdr->pd_upper ||
        phdr->pd_upper > phdr->pd_special ||
        phdr->pd_special > BLCKSZ)
        ereport(PANIC,
                (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("corrupted page pointers: lower = %u, upper = %u, special = %u",
                               phdr->pd_lower, phdr->pd_upper, phdr->pd_special)));

    /*
     * Select offsetNumber to place the new item at
     */
    limit = OffsetNumberNext(PageGetMaxOffsetNumber(page));

    /**
     * Don't bother searching, heap tuples are never updated or deleted.
     * The prototype is only doing sequential insertions*/

    offsetNumber = limit;


    /* Reject placing items beyond heap boundary, if heap */
    if ((flags & PAI_IS_HEAP) != 0 && offsetNumber > MaxHeapTuplesPerPage)
    {
        elog(WARNING, "can't put more than MaxHeapTuplesPerPage items in a heap page");
        exit(1);
    }
    /*
     * Compute new lower and upper pointers for page, see if it'll fit.
     *
     * Note: do arithmetic as signed ints, to avoid mistakes if, say,
     * alignedSize > pd_upper.
     */

    lower = phdr->pd_lower + sizeof(ItemIdData);

    alignedSize = MAXALIGN(size);

    upper = (int) phdr->pd_upper - (int) alignedSize;

    /*
     * OK to insert the item.  First, shuffle the existing pointers if needed.
     */
    itemId = PageGetItemId(phdr, offsetNumber);

    /* set the item pointer */
    ItemIdSetNormal(itemId, upper, size);
    elog(DEBUG1, "Writting item to offset %d\n", upper);

    /* copy the item's data onto the page */
    memcpy((char *) page + upper, item, size);

    /* adjust page header */
    phdr->pd_lower = (LocationIndex) lower;
    phdr->pd_upper = (LocationIndex) upper;

    elog(DEBUG1, "page header lower %d\n", lower);
    elog(DEBUG1, "page header lower %d\n", upper);


    result  = write(page, BLCKSZ, (BlockNumber) currentBlock, stateTable);
    /**Whether the page was allocated in this function or in page_read on the obliv_ofile.c, it can be freed as it will
     *no longer be used. Test for memory leaks.
     */
    free(page);

}


bool getTuple(OblivScanState* state){

    elog(DEBUG1, "gettuple %d", currentBlock);

    char* page;
    int result;
    OblivPageOpaque  opaque;
    ItemId lpp;
    size_t lineoff;
    HeapTuple tuple = &(state->tuple);

    elog(DEBUG1, "Reading block %d from oram file", currentBlock);
    result = read(&page, (BlockNumber) currentBlock, stateTable);

    if(result == DUMMY_BLOCK) {

        return false;
    }

    opaque = (OblivPageOpaque) PageGetSpecialPointer(page);
    elog(DEBUG1, "Page block number read from disk is %d", opaque->o_blkno);

    lineoff = PageGetMaxOffsetNumber(page);
    lpp = PageGetItemId((Page) page, lineoff);
    Assert(ItemIdIsNormal(lpp));
    tuple->t_data = (HeapTupleHeader) PageGetItem((Page) page, lpp);
    tuple->t_len = ItemIdGetLength(lpp);
    ItemPointerSet(&(tuple->t_self), (BlockNumber) currentBlock, lineoff);


    return true;

}
