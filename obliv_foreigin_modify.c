
static void
obliviousBeginForeignModify(ModifyTableState * mtstate,
							ResultRelInfo * rinfo, List * fdw_private,
							int subplan_index, int eflags)
{

	elog(DEBUG1, "In obliviousBeginForeignModify");
	Oid			mappingOid;
	Ostatus		obliv_status;
    MemoryContext mappingMemoryContext;
    MemoryContext oldContext;

    Relation oblivFDWTable;
	Relation oblivMappingRel;

	//underlying heap files
    Relation    heapTableRelation;
    Relation	indexRelation;
	FdwOblivTableStatus oStatus;
    char* relationName;
    mappingMemoryContext = AllocSetContextCreate(CurrentMemoryContext, "Obliv Mapping Table",  ALLOCSET_DEFAULT_SIZES);
    oldContext = MemoryContextSwitchTo(mappingMemoryContext);

	oblivFDWTable = rinfo->ri_RelationDesc;
    relationName = RelationGetRelationName(oblivFDWTable);

	mappingOid = get_relname_relid(OBLIV_MAPPING_TABLE_NAME, PG_PUBLIC_NAMESPACE);

	if (mappingOid != InvalidOid)
	{
		oblivMappingRel = heap_open(mappingOid, RowShareLock);

		oStatus = getOblivTableStatus(oblivFDWTable->rd_id, oblivMappingRel);
		oStatus.tableRelFileNode = oblivFDWTable->rd_id;

		obliv_status = validateIndexStatus(oStatus);
		if (obliv_status == OBLIVIOUS_UNINTIALIZED)
		{
			elog(DEBUG1, "Index has not been created");

			//Create heap file for obliv index.
			indexRelation = obliv_index_create(oStatus);

			//Create heap file for obliv table
            heapTableRelation = obliv_table_create(oblivFDWTable);

            //ipdate ostates data
            oStatus.indexRelFileNode =  indexRelation->rd_id;
            oStatus.heapTableRelFileNode = heapTableRelation->rd_id;

            //update OBLIV_MAPPING_TABLE records
			setOblivStatusInitated(oStatus, oblivMappingRel);
			heap_close(oblivMappingRel, RowShareLock);
            setupOblivStatus(oStatus);
		}
		else if (obliv_status == OBLIVIOUS_INITIALIZED)
		{
			elog(DEBUG1, "Index has already been created");
			/* Index has been created. */
			/**
			 * Initiate secure operator evaluator (SOE).
			 * Current ORAM bucket capacity is hardcoded to 1.
			 * */
			heap_close(oblivMappingRel, RowShareLock);

			initSOE(relationName, (size_t) oStatus.tableNBlocks, 1);
		}
		/**
		 *  If none of the above cases is valid, the record stored in
		 *  OBLIV_MAPPING_TABLE_NAME is invalid and an error message
		 *  has already been show to the user by the function
		 *  validateIndexStatus.
		 * */

	}
	else
	{
		/*
		 * The database administrator should create a Mapping table which maps
		 * the oid of the foreign table to its mirror table counterpart. The
		 * mirror table is used by this extension to find a matching index and
		 * simulate it.
		 *
		 */
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("Mapping table %s does not exist in the database!",
						OBLIV_MAPPING_TABLE_NAME)));
	}

	pfree(relationName);
	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(mappingMemoryContext);
    elog(DEBUG1, "closed begin foreing modify");


}
