//
// Created by Rogério Pontes on 2019-03-29.
//

#ifndef FDW_OBLIV_OFILE_H
#define FDW_OBLIV_OFILE_H

#include "obliv_status.h"

void print_status(void);
void setupOblivStatus(FdwOblivTableStatus instatus, const char* tableName, const char* indexName, Oid indexHandlerOID);
void initIndex(const char* filename,  const char* pages, unsigned int nblocks, unsigned int blockSize, int initOffset);
void initRelation(const char* filename,  const char* pages, unsigned int nblocks, unsigned int blockSize);

void closeOblivStatus(void);

#endif //FDW_OBLIV_OFILE_H

