#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "sht_table.h"
#include "ht_table.h"
#include "record.h"

#include <assert.h>

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

typedef struct {
  int blockId;
  char name[16];
} secIndexEntry;


int SHT_CreateSecondaryIndex(char *sfileName,  int buckets, char* fileName) {
	
	int error;
	int fileDescriptor;

	error = TC(BF_CreateFile(sfileName));
  	error += TC(BF_OpenFile(sfileName, &fileDescriptor));
	if (error != 0) return -1;


  	// Write to the first block to make it a Hash File
	BF_Block* block;
	BF_Block_Init(&block);
	BF_AllocateBlock(fileDescriptor, block); // Allocate the first block

  	SHT_info info;
  	info.fileDesc = fileDescriptor;
  	info.numBuckets = buckets;
  	info.isHashFile = true;
	info.isHeapFile = false;

  	// Although named "records", we hold a much smaller entity, a secIndexEntry struct, with only (name,blockId)
  	info.recordsPerBlock = (sizeof(char) * BF_BLOCK_SIZE - sizeof(SHT_block_info)) / sizeof(secIndexEntry);
	
  	// totalSizeOfBuckets => How big the hashTable has to be
  	int totalSizeOfBuckets = buckets * sizeof(int);
  	// How much space is available in total for the hashTable
  	int hashTableSize = (sizeof(char) * BF_BLOCK_SIZE - sizeof(SHT_info));

  	info.hashTable = malloc(sizeof(int) * buckets);
	char* data = BF_Block_GetData(block);

  	// Allocate momory for the buckets
	for (int i=0; i<buckets; i++) {
		BF_Block* bucket;
		BF_Block_Init(&bucket);

    	// Allocate a block for the bucket
		error = TC(BF_AllocateBlock(fileDescriptor, bucket)); 
		if (error != 0) return -1;


		SHT_block_info blockInfo; // Create a SHT_block_info struct to write to the bucket
		blockInfo.recordsCount = (sizeof(char) * BF_BLOCK_SIZE - sizeof(SHT_block_info)) / (sizeof(secIndexEntry)); // And can fit this many records inside
		blockInfo.currentRecords = 0; // Has no records inside
		blockInfo.nextBlock = -1; 	// Has no next block
		
		char* bucketData = BF_Block_GetData(bucket);  	        // Get the data of the bucket
		memcpy(bucketData, &blockInfo, sizeof(SHT_block_info)); // Write the SHT_block_info struct to the bucket

		printf("Can hold: %d, holds: %d\n", blockInfo.recordsCount, blockInfo.currentRecords);
		assert(blockInfo.currentRecords == 0);
		
		// last bucket is in position counter-1. so hashTable[i] = counter-1
		int newBucketIn; BF_GetBlockCounter(fileDescriptor, &newBucketIn); // Get the position of the bucket
		newBucketIn--; // Decrease it by one
		
		info.hashTable[i] = newBucketIn;
		printf("Created bucket for [%d], saved in block: %d\n", i, info.hashTable[i]);

		BF_Block_SetDirty(bucket); 	 // Mark the block as dirty

		error = TC(BF_UnpinBlock(bucket)); 		// Unpin the block because we don't need it anymore
		if (error != 0) return -1;
	
		BF_Block_Destroy(&bucket); // Destroy the block allocated

	}

	// Now copy in the first block the SHT_info
  	memcpy(data, &info, sizeof(SHT_info));

  	BF_Block_SetDirty(block);

	error = TC(BF_UnpinBlock(block));
	if (error != 0) return -1;
	
	// Close the file
	error = TC(BF_CloseFile(fileDescriptor));
	if (error != 0) return -1;

	BF_Block_Destroy(&block);

	return 0;
}

SHT_info* SHT_OpenSecondaryIndex(char *indexName){

	int error;	// Error-checking
  	int fileDescriptor; // The file descriptor
	error = TC(BF_OpenFile(indexName, &fileDescriptor));
	if (error != 0) return NULL;

	BF_Block* block; 		// Create a block
	BF_Block_Init(&block); // Initialize the block
	SHT_info* toReturn = (SHT_info*) malloc(sizeof(SHT_info)); // Allocate memory for the returning HT_info struct

	error = TC(BF_GetBlock(fileDescriptor, 0, block));  // Get the first block
	if (error != 0) return NULL;
	
	char* data = BF_Block_GetData(block);  // Get the data of the first block
	SHT_info* infoSaved = (SHT_info*) data; // Cast the data to HT_info


	// If the file is not a Hash File, return NULL
	if (infoSaved->isHeapFile) { return NULL; }

	// Copy the data from the first block to the returning HT_info struct
	memcpy(toReturn, infoSaved, sizeof(SHT_info)); 
  
  	toReturn->fileDesc = fileDescriptor;

	printf("Opened file\n");
  	printf("Fd SHT after: %d\n", infoSaved->fileDesc);


	error = TC(BF_UnpinBlock(block)); 	   // Unpin the first block because we don't need it anymore
	if (error != 0) return NULL;

	BF_Block_Destroy(&block); // Destroy the block

  	return toReturn;
}


int SHT_CloseSecondaryIndex( SHT_info* SHT_info ){
	int error;
	int fileDescriptor = SHT_info->fileDesc; // Get the file descriptor
	BF_Block* block;	BF_Block_Init(&block);
  
	error = TC(BF_GetBlock(fileDescriptor, 0 , block)); // Get the first block
	if (error != 0) return -1;

	char* data = BF_Block_GetData(block); 	// Get the data of the first block
	
	assert(SHT_info != NULL);
	assert(data != NULL);
	memcpy(data, SHT_info, sizeof(SHT_info)); // Copy the data from the SHT_info struct to the first block
	
	BF_Block_SetDirty(block);
	error = TC(BF_UnpinBlock(block));
	if (error != 0) return -1;
	
	BF_Block_Destroy(&block);

	free(SHT_info->hashTable);
	free(SHT_info); // Free the memory of the HT_info struct
	
	error = TC(BF_CloseFile(fileDescriptor)); // Close the file
	if (error != 0) return -1;
	
  	return 0;
}

int SHT_SecondaryInsertEntry(SHT_info* sht_info, Record record, int block_id){
	int error;
	int fileDescriptor = sht_info->fileDesc;
  	int buckets = sht_info->numBuckets;
  	int recordsPerBlock = sht_info->recordsPerBlock;
  	int* hashTable = sht_info->hashTable;

  	int hash = hash_string(record.name) % buckets;
  	int bucket = hashTable[hash];

  	secIndexEntry toInsert;
  	toInsert.blockId = block_id;
  	strcpy(toInsert.name, record.name);


  	BF_Block* block; 		// Create a block
	BF_Block* iterativeBlock;
	BF_Block_Init(&block); // Initialize the block
	BF_Block_Init(&iterativeBlock);

	error = TC(BF_GetBlock(fileDescriptor, bucket, block));
	if (error != 0) return -1;

	error = TC(BF_GetBlock(fileDescriptor, bucket, iterativeBlock));
	if (error != 0) return -1;

	char* blockData = BF_Block_GetData(iterativeBlock);

  	SHT_block_info* blockInfoRead = (SHT_block_info *) blockData;
  	SHT_block_info* first = blockInfoRead;

  	bool insertEntryInIndex = true;
	bool iterated = false;

	// Before inserting, assert that the whole bucket chain does not already contain
	// a entry of <name,bucketID>, which would create a problem in GetAllEntries
	// as a block could be visited more than once

  	while ( true ) {
		
		// Iterate through all entries of the bucket
		for(int i = 0; i < first->currentRecords; i++) {

			// Check every entry in block
			char* data = blockData +  sizeof(SHT_block_info) + i * (sizeof(secIndexEntry));
			secIndexEntry entry = (secIndexEntry) *( (secIndexEntry*) data); // Cast the data to Record
			
			// Check if tuple <name, block_id> exists 
      		if (entry.blockId == block_id && strcmp(entry.name, record.name) == 0) {
        		insertEntryInIndex = false;
        		// printf("\n\n\nThere already exists an entry: <%s,%d> in the index\n\n\n", record.name, block_id);
        		break;
      		}
		}

		// If we found an entry, break out of the while loop, stop searching more
    	if (insertEntryInIndex == false) break;

		// Check if there is a next block (overflow)
		if ( first->nextBlock == -1) 
			break;
		else {
      		// Before I get the next block in the chain, unpin the current one
			if (iterated) {
				error = TC(BF_UnpinBlock(iterativeBlock));
				if (error != 0) return -1;
			}
			iterated = true;
      		
      		// Now get the next block
			error = TC(BF_GetBlock(fileDescriptor, first->nextBlock, iterativeBlock));
			if (error != 0) return -1;

			blockData = BF_Block_GetData(iterativeBlock);
			first = (SHT_block_info*) blockData;
		}
	}

	// Unpin the last one in the chain which was not unpinned
	if (iterated) {
		// printf("The iterative block has changed\n");
		error = TC(BF_UnpinBlock(iterativeBlock));
		if (error != 0) return -1;
	} else {
		// printf("Iterative block same as first\n");
	}

	// We have now asserted uniqueness inside the chain of blocks (whole "bucket")
  	// So an entry like the one we want does not already exist inside
	// and proceed with the insertion

	if (insertEntryInIndex) {

		int recordsInBlock = blockInfoRead->currentRecords;
		
    	// If records fits in block, just place it inside
		if (recordsInBlock < blockInfoRead->recordsCount) {
	  		// printf("No overflow in bucket: %d saved in block: %d\n", hash, bucket );
	  		char* data = blockData +  sizeof(SHT_block_info) + recordsInBlock * (sizeof(secIndexEntry));
	  		memcpy(data, &toInsert, sizeof(secIndexEntry));
	  		blockInfoRead->currentRecords++;
	  		BF_Block_SetDirty(block); // Mark the block as dirty
	  	} else {

	  		// If records doesn't fit in block, create a new block and place it there
	  		BF_Block* newBlock; 		// Create a block
	  		BF_Block_Init(&newBlock); // Initialize the block

			error = TC(BF_AllocateBlock(fileDescriptor, newBlock)); // Allocate a block for the new record
			if (error != 0) return -1;

     		// Get block number of last allocated block ( = blockCounter - 1)
	  		int blockCounter; BF_GetBlockCounter(fileDescriptor,&blockCounter ); // Get the number of the last allocated block
	  		blockCounter--; // Get the number of the last allocated block

	  		char* newBlockData = BF_Block_GetData(newBlock); // Get the data of the new block
	  		SHT_block_info* newBlockInfo = (SHT_block_info *) newBlockData; // Cast the data to HT_block_info
	  		
			newBlockInfo->currentRecords = 1; // Set the current records to 1

	  		// Connect newly allocated block with the previous block in place
	  		newBlockInfo->nextBlock = bucket; // Set the next block to previous bucket (reverse chaining)		
			newBlockInfo->recordsCount = (sizeof(char) * BF_BLOCK_SIZE  - sizeof(SHT_block_info) )/ sizeof(secIndexEntry); // Set the records count to the maximum number of records that can fit in a block

	  		
			char* data = newBlockData +  sizeof(SHT_block_info); // Get the data of the new block
	  		memcpy(data, &toInsert, sizeof(secIndexEntry)); // Copy the data from the record to the new block

			BF_Block_SetDirty(newBlock); // Mark the new block as dirty

			error = TC(BF_UnpinBlock(newBlock)); // Unpin the new block because we don't need it anymore
			if (error != 0) return -1;

			BF_Block_Destroy(&newBlock); // Destroy the new block
	
	  		sht_info->hashTable[hash] = blockCounter; // Set the bucket to the new block

	  	}
  	
	}
	BF_UnpinBlock(block); // Unpin the block because we don't need it anymore
	BF_Block_Destroy(&block); // Destroy the block
	BF_Block_Destroy(&iterativeBlock);
	return 0;
}

int SHT_SecondaryGetAllEntries(HT_info* ht_info, SHT_info* sht_info, char* name) {

	int error;
  	int hash = hash_string(name) % sht_info->numBuckets;
  	int bucket = sht_info->hashTable[hash];

  	BF_Block* block; 		// Create a block
	BF_Block_Init(&block); 	// Initialize the block

	error = TC(BF_GetBlock(sht_info->fileDesc, bucket, block));
	if (error != 0) return -1;

	char* blockData = BF_Block_GetData(block);

  	error = TC(BF_UnpinBlock(block));
	if (error != 0) return -1;

  	SHT_block_info* blockInfoRead = (SHT_block_info *) blockData;
  	int blocksRead = 0;

  	// Go down the chain of blocks in the SECONDARY INDEX
  	while ( true ) {
    
    	// Iterate through all entries inside the bucket of the SECONDARY INDEX
    	for(int i = 0; i < blockInfoRead->currentRecords; i++) {
      
      		// Check every entry inside the SECONDARY INDEX
      		char* data = blockData +  sizeof(SHT_block_info) + i * (sizeof(secIndexEntry));
      		secIndexEntry entry = (secIndexEntry) *( (secIndexEntry*) data); // Cast the data to Record
	
     		if	(strcmp(entry.name, name) == 0) {
        		printf("Found entry: <%s,%d> in the index\n", entry.name, entry.blockId); // Print the record
        		error = TC(BF_GetBlock(ht_info->fileDesc, entry.blockId, block)); 		// Get the block of the record
				if (error != 0) return -1;
        		
				HT_block_info* HT_header = (HT_block_info*) BF_Block_GetData(block);   // Get the data of the block
        		blocksRead++; // Increase the number of blocks read

        		// Iterate through all records of the block of the PRIMARY INDEX
        		// To find if there is a record inside, with the same name
        		for (int i = 0; i < HT_header->currentRecords; i++) { 
					char* data = (char*) HT_header +  sizeof(HT_block_info) + i * (sizeof(Record)); 
        		  	Record record = (Record) *( (Record*) data); // Cast the data to Record
        		  	// If the name of the record is the same as the name we are looking for
        		  	if ( strcmp(name, record.name ) == 0) {
				    	    printf("%d \t\t %s \t %s \t %s \n", record.id, record.name, record.surname, record.city);
        		    }
        		}
        		error = TC(BF_UnpinBlock(block));
				if (error != 0) return -1;
			}
    	}
    	// Check if there is a next block (overflow)
	  	if ( blockInfoRead->nextBlock == -1) 
			break; // If there is no next block, break the loop
	  	else {
      		// Now get the next block
			error = TC(BF_GetBlock(sht_info->fileDesc, blockInfoRead->nextBlock, block));

			blockData = BF_Block_GetData(block); // Get the data of the block
			blockInfoRead = (SHT_block_info*) blockData; // Cast the data to HT_block_info
		}
	}

	BF_Block_Destroy(&block);
	return blocksRead;
}

unsigned int hash_string(void* value) {
	// djb2 hash function, απλή, γρήγορη, και σε γενικές γραμμές αποδοτική
    unsigned int hash = 5381;
    for (char* s = value; *s != '\0'; s++)
		hash = (hash << 5) + hash + *s;			// hash = (hash * 33) + *s. Το foo << 5 είναι γρηγορότερη εκδοχή του foo * 32.
    return hash;
}

int HashStatisticsSHT(char* filename) {
	// Open file
	int fileDesc;
	BF_ErrorCode code = BF_OpenFile(filename, &fileDesc);
	if (code != BF_OK) {
		BF_PrintError(code);
		return -1;
	}

	// Get block 0
	BF_Block* block;
	BF_Block_Init(&block);
	code = BF_GetBlock(fileDesc, 0, block);
	if (code != BF_OK) {
		BF_PrintError(code);
		return -1;
	}

	// Get block data
	char* blockData = BF_Block_GetData(block);
	SHT_info* info = (SHT_info*) blockData;

	// Get number of blocks
	int blockCounter;
	code = BF_GetBlockCounter(fileDesc, &blockCounter);
	if (code != BF_OK) {
		BF_PrintError(code);
		return -1;
	}

	// Get number of buckets
	int buckets = info->numBuckets;
	int recordsCount = 0;

	// for each bucket:
	// MIN,MID AND MAX NUMBER OF RECORDS in buckets
	// Go through each bucket, get number of records
	// all the chain through
	
	BF_Block* blockOfBucket;
	BF_Block_Init(&blockOfBucket);
	printf("Buckets: %d\n", buckets);
	// meso aritho blocks pou exei kathe bucket
	int* blocksInBucket = malloc(buckets * sizeof(int));
	int* recordsInBuckets = malloc(sizeof(int) * buckets);
		
	for(int i = 0; i < buckets; i++) {
		// They begin with at least one block inside
		blocksInBucket[i] = 1;
		// And no records to begin with
		recordsInBuckets[i] = 0;
	} 

	for(int i = 0; i < buckets; i++) {
		int error;
		int bucket = info->hashTable[i];
		error = TC(BF_GetBlock(fileDesc, bucket, blockOfBucket));
		if (error != 0) return -1;

		void* data = BF_Block_GetData(blockOfBucket);
		SHT_block_info* blockInfo = (SHT_block_info*) data;
		
		recordsCount += blockInfo->currentRecords;
		recordsInBuckets[i] += blockInfo->currentRecords;
		while( true ) {
			if (blockInfo->nextBlock == -1)
				break;
			else {
				blocksInBucket[i]++;
				error = TC(BF_UnpinBlock(blockOfBucket));
				if (error != 0) return -1;

				error = TC(BF_GetBlock(fileDesc, blockInfo->nextBlock, blockOfBucket));
				if (error != 0) return -1;

				data = BF_Block_GetData(blockOfBucket);
				blockInfo = (SHT_block_info*) data;
				recordsCount += blockInfo->currentRecords;
				recordsInBuckets[i] += blockInfo->currentRecords;
			}
		}

		BF_UnpinBlock(blockOfBucket);
	}

	BF_UnpinBlock(block);
	BF_Block_Destroy(&blockOfBucket);
	BF_Block_Destroy(&block);
	
	int totalNumberOfBlocks = 0;
	for(int i = 0; i < buckets; i++)
		totalNumberOfBlocks += blocksInBucket[i];
	
	printf("1. Blocks in the file: %d\n", blockCounter);
	printf("2. Total number of records: %d\n", recordsCount);
	printf("\t Average number of records per bucket: %d\n", recordsCount/buckets);
	
	MinMax ptr = findMinAndMax(recordsInBuckets, buckets);
	printf("\t Min number of records in a block: %d\n", ptr->min);
	printf("\t Max number of records in a block: %d\n", ptr->max);

	// Plithos buckets pou exoun block yperxilisis
	int nofBucketsWithOverflow = 0;
	for(int i = 0; i < buckets; i++)
		// If only one block has been allocated there is NO overflow
		if (blocksInBucket[i] != 1) {
			nofBucketsWithOverflow++;
		}

	printf("4. Total Number of buckets with overflow blocks: %d\n", nofBucketsWithOverflow);
	
	// kai posa block einai auta gia kathe block
	for(int i = 0; i < buckets;i++) {
		if (blocksInBucket[i] != 1) {
			printf("\tBucket: %d has %d overflow blocks\n", i, blocksInBucket[i] - 1 );
		}
	}

	free(blocksInBucket);
	free(recordsInBuckets);
	free(ptr);
	
	// Close file
	code = BF_CloseFile(fileDesc);
	if (code != BF_OK) {
		BF_PrintError(code);
		return -1;
	}

	return 0;
}