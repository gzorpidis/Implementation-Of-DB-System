#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
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

int TC(BF_ErrorCode error) {
    if (error != BF_OK) {
        BF_PrintError(error);
        return (-1);
    } else {
        return 0;
    }
}


// 	/*
// 	Block allocated: 1 (0)
// 					Start of Hashtable
// 					˅ 
// 	_________________________________________________________________________
// 	|				|				|					|					|
// 	|				|				|					|					|
// 	|	HT_block	|	bucket[0]	|	bucket[1]		|		bucket[N]	|
// 	|				|				|					|					|
// 	|				|				|					|					|
// 	|_______________|_______________|___________________|___________________|
// 					/				|					|
// 				   |				|					|
// 	|--------------|				|					|_____________________________________________
// 	|								|____________________											  |
// 	|													˅ 											  ˅  
// 	˅                          
// 	____________________________________________		____________________________________
// 	|							|				|		|						|			|		
// 	|		HT_block_info		|	Rec[0] ... 	|		|		HT_block_info	|			|
// 	|							|				|		|						|	Rec[0]	|		....
// 	|							|				|		|						|			|
// 	|							|				|		|						|			|
// 	L-------------------------------------------|		|-----------------------|-----------|
// 	block that stored info for buckets that land in [0]
// 	*/

int HT_CreateFile(char *fileName,  int buckets){

	int error;

	// Create a file with name fileName
	error = TC(BF_CreateFile(fileName));
	if (error != 0) return -1;


	// Open the file with name fileName
	int fileDescriptor;

	error = TC(BF_OpenFile(fileName, &fileDescriptor));
	if (error != 0) return -1;


	// Write to the first block to make it a Hash File
	BF_Block* block;
	BF_Block_Init(&block);

	error = TC(BF_AllocateBlock(fileDescriptor, block)); // Allocate the first block
	if (error != 0) return -1;

	// Create a HT_info struct to write to the first block
	HT_info info;
	info.fileDesc = fileDescriptor;  // Write the file descriptor
	info.numBuckets = buckets;		// Write the number of buckets
	info.isHashFile = true; 	   // Write that this is a Hash File
	info.isHeapFile = false;  	  // Write that this is not a Heap File
	info.recordsPerBlock = (sizeof(char) * BF_BLOCK_SIZE - sizeof(HT_block_info)) / (sizeof(Record));
	
	int totalSizeOfBuckets = buckets * (sizeof(int));
	int hashTableSize = ( sizeof(char) * BF_BLOCK_SIZE - sizeof(HT_info) );

	assert(totalSizeOfBuckets <= hashTableSize);

	// info.hashTable = malloc(sizeof(int) * buckets);
	// Get the data of the first block
	char* data = BF_Block_GetData(block);

	// Allocate momory for the buckets
	for (int i=0; i<buckets; i++) {
		BF_Block* bucket;
		BF_Block_Init(&bucket);
		error = TC(BF_AllocateBlock(fileDescriptor, bucket)); // Allocate a block for the bucket
		if (error != 0) return -1;

		HT_block_info blockInfo; // Create a HT_block_info struct to write to the bucket
		// blockInfo.overflow = -1; // The new bucket doesn't have a overflow
		blockInfo.recordsCount = (sizeof(char) * BF_BLOCK_SIZE - sizeof(HT_block_info)) / (sizeof(Record)); // And can fit this many records inside
		blockInfo.currentRecords = 0; // Has no records inside
		blockInfo.nextBlock = -1; // Has no next block
		
		char* bucketData = BF_Block_GetData(bucket);  	   // Get the data of the bucket
		memcpy(bucketData, &blockInfo, sizeof(HT_block_info)); // Write the HT_block_info struct to the bucket

		// printf("Can hold: %d, holds: %d\n", blockInfo.recordsCount, blockInfo.currentRecords);
		assert(blockInfo.currentRecords == 0);
		
		// last bucket is in position counter-1. so hashTable[i] = counter-1
		int newBucketIn; BF_GetBlockCounter(fileDescriptor, &newBucketIn); // Get the position of the bucket
		newBucketIn--; // Decrease it by one
		
		info.hashTable[i] = newBucketIn;
		// printf("Created bucket for [%d], saved in block: %d\n", i, info.hashTable[i]);


		BF_Block_SetDirty(bucket); 	 // Mark the block as dirty
		error = TC(BF_UnpinBlock(bucket)); 		// Unpin the block because we don't need it anymore
		if (error != 0) return -1;
		
		BF_Block_Destroy(&bucket); // Destroy the block
	}

	memcpy(data, &info, sizeof(HT_info)); // Write the HT_info struct to the first block
	
	BF_Block_SetDirty(block);
	
	error = TC(BF_UnpinBlock(block));
	if (error != 0) return -1;
	
	// close the file
	error = TC(BF_CloseFile(fileDescriptor));
	if (error != 0) return -1;

	BF_Block_Destroy(&block);

	return 0;
}

HT_info* HT_OpenFile(char *fileName){
	// Open the file with name fileName
	int fileDescriptor; // The file descriptor
	int error;

	error = TC(BF_OpenFile(fileName, &fileDescriptor));
	if (error != 0) return NULL;
	

	BF_Block* block; 		// Create a block
	BF_Block_Init(&block); // Initialize the block
	HT_info* toReturn = (HT_info*) malloc(sizeof(HT_info)); // Allocate memory for the returning HT_info struct

	error = TC(BF_GetBlock(fileDescriptor, 0, block));  // Get the first block
	if (error != 0) return NULL;

	char* data = BF_Block_GetData(block);  // Get the data of the first block
	HT_info* infoSaved = (HT_info*) data; // Cast the data to HT_info

	// If the file is not a Hash File, return NULL
	if (infoSaved->isHeapFile) { return NULL; }

	// Copy the data from the first block to the returning HT_info struct
	memcpy(toReturn, infoSaved, sizeof(HT_info)); 
	toReturn->fileDesc = fileDescriptor;


	error = TC(BF_UnpinBlock(block)); 	   // Unpin the first block because we don't need it anymore
	if (error != 0) return NULL;
	BF_Block_Destroy(&block); // Destroy the block

	printf("HT: Opened file\n");
    return toReturn;
}


int HT_CloseFile( HT_info* HT_inf ){
	int fileDescriptor = HT_inf->fileDesc; // Get the file descriptor
	int error;

	BF_Block* block;	BF_Block_Init(&block);
	
	printf("HT: Closed File\n");
	error = TC(BF_GetBlock(fileDescriptor, 0, block)); // Get the first block
	if (error != 0) return -1;

	char* data = BF_Block_GetData(block); 	// Get the data of the first block
	memcpy(data, HT_inf, sizeof(HT_info)); // Copy the data from the HT_info struct to the first block
	
	HT_info* other_info = (HT_info*) data;
	
	for(int i = 0; i < HT_inf->numBuckets; i++) {
		other_info->hashTable[i] = HT_inf->hashTable[i];
	}

	HT_info* info = (HT_info*) data;
	for(int i = 0; i < info->numBuckets; i++)
		assert(info->hashTable[i] == other_info->hashTable[i]);

	BF_Block_SetDirty(block);
	error = TC(BF_UnpinBlock(block));
	if (error != 0) return -1;


	// free(HT_info->hashTable);
	BF_Block_Destroy(&block);

	free(HT_inf); // Free the memory of the HT_info struct

	error = TC(BF_CloseFile(fileDescriptor)); // Close the file
	if (error != 0) return -1;


    return 0;
}

int hashFunc(int key, int buckets) {
	return key % buckets;
}

int HT_InsertEntry(HT_info* ht_info, Record record){
	
	int error;
	BF_Block* block; 		// Create a block
	BF_Block_Init(&block); // Initialize the block
	int fileDescriptor = ht_info->fileDesc; // Get the file descriptor
	int hash = hashFunc(record.id, ht_info->numBuckets); // Get the hash of the record
	int bucket = ht_info->hashTable[hash]; // Get the number of the bucket that contains the record
	int returnBlockId;

	error = TC(BF_GetBlock(fileDescriptor, bucket, block));
	if (error != 0) return -1;

	char* blockData = BF_Block_GetData(block);
	
	HT_block_info* blockInfoRead = (HT_block_info *) blockData;
	

	int recordsInBlock = blockInfoRead->currentRecords;
	// If records fits in block, just place it inside
	if (recordsInBlock < blockInfoRead->recordsCount) {
		char* data = blockData +  sizeof(HT_block_info) + recordsInBlock * (sizeof(Record));
		memcpy(data, &record, sizeof(Record));
		blockInfoRead->currentRecords++;
		BF_Block_SetDirty(block); // Mark the block as dirty
		
		// return the block id
		returnBlockId = bucket;

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
		HT_block_info* newBlockInfo = (HT_block_info *) newBlockData; // Cast the data to HT_block_info
		newBlockInfo->currentRecords = 1; // Set the current records to 1

		// Connect newly allocated block with the previous block in place
		newBlockInfo->nextBlock = bucket; // Set the next block to previous bucket (reverse chaining)		
		newBlockInfo->recordsCount = (sizeof(char) * BF_BLOCK_SIZE  - sizeof(HT_block_info) )/ sizeof(Record); // Set the records count to the maximum number of records that can fit in a block
		
		char* data = newBlockData +  sizeof(HT_block_info); // Get the data of the new block
		memcpy(data, &record, sizeof(Record)); // Copy the data from the record to the new block
		BF_Block_SetDirty(newBlock); // Mark the new block as dirty
		BF_UnpinBlock(newBlock); // Unpin the new block because we don't need it anymore
		BF_Block_Destroy(&newBlock); // Destroy the new block
		
		ht_info->hashTable[hash] = blockCounter; // Set the bucket to the new block
		
		// returnBlockId is the id of the last inserted block (saved in blockCounter)
		returnBlockId = blockCounter;
	}

	printf("Inserted: %d \t\t %s \t %s \t %s IN-> %d\n", record.id, record.name, record.surname, record.city, returnBlockId);
	
	error = TC(BF_UnpinBlock(block)); // Unpin the block because we don't need it anymore
	if (error != 0) return -1;
	
	BF_Block_Destroy(&block); // Destroy the block
	
	
    return returnBlockId; // Return the block id
}

int HT_GetAllEntries(HT_info* ht_info, int* value ){

	int error;
	int fileDescriptor = ht_info->fileDesc; // Get the file descriptor
	BF_Block* block; 		// Create a block
	BF_Block_Init(&block); // Initialize the block
	
	int blocksRead = 0;
	int hashValue = hashFunc((int) * ((int*) value), ht_info->numBuckets); // Get the hash of the value
	int bucket = ht_info->hashTable[hashValue];

	
	// Get block number of last allocated block ( = blockCounter - 1)
	error = TC(BF_GetBlock(fileDescriptor, bucket, block));
	if (error != 0) return -1;

	char* blockData = BF_Block_GetData(block);
	HT_block_info* info = (HT_block_info*) blockData;
	

	while ( true ) {
		// Iterate through all records of the bucket
		blocksRead++;

		for(int i = 0; i < info->currentRecords; i++) {
			// Check every record in block
			char* data = blockData +  sizeof(HT_block_info) + i * (sizeof(Record));
			Record rec = (Record) *( (Record*) data); // Cast the data to Record
			if (rec.id == (int) * ((int*)value) ) {
				printf("Found\n");
			}
		}
		// Check if there is a next block (overflow)
		if ( info->nextBlock == -1) 
			break;
		else {
			error = TC(BF_UnpinBlock(block));
			if (error != 0) return -1;
			
			error = TC(BF_GetBlock(fileDescriptor, info->nextBlock, block));
			if (error != 0) return -1;

			blockData = BF_Block_GetData(block);
			info = (HT_block_info*) blockData;
		}
	}

	BF_UnpinBlock(block);
	BF_Block_Destroy(&block);

    return blocksRead;
}

int HashStatisticsHT(char* filename) {
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
	HT_info* info = (HT_info*) blockData;

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
	// They begin with at least one block inside
	// int blocksInBucket[10];

	for(int i = 0; i < buckets; i++) {
		blocksInBucket[i] = 1;
		// printf("Init with: %d\n", blocksInBucket[i]);
		recordsInBuckets[i] = 0;
	} 

	for(int i = 0; i < buckets; i++) {
		int error;

		int bucket = info->hashTable[i];
		// printf("Currently at bucket: %d starting in block: %d\n", i, info->hashTable[i]);
		error = TC(BF_GetBlock(fileDesc, bucket, blockOfBucket));
		if (error != 0) return -1;

		void* data = BF_Block_GetData(blockOfBucket);
		HT_block_info* blockInfo = (HT_block_info*) data;
		
		recordsCount += blockInfo->currentRecords;
		recordsInBuckets[i] += blockInfo->currentRecords;
		while( true ) {
			if (blockInfo->nextBlock == -1)
				break;
			else {
				blocksInBucket[i]++;
				// printf("Now for bucket: %d counted: %d\n", i, blocksInBucket[i]);
				error = TC(BF_UnpinBlock(blockOfBucket));
				if (error != 0) return -1;

				error = TC(BF_GetBlock(fileDesc, blockInfo->nextBlock, blockOfBucket));
				if (error != 0) return -1;

				data = BF_Block_GetData(blockOfBucket);
				blockInfo = (HT_block_info*) data;
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

	printf("3. Average number of blocks per bucket: %d\n", (totalNumberOfBlocks/buckets));

	
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
	
	return 0;
}

MinMax findMinAndMax(int arr[], int N) {
	MinMax result = malloc(sizeof(minmax));

	int min = arr[0], max = arr[0];
	// Traverse the given array
    for (int i = 0; i < N; i++)
    {
        // If current element is smaller
        // than min then update it
        if (arr[i] < min)
        {
            min = arr[i];
        }
        // If current element is greater
        // than max then update it
        if (arr[i] > max)
        {
            max = arr[i];
        }
    }

	result->max = max;
	result->min = min;
	return result;
}

