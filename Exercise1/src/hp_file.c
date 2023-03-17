#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hp_file.h"
#include "record.h"
#include <assert.h>

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}

int HP_CreateFile(char *fileName){

	/*
	Record block structure:
	_____________________________________________________
	|						|							|
	|						|							|
	|	HP Info Header		|		nextBlock			|
	|		(HP_Info)		|			(int)			|
	|						|							|
	-----------------------------------------------------

	*/

	HP_info info;
	int fileDescriptor;
	int blockCounter;
	int oldBlockCounter;
	BF_Block* block; BF_Block_Init(&block);
	char* data;
	int error;


	error = TC(BF_CreateFile(fileName));
	error += TC(BF_OpenFile(fileName, &fileDescriptor));
	error += TC(BF_GetBlockCounter(fileDescriptor, &blockCounter));
	if (error != 0) return -1;
	
	oldBlockCounter = blockCounter;

	// Allocate the first block, in which we save
	// metadata
	error += TC(BF_AllocateBlock(fileDescriptor, block));
	error += TC(BF_GetBlockCounter(fileDescriptor, &blockCounter));
	if (error != 0) return -1;

	assert(oldBlockCounter + 1 == blockCounter);

	// Info we want to keep in the header as metadata
	info.fileDesc = fileDescriptor;
	info.headerPosition = oldBlockCounter;
	info.isHash = false;
	info.isHeapFile = true;
	info.recordsPerBlock = ( sizeof(char) * BF_BLOCK_SIZE - sizeof(HP_block_info ) ) / sizeof(Record);
	info.lastBlock = -1;
	info.nextBlock = -1;

	printf("Records per block = %d\n", info.recordsPerBlock);
	
	error += TC(BF_GetBlock(fileDescriptor, blockCounter - 1, block));
	if (error != 0) return -1;
	data = BF_Block_GetData(block);
	
	memcpy(data, &info, sizeof(HP_info));
	
	BF_Block_SetDirty(block);
	error = TC(BF_UnpinBlock(block));
	if (error == -1) return -1;

	BF_Block_Destroy(&block);

	error = TC(BF_CloseFile(fileDescriptor));
	if (error == -1 ) return -1;

}

HP_info* HP_OpenFile(char *fileName){
    
	int fileDescriptor;
	int error;
	char* data;
	BF_Block* block; BF_Block_Init(&block);
	HP_info* toReturn = (HP_info* ) malloc(sizeof(HP_info));

	error = TC(BF_OpenFile(fileName, &fileDescriptor));
	if (error == -1) return NULL;

	error = TC(BF_GetBlock(fileDescriptor, 0, block));
	if (error == -1) return NULL;
	
	data = BF_Block_GetData(block);
	HP_info* infoSaved = (HP_info *) data;

	// Assert we are talking about a heap file
	if ( infoSaved->isHash ) { return NULL; }
	assert(infoSaved->isHeapFile);

	memcpy(toReturn, infoSaved, sizeof(HP_info));
	toReturn->fileDesc = fileDescriptor;

	error = TC(BF_UnpinBlock(block));
	if (error == -1) return NULL;
	BF_Block_Destroy(&block);

	return toReturn;
}


int HP_CloseFile( HP_info* hp_info ){
	int fileDescriptor = hp_info->fileDesc; // Get the file descriptor
	BF_Block* block;	BF_Block_Init(&block);
	int error;	

	BF_GetBlock(fileDescriptor, 0, block); // Get the first block
	char* data = BF_Block_GetData(block); 	// Get the data of the first block
	memcpy(data, hp_info, sizeof(hp_info)); // Copy the data from the hp_info struct to the first block

	BF_Block_SetDirty(block);
	error = TC(BF_UnpinBlock(block));
	if (error == -1) return -1;

	BF_Block_Destroy(&block);

	free(hp_info); // Free the memory of the hp_info struct
	
	error = TC(BF_CloseFile(fileDescriptor));
	if (error == -1) return -1;
	
	return 0;
}

int HP_InsertEntry(HP_info* hp_info, Record record){

	/*
	Record block structure:
	________________________________________________________________________________
	|						|							|			|				|
	|						|							|			|				|
	|	recordsInBlock		|	Pointer to next block	|	Record1	|	Record...	|
	|		(int)			|			(int)			|			|				|
	|						|							|			|				|
	---------------------------------------------------------------------------------

	________________________________________________________________________________
	|			|			|		|												|
	|			|			|		|												|
	|	Rec[0]	| 	Rec[1] 	|	...	| 				HP__Block_info					|
	|	(Rec)	|			|		|		(recordsCount, pointer to block)		|
	|			|			|		|												|
	L_______________________________________________________________________________|
	^								^					^							^
	|								|					|							|
	data				data + BS - sizeof(HP_Block_info)
	(after Block_GetData)
*/
	int fileDescriptor;
	int error;
	char* data;
	int recordCounter;
	int blockCounter;
	BF_Block* block; BF_Block_Init(&block);

	fileDescriptor = hp_info->fileDesc;

	error = TC(BF_GetBlockCounter(fileDescriptor, &blockCounter));

	// Only the header metadata block is present
	if (blockCounter == 1) {
		printf("Inserting record, no blocks for records yet\n");
		int blockCounter;
		int nextBlock;

		error = TC( BF_AllocateBlock(fileDescriptor, block) );
		error = TC( BF_GetBlockCounter(fileDescriptor, &blockCounter));

		// Connect header to last block 
		// new_info->lastBlock = blockCounter - 1;
		hp_info->lastBlock = blockCounter - 1;
	
		// Get the data of the new allocated block
		hp_info->nextBlock = blockCounter - 1;
		nextBlock = hp_info->nextBlock;

		// Now get the new allocated block, saved at blockCounter - 1 position
		error = TC(BF_GetBlock(fileDescriptor, blockCounter-1, block));
		data = BF_Block_GetData(block);

		HP_block_info blockInfo;
		blockInfo.nextBlock = -1;
		blockInfo.currentRecords = 1;
		blockInfo.recordsCount = ( sizeof(char) * BF_BLOCK_SIZE - sizeof(HP_block_info) ) / sizeof(Record);

		// Copy the records inside
		memcpy(data, &record, sizeof(Record) );
		Record recInserted =  *( (Record*) data);

		// Go to the end minus 2 ints, to place how many records the block stores
		data += sizeof(char) * BF_BLOCK_SIZE - sizeof(HP_block_info);
		memcpy(data, &blockInfo, sizeof(HP_block_info));
		
		printf("Successfuly inserted: \n");
		printf("%d \t\t %s \t %s \t %s \n", record.id, record.name, record.surname, record.city);
		
		BF_Block_SetDirty(block);
		BF_UnpinBlock(block);
		BF_Block_Destroy(&block);

		return nextBlock;

	} else {
		// There exist more block records, not
		// only the header
		
		BF_Block* lBlock; BF_Block_Init(&lBlock);

		// Go to the last position
		error = TC(BF_GetBlock(fileDescriptor, hp_info->lastBlock, lBlock));
		data = BF_Block_GetData(lBlock);
		char* dataInit = data;	// save initial pointer

		
		data += sizeof(char) * BF_BLOCK_SIZE - (sizeof(HP_block_info));
		HP_block_info read = (HP_block_info) * ( (HP_block_info*) data);
		
		int nextBlock = read.nextBlock;
		int recordsInsideBlock = read.currentRecords;

		data = sizeof(Record) * recordsInsideBlock + dataInit;

		if (recordsInsideBlock < hp_info->recordsPerBlock) {
			// We have less records inside the block
			// than a block can take, then we can attach it to
			// the current block

			// Copy the record in the free spot
			memcpy(data, &record, sizeof(Record));

			// Now go the position of the HP_block_info
			data = dataInit + sizeof(char) * BF_BLOCK_SIZE - (sizeof(HP_block_info));
			HP_block_info* newInfo = (HP_block_info*) data;

			// Increment the records saved inside the block
			newInfo->currentRecords++;

			// Since the record is always added in the last block, its next must not exist
			assert(newInfo->nextBlock == -1);

			printf("Successfuly inserted: \n");
			printf("%d \t\t %s \t %s \t %s \n", record.id, record.name, record.surname, record.city);

			BF_Block_SetDirty(lBlock);
			BF_UnpinBlock(lBlock);
			BF_Block_Destroy(&lBlock);
			BF_Block_Destroy(&block);
			return hp_info->lastBlock;
		} else {
			// The last block is fully filled
			// So we have to allocate a new block to place the record into
			BF_Block* oldLast; BF_Block_Init(&oldLast);
			
			// Allocate new block
			BF_Block* allocatedBlock; BF_Block_Init(&allocatedBlock);

			// Allocation, and copy data into the newly allocated block

			error = TC(BF_AllocateBlock(fileDescriptor, allocatedBlock));
			if (error != 0) return -1;

			data = BF_Block_GetData(allocatedBlock);
			
			// Copy record
			memcpy(data, &record, sizeof(Record));

			printf("Successfuly inserted: \n");
			printf("%d \t\t %s \t %s \t %s \n", record.id, record.name, record.surname, record.city);

			// Go the HP_block_info position
			data += sizeof(char) * BF_BLOCK_SIZE - sizeof(HP_block_info);
			
			// Create the block_info for the newly allocated block
			HP_block_info info;
			info.currentRecords = 1;	// It only has 1 record inside, the one inserted above
			info.nextBlock = -1;		// And no next block
			info.recordsCount = (sizeof(char) * BF_BLOCK_SIZE - sizeof(HP_block_info) ) / sizeof(Record);
			
			// Copy the block_info in the block
			memcpy(data, &info, sizeof(HP_block_info));

			BF_Block_SetDirty(allocatedBlock);
			error = TC(BF_UnpinBlock(allocatedBlock));
			if (error != 0) return -1;

			// End of copying into newly allocated block


			// Now time to change 2 blocks:
			// 1) Header file's last block
			// 2) LastBlock's new next block pointer (now it's no longer the last one, so it has a next)

			int blockCounter;

			error = TC(BF_GetBlockCounter(fileDescriptor, &blockCounter));
			if (error != 0) return -1;

			// Save which is the last block, before we change it
			int oldLastBlock = hp_info->lastBlock;
			
			// Update the header's last block to the (counter - 1), a.k.a the newly allocated block
			hp_info->lastBlock = blockCounter - 1;

			// Now read the old last block, to change its
			// next block pointer to the newly allocated one
			error = TC(BF_GetBlock(fileDescriptor, oldLastBlock, oldLast));
			if (error != 0) return -1;

			data = BF_Block_GetData(oldLast);
			
			// data += sizeof(char) * BF_BLOCK_SIZE - sizeof(int);
			data += sizeof(char) * BF_BLOCK_SIZE - sizeof(HP_block_info);
			HP_block_info* oldBlockInfo = (HP_block_info*) data;

			// Its nextBlockCounter should have been -1, it hasn't changed yet
			assert( oldBlockInfo->nextBlock == -1);
			oldBlockInfo->nextBlock = hp_info->lastBlock;
			assert( oldBlockInfo->nextBlock == blockCounter - 1);


			BF_Block_SetDirty(oldLast);
			error = TC(BF_UnpinBlock(oldLast));
			if (error != 0) return -1;

			BF_Block_Destroy(&oldLast);

			BF_Block_Destroy(&block);
			BF_Block_Destroy(&lBlock);
			BF_Block_Destroy(&allocatedBlock);

			printf("Successfuly inserted: \n");
			printf("%d \t\t %s \t %s \t %s \n", record.id, record.name, record.surname, record.city);

			// End of changing header and last block's pointer
			return hp_info->lastBlock;
		}
	}
}

int HP_GetAllEntries(HP_info* hp_info, int value){
	int fileDescriptor;
	int error;
	char* data;
	BF_Block* block;
	BF_Block_Init(&block);


	fileDescriptor = hp_info->fileDesc;
	int nextBlock = hp_info->nextBlock;

	printf("\n%s \t\t %s \t %s \t %s\n", "ID", "NAME" , "SURNAME", "CITY");

	if (nextBlock == -1) return -1;

	int blocksRead = 1;
	bool found = false;

	while(nextBlock != -1) {
		
		error = TC(BF_GetBlock(fileDescriptor, nextBlock, block));
		data = BF_Block_GetData(block);

		char* dataInit = data;

		data += sizeof(char) * BF_BLOCK_SIZE - sizeof(HP_block_info);
		HP_block_info * infoRead = (HP_block_info*) data;
		
		int recordsInBlock = infoRead->currentRecords;
		nextBlock = infoRead->nextBlock;

		data = dataInit;

		for (int i = 0; i < recordsInBlock; i++) {
			Record recInside = *((Record*) data);
			
			if (recInside.id == value) {
				found = true;
				printf("FOUND!\n");
				printf("%d \t\t %s \t %s \t %s \n", recInside.id, recInside.name, recInside.surname, recInside.city);
				break;
			}
			data += sizeof(Record);
		}

		BF_UnpinBlock(block);
		if (found) break;
		blocksRead++;
	}


	BF_Block_Destroy(&block);
	return (found) ? blocksRead : -1;
}

int TC(BF_ErrorCode error) {
    if (error != BF_OK) {
        BF_PrintError(error);
        return (-1);
    } else {
        return 0;
    }
}
