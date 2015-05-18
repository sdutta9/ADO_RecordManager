#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>


//DataStructures
typedef struct RM_TableMgmtData
{
    int num_of_tuples;	// total number of tuples in the table
    int free_page;		// first free page which has empty slots in table
	BM_PageHandle ph;	// Buffer Manager PageHandle 
    BM_BufferPool bm;	// Buffer Manager Buffer Pool
	
} RM_TableMgmtData;

// Structure for Scanning tuples in Table
typedef struct RM_ScanMgmtData
{
    BM_PageHandle ph;
    RID rid; // current row that is being scanned 
    int count; // no. of tuples scanned till now
    Expr *condition; // expression to be checked
    
} RM_ScanMgmtData;
int count = 0;
int size = 35;
RM_TableMgmtData *tableMgmt;
// table and manager
extern RC initRecordManager (void *mgmtData){
	initStorageManager();	// initialize Storage Manager inside Record Manager
	return RC_OK;
}
extern RC shutdownRecordManager (){
	
	return RC_OK;
}


// Create Table with filename "name"
extern RC createTable (char *name, Schema *schema){
    SM_FileHandle fh;
     	
    tableMgmt = (RM_TableMgmtData*) malloc( sizeof(RM_TableMgmtData) ); // Allocate memory for table management data
    initBufferPool(&tableMgmt->bm, name, size, RS_LFU, NULL);

    char data[PAGE_SIZE];
	char *ph = data;
	 
	RC rc;
	int i;
	
	memset(ph, 0, PAGE_SIZE);
	*(int*)ph = 0; // Number of tuples
    ph+= sizeof(int); //increment char pointer
	
	*(int*)ph = 1; // First free page is 1 because page 0 is for schema and other info
    ph += sizeof(int); //increment char pointer
	
	*(int*)ph = schema->numAttr; //set num of attributes
    ph += sizeof(int); 

    *(int*)ph = schema->keySize; // set keySize of Attributes
    ph += sizeof(int);
	
	for(i=0; i<schema->numAttr; i++)
    {
       
       strncpy(ph, schema->attrNames[i], 10);	// set Attribute Name 
       ph += 10;

       *(int*)ph = (int)schema->dataTypes[i];	// Set the Data Types of Attribute
       ph += sizeof(int);

       *(int*)ph = (int) schema->typeLength[i];	// Set the typeLength of Attribute
       ph += sizeof(int);

    }
		
	rc = createPageFile(name);		// Create file for table using Storage Manager Function
	if(rc != RC_OK)
        return rc;
		
	rc = openPageFile(name, &fh);	// open Created File
	if(rc != RC_OK)
        return rc;
		
	rc=writeBlock(0, &fh, data);	// Write Schema To 0th page of file
	if(rc != RC_OK)
        return rc;
		
	rc=closePageFile(&fh);			// Closing File after writing
	if(rc != RC_OK)
        return rc;


    return RC_OK;
}


extern RC openTable (RM_TableData *rel, char *name){
  
    SM_PageHandle ph;    
    int numAttrs, i;
	
    rel->mgmtData = tableMgmt;		// Set mgmtData 
    rel->name = strdup(name); 		// Set mgmtData  
    
	pinPage(&tableMgmt->bm, &tableMgmt->ph, (PageNumber)0); // pinning the 0th page
	
	ph = (char*) tableMgmt->ph.data;	// set char to pointer to 0th page data 
	
	tableMgmt->num_of_tuples= *(int*)ph; 	// retrieve the total number of tuples from file
	//printf("Num of Tuples: %d \n", tableMgmt->num_of_tuples );
    ph+= sizeof(int);
    
	tableMgmt->free_page= *(int*)ph;	// retrieve the free page 
	//printf("First free page: %d \n", tableMgmt->free_page );
    ph+= sizeof(int);
	
    numAttrs = *(int*)ph;		// retrieve the number of Attributes  
	ph+= sizeof(int);
 	
 	// Set all values to Schema object
 	
    Schema *schema;
    schema= (Schema*) malloc(sizeof(Schema));
    
    schema->numAttr= numAttrs;
    schema->attrNames= (char**) malloc( sizeof(char*)*numAttrs);
    schema->dataTypes= (DataType*) malloc( sizeof(DataType)*numAttrs);
    schema->typeLength= (int*) malloc( sizeof(int)*numAttrs);

    for(i=0; i < numAttrs; i++)
       schema->attrNames[i]= (char*) malloc(10); //10 is max field length
      
    
   for(i=0; i < schema->numAttr; i++)
    {
      
       strncpy(schema->attrNames[i], ph, 10);
       ph += 10;
	   
	   schema->dataTypes[i]= *(int*)ph;
       ph += sizeof(int);

       schema->typeLength[i]= *(int*)ph;
       ph+=sizeof(int);
    }
	
	rel->schema = schema; // set schema object to rel object 	
    // Unpin after reading
    unpinPage(&tableMgmt->bm, &tableMgmt->ph);
    forcePage(&tableMgmt->bm, &tableMgmt->ph);
    return RC_OK;    
   
}   
   


//////////////////////////////////////////////////////////

extern RC closeTable (RM_TableData *rel){
	RM_TableMgmtData *tableMgmt;
	tableMgmt = rel->mgmtData;	// set rel->mgmtData value to tableMgmt before Closing
	shutdownBufferPool(&tableMgmt->bm);	//  Shutdown Buffer Pool 
	rel->mgmtData = NULL;	
	return RC_OK;
}

// Delete the Table  file
extern RC deleteTable (char *name){
	//free(tableMgmt);
	destroyPageFile(name);	// removing  file 
	return RC_OK;
}
extern int getNumTuples (RM_TableData *rel){
		
		RM_TableMgmtData *tmt;	 
    	tmt = rel->mgmtData;
	
	return tmt->num_of_tuples;
}

//function defined by Rakesh
// give First Free Slot of Particular Page 
int getFreeSlot(char *data, int recordSize)
{
	
    int i;
    int totalSlots = floor(PAGE_SIZE/recordSize); 

    for (i = 0; i < totalSlots; i++)
    {
    	
        if (data[i * recordSize] != '#'){
            
            //printf("data[ slot num : %d] contains: %c  \n\n", i, data[i * recordSize]);
            return i;
            
            }
    }
    return -1;
}


// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record){
	
	RM_TableMgmtData *tableMgmt;
	tableMgmt = rel->mgmtData;	
	
	RID *rid = &record->id;	// set rid from current Record 
	
	
	char *data;
	char * slotAddr;
	
	int recordSize = getRecordSize(rel->schema);	// record size of particular Record 
	
	rid->page = tableMgmt->free_page; // set First Free page to current page
	//printf("page number : %d \n", rid->page );
	pinPage(&tableMgmt->bm, &tableMgmt->ph, rid->page);	// pinning first free page 
	//printf("Create Record\n");
	
	data = tableMgmt->ph.data;	// set character pointer to current page's data
	rid->slot = getFreeSlot(data, recordSize);	// retrieve first free slot of current pinned page

	while(rid->slot == -1){
	unpinPage(&tableMgmt->bm, &tableMgmt->ph);	// if pinned page doesn't have free slot the unpin that page
	
	rid->page++;	// increment page number
	pinPage(&tableMgmt->bm, &tableMgmt->ph, rid->page);
	data = tableMgmt->ph.data;
	rid->slot = getFreeSlot(data, recordSize);
	}
	
	//printf(" \nSlot Number : %d \n", rid->slot);
	slotAddr = data;
	
	//write record
	markDirty(&tableMgmt->bm,&tableMgmt->ph);
	
	slotAddr += rid->slot * recordSize;
	*slotAddr = '#';
	slotAddr++;
	
	memcpy(slotAddr, record->data + 1, recordSize - 1);
	
	//////////print to check
/*	
	char *temp = slotAddr;
	printf("1ST ATTR: %d \t", *(int*)temp);
			temp += sizeof(int);
			
			char * string = (char *)malloc(5);
			
			strncpy(string,temp , 4);
			string[5] = '\0';
			
			printf("2nd ATTR: %s \t", string);
			temp += 4;
			
			printf("3RD ATTR: %d \t", *(int*)temp);
			
		//	free(string);
	
		
		
*/		
	//////////////////////////////
		
	
	unpinPage(&tableMgmt->bm, &tableMgmt->ph);
	
	tableMgmt->num_of_tuples++;
		
	/////////////////////////////////////////////// to be removed
	
	pinPage(&tableMgmt->bm, &tableMgmt->ph, 0);
	data = tableMgmt->ph.data;
	
	//unpinPage(&tableMgmt->bm, &tableMgmt->ph);
		
	return RC_OK;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Delete Record From Table having particular RID

extern RC deleteRecord (RM_TableData *rel, RID id){

	RM_TableMgmtData *tableMgmt = rel->mgmtData;
    pinPage(&tableMgmt->bm, &tableMgmt->ph, id.page); // pinning page which have record that needs to be deleted
	tableMgmt->num_of_tuples--; //update number of tuples
	tableMgmt->free_page = id.page; //update first free page
	
	char * data = tableMgmt->ph.data; // set character pointer to pinned page data
	*(int*)data =  tableMgmt->num_of_tuples; // retrieve number of tuples

	int recordSize = getRecordSize(rel->schema);  // get Record Size
	data += id.slot * recordSize; // set pointer to perticular slot of record
	
	*data = '0'; // set tombstone '0' for deleted record
		
	markDirty(&tableMgmt->bm, &tableMgmt->ph); // mark page as Dirty because of deleted record
	unpinPage(&tableMgmt->bm, &tableMgmt->ph); // unpin the page after deletion
	//move to slot
    
    // need to reduce the number of tuples in 0th page

	return RC_OK;
}

// Update particular Record of Table
extern RC updateRecord (RM_TableData *rel, Record *record){
		
	RM_TableMgmtData *tableMgmt = rel->mgmtData;
	pinPage(&tableMgmt->bm, &tableMgmt->ph, record->id.page); // pinning the age have record which we need to Update
	char * data;
	RID id = record->id;
	
	data = tableMgmt->ph.data;
	int recordSize = getRecordSize(rel->schema); // get Record size
	data += id.slot * recordSize; // set pointer to desire slot
	
	*data = '#'; // set tombstone as '#' because it will become non-empty record
    
	data++; // increment data pointer by 1
	
	memcpy(data,record->data + 1, recordSize - 1 ); // write new record to old record means update the record with new record
	
	markDirty(&tableMgmt->bm, &tableMgmt->ph); // mark page as dirty because record is updated
	unpinPage(&tableMgmt->bm, &tableMgmt->ph); // unpinning the page after updation
	
	return RC_OK;	
}

// get particular record from Table
extern RC getRecord (RM_TableData *rel, RID id, Record *record){
	RM_TableMgmtData *tableMgmt = rel->mgmtData;
		
	//printf("\nId.Page %d\n Id.Slot : %d\n",id.page,id.slot);
	//forceFlushPool(&tableMgmt->bm);
	
	pinPage(&tableMgmt->bm, &tableMgmt->ph, id.page); // pinning the age have record which we need to GET
		
	int recordsize = getRecordSize(rel->schema); // get Record Size
	char * slotAddr = tableMgmt->ph.data;
	slotAddr+=id.slot*recordsize;
	if(*slotAddr != '#')
		return RC_RM_NO_TUPLE_WITH_GIVEN_RID; // return code for not matching record in the table
	else{
		char *target = record->data; // set pointer to data of records
		*target='0';
		target++;
		memcpy(target,slotAddr+1,recordsize-1); // het record data
		record->id = id; // set Record ID
        
        // Comment The code of Print
		
		char *slotAddr1 = record->data;
		
		// printf("\n Tomestone : %c", *slotAddr1);
        
		char *temp1 = slotAddr1+1; // increment pointer to next attribute
        
		// printf("\t1ST ATTR: %d \t", *(int*)temp1);
		temp1 += sizeof(int);
		char * string = (char *)malloc(5);
		strncpy(string,temp1 , 4);
		string[4] = '\0';
		printf("2nd ATTR: %s \t", string);
		temp1 += 4;
		printf("3RD ATTR: %d \n", *(int*)temp1);
		free(string);
		
	}
	unpinPage(&tableMgmt->bm, &tableMgmt->ph); // Unpin the page after getting record
	return RC_OK;
}

// scan is Start with this method
int flag;
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
	
	
		closeTable(rel);
   		openTable(rel, "test_table_r");
 
    	RM_ScanMgmtData *scanMgmt;
    	scanMgmt = (RM_ScanMgmtData*) malloc( sizeof(RM_ScanMgmtData) ); //allocate memory for scanManagement Data
    	
    	scan->mgmtData = scanMgmt;
    	
    	scanMgmt->rid.page= 1;      // start scan from 1st page
    	scanMgmt->rid.slot= 0;      // start scan from 1st slot
    	scanMgmt->count= 0;         // count of n number of scan
    	scanMgmt->condition = cond; // set the condition of scan
    	RM_TableMgmtData *tmt;
    	 
    	tmt = rel->mgmtData;
    	tmt->num_of_tuples = 10;    // set the number of tuples
    	scan->rel= rel;
    	flag = 0;
	return RC_OK;
}

extern RC next (RM_ScanHandle *scan, Record *record){
	
	
	RM_ScanMgmtData *scanMgmt; 
	scanMgmt = (RM_ScanMgmtData*) scan->mgmtData;
    RM_TableMgmtData *tmt;
    tmt = (RM_TableMgmtData*) scan->rel->mgmtData;	//tableMgmt;
    
     Value *result = (Value *) malloc(sizeof(Value));
   
  	 static char *data;
   
     int recordSize = getRecordSize(scan->rel->schema);
     int totalSlots = floor(PAGE_SIZE/recordSize);
  //  printf("\n\nOutside while with total tuples: %d\n\n",tmt->num_of_tuples);
    	if (tmt->num_of_tuples == 0)
    	    return RC_RM_NO_MORE_TUPLES;
  

	while(scanMgmt->count <= tmt->num_of_tuples ){  //scan until all tuples are scanned 
	//	printf("\n\nInside while with scan count : %d , tot Tuples : %d \n\n",scanMgmt->count, tmt->num_of_tuples );
		if (scanMgmt->count <= 0)
		{
		//    printf("Inside scanMgmt->count <= 0 \n");
		    scanMgmt->rid.page = 1;
		    scanMgmt->rid.slot = 0;
		    
		    pinPage(&tmt->bm, &scanMgmt->ph, scanMgmt->rid.page);
		    data = scanMgmt->ph.data;
		}else{
			scanMgmt->rid.slot++;
			if(scanMgmt->rid.slot >= totalSlots){
		//	printf("SLOTS FULLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL\n");
			scanMgmt->rid.slot = 0;
			scanMgmt->rid.page++;
			}
			
			pinPage(&tmt->bm, &scanMgmt->ph, scanMgmt->rid.page);
		    data = scanMgmt->ph.data;
		}
        
		data += scanMgmt->rid.slot * recordSize;
	//	printf("\tSLOT NUMBER : %d \t",scanMgmt->rid.slot );
	//	printf("PAGE NUMBER : %d \n",scanMgmt->rid.page );
		
		record->id.page=scanMgmt->rid.page;
		record->id.slot=scanMgmt->rid.slot;
		//memcpy(record->data, data, recordSize);
		scanMgmt->count++;
		
		char *target = record->data;
		*target='0';
		target++;
		
		memcpy(target,data+1,recordSize-1);
	
	/*	
		char *slotAddr1 = record->data;
	    printf("\n Tombstone : %c", *slotAddr1);
		char *temp1 = slotAddr1+1;
		printf("\t1ST ATTR: %d \t", *(int*)temp1);
		temp1 += sizeof(int);
		char * string = (char *)malloc(5);
		strncpy(string,temp1 , 4);
		string[4] = '\0';
		printf("2nd ATTR: %s \t", string);
		temp1 += 4;
		printf("3RD ATTR: %d \t", *(int*)temp1);
		free(string);
	*/
		      
		if (scanMgmt->condition != NULL){
			evalExpr(record, (scan->rel)->schema, scanMgmt->condition, &result); 
			//printf("result: %s \n",result->v.boolV);
			}
		
		if(result->v.boolV == TRUE){  //v.BoolV is true when the condition checks out
		    unpinPage(&tmt->bm, &scanMgmt->ph);
		    flag = 1;
			return RC_OK;  //return RC_Ok
		}
	}
    
	    unpinPage(&tmt->bm, &scanMgmt->ph);
	    scanMgmt->rid.page = 1;
	    scanMgmt->rid.slot = 0;
	    scanMgmt->count = 0;
            
	return RC_RM_NO_MORE_TUPLES;
       
}

/// Close Scan after finishing it
extern RC closeScan (RM_ScanHandle *scan){
	RM_ScanMgmtData *scanMgmt= (RM_ScanMgmtData*) scan->mgmtData;
	RM_TableMgmtData *tableMgmt= (RM_TableMgmtData*) scan->rel->mgmtData;
	
	//if incomplete scan
	if(scanMgmt->count > 0){
	unpinPage(&tableMgmt->bm, &scanMgmt->ph); // unpin the page
	 scanMgmt->rid.page= 1; // reset scanMgmt to 1st page
     scanMgmt->rid.slot= 0; // reset scanMgmt to 1st slot
     scanMgmt->count = 0; // reset count to 0
	}
	
        // Free mgmtData
        
    	scan->mgmtData= NULL;
    	free(scan->mgmtData);  
	return RC_OK;
}


// dealing with schemas and give the Record Size
extern int getRecordSize (Schema *schema){
	int offset = 0, i = 0; // offset set to zero
	for(i = 0; i < schema->numAttr; i++){ // loop for total number of attribute
		switch(schema->dataTypes[i]){  // check the data types of attributes
			 case DT_STRING:
				offset += schema->typeLength[i];  // if data types is string then increment offset according to its typeLength
				break;
			  case DT_INT:
				offset += sizeof(int); // if data types is INT then increment offset to size of INTEGER
				break;
			  case DT_FLOAT:
				offset += sizeof(float); // if data types is FLOAT then increment offset to size of FLOAT
				break;
			  case DT_BOOL:
				offset += sizeof(bool); // if data types is BOOLEAN then increment offset to size of BOOLEAN
				break;
		}
	}
	
	offset += 1; // plus 1 for end of string
	return offset;
}

// Create Schema of Table
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
	Schema *result = (Schema *)malloc(sizeof(Schema)); // allocate memory to schema
	result->numAttr = numAttr; // set Number of Attribute
	result->attrNames = attrNames; // set Attribute Names
	result->dataTypes =dataTypes; // set  Attribute's data types
	result->typeLength = typeLength; // set Attribute's type length
	result->keySize = keySize;  // set Key size
	result->keyAttrs = keys; // set key attribute
	return result; 
}

// Free schema
extern RC freeSchema (Schema *schema){
	free(schema); // di-allocate the memory of schema
	return RC_OK;
}

// dealing with records and attribute values


// Creating a Blank record in Memory
extern RC createRecord (Record **record, Schema *schema)
{
    Record *tempRecord = (Record*) malloc( sizeof(Record) ); // allocate memory to empty record
	int recordSize = getRecordSize(schema); // get Record Size
    
	tempRecord->data= (char*) malloc(recordSize); // Allocate memory for data of record
    char * temp = tempRecord->data; // set char pointer to data of record
	*temp = '0'; // set tombstone '0' because record is still empty
	
    temp += sizeof(char);
	*temp = '\0'; // set null value to record after tombstone
	
    tempRecord->id.page= -1; // page number is not fixed for empty record which is in memory
    tempRecord->id.slot= -1; // slot number is not fixed for empty record which is in memory

    *record = tempRecord; // set tempRecord to Record
    return RC_OK;
}

// return position of particular attribute
RC attrOffset (Schema *schema, int attrNum, int *result)
{
  int offset = 1;
  int attrPos = 0;
  
  for(attrPos = 0; attrPos < attrNum; attrPos++)  // loop for number of attribute
    switch (schema->dataTypes[attrPos]) // check dataTypes of attributes
      {
      case DT_STRING:
	offset += schema->typeLength[attrPos]; // if data types is string then increment offset according to its typeLength
	break;
      case DT_INT:
	offset += sizeof(int); // if data types is INT then increment offset to size of INTEGER
	break;
      case DT_FLOAT:
	offset += sizeof(float); // if data types is FLOAT then increment offset to size of FLOAT
	break;
      case DT_BOOL:
	offset += sizeof(bool); // if data types is BOOLEAN then increment offset to size of BOOLEAN
	break;
      }
  
  *result = offset;
  return RC_OK;
}

// Free Record after used
extern RC freeRecord (Record *record){
	free(record); // Free memory of record
	return RC_OK;
}

// get Attribute from record
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){

	int offset = 0; 
	
	attrOffset(schema, attrNum, &offset); // to offset to the givent attribute number
	
    Value *tempValue = (Value*) malloc(sizeof(Value)); // allocate memory to value object

	char *string = record->data;
	
	string += offset;
//	printf("\n attrNum : %d \n\n", attrNum);
	if(attrNum == 1){
	schema->dataTypes[attrNum] = 1;
	}
	
	 switch(schema->dataTypes[attrNum])
    {
    case DT_INT: // get attribute value from record of type Integer
      {
		int val = 0;
		memcpy(&val,string, sizeof(int));
		tempValue->v.intV = val;
		tempValue->dt = DT_INT;
      }
      break;
    case DT_STRING: // get attribute value from record of type String
      {
     // printf("\n\n DT_STRING \n");
    tempValue->dt = DT_STRING;
	
	int len = schema->typeLength[attrNum];
	tempValue->v.stringV = (char *) malloc(len + 1);
	strncpy(tempValue->v.stringV, string, len);
	tempValue->v.stringV[len] = '\0';
	//printf("STRING : %s \n\n", tempValue->v.stringV);
	
	
      }
      break;
    case DT_FLOAT: // get attribute value from record of type Float
      {
      tempValue->dt = DT_FLOAT;
	  float val;
	  memcpy(&val,string, sizeof(float));
	  tempValue->v.floatV = val;
      }
      break;
    case DT_BOOL: // get attribute value from record of type Boolean
      {
	  tempValue->dt = DT_BOOL;
	  bool val;
	  memcpy(&val,string, sizeof(bool));
	  tempValue->v.boolV = val;
      }
      break;
    default:
      	printf("NO SERIALIZER FOR DATATYPE \n\n\n\n");
    }
    
    
 	
 	*value = tempValue;
	return RC_OK;
}

// Set the Attribute value into the Record
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
		int offset = 0;
		attrOffset(schema, attrNum, &offset); // retrieve the attribute offset
		char *data = record->data;
		data += offset;
		
		switch(schema->dataTypes[attrNum])
		{
		case DT_INT: // set the attribute value of type Integer
			*(int *)data = value->v.intV;	  
			data += sizeof(int);
		  	break;
		case DT_STRING: // set the attribute value of type String
		  {
			char *buf;
			int len = schema->typeLength[attrNum]; // length of string
			buf = (char *) malloc(len + 1); // allocate memory to buffer
			strncpy(buf, value->v.stringV, len); // copy string to buffer
			buf[len] = '\0';
			strncpy(data, buf, len); // copy data to buffer
			free(buf); // free memory of buffer
			data += schema->typeLength[attrNum];
		  }
		  break;
		case DT_FLOAT: // set the attribute value of type Float
		  {
			*(float *)data = value->v.floatV;	// set value of attribute
			data += sizeof(float); // increment  the data pointer
		  }
		  break;
		case DT_BOOL: // set the attribute value of type Boolean
		  {
			*(bool *)data = value->v.boolV;	// copy the boolean value
			data += sizeof(bool);
		  }
		  break;
		default:
		  printf("NO SERIALIZER FOR DATATYPE");
		}
				
	return RC_OK;
}

