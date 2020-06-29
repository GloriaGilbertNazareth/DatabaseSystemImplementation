#include "BigQ.h"
#include <vector>
#include <string>
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <algorithm>
#include<set>

class recordsCompare {
public:
	int operator() (Records *r1, Records *r2 ) {
	ComparisonEngine ce;
	int result = ce.Compare( &(r1->record), &(r2->record), r1->orderMaker );
	if (result < 0)
	 	return 1;
	else
	 	return 0;
	}
};

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

	inputPipe = &in;
	outputPipe = &out;
	sortingOrder = &sortorder;
	pthread_t sortingThread;
	runLength = runlen;
	fileName ="BigQtmp_001.bin";
	sortedFile.Open(0, fileName);
	currentPageNum=0;
	numberRuns=0;
	int output = pthread_create(&sortingThread, NULL, &Worker, (void*)this);
}

BigQ::~BigQ () {
}
//worker thread to perform TPMMS
void* BigQ:: Worker(void *arg)
{
	BigQ *bigQobj = (BigQ *) arg; //copying all set data to current object
	bigQobj -> sortRecords();
        
	pthread_exit(NULL);
}

//divides the data into runs and sorts the records using inbuilt sorting
//it then adds sorted runs to file amd calls merge 
void BigQ::sortRecords()
{
	vector<CompareRecords*> recVector;
    CompareRecords *temp;
	Record *getRecord;
	int pageCount=0;
    Page currentPage;
    getRecord = new Record;
    int count =0;
    //dividing, sorting and adding sorted runs to run vector
	while(inputPipe->Remove(getRecord))
	{               
        temp = new CompareRecords;
        (temp->record).Copy(getRecord);
        (temp->orderMaker) = sortingOrder;
		if(!currentPage.Append(getRecord)) 
		{
		    pageCount++;
            if (pageCount == runLength)
            {
               sort(recVector.begin(), recVector.end(), CompareRecords::compareRecords);   
               writeInFile(recVector);           
               recVector.clear();
               pageCount = 0;
            }
            else
            {
               currentPage.EmptyItOut(); 
               currentPage.Append(getRecord);
            }
        }
        recVector.push_back(temp);
        count++;
	}

	//sorting the last run of size < runlength
    if(recVector.size() != 0) 
    {
       	sort(recVector.begin(), recVector.end(), CompareRecords::compareRecords);
       	writeInFile(recVector);
       	recVector.clear();
    }
    inputPipe->ShutDown();
    //calling merge
    mergeRecords(); 
}

//writing the sorted runs into pages in a temp file and 
//keeping record of start and end page of each run
void BigQ :: writeInFile(vector<CompareRecords*> rcVector) 
{
	numberRuns++;
    Page page;
    RunMetaData *rmd = new RunMetaData;
    rmd->startPage = currentPageNum;
    vector<CompareRecords*>::iterator startRecord = rcVector.begin();
    vector<CompareRecords*>::iterator endRecord = rcVector.end();
    while(startRecord != endRecord) {
    
        if(!page.Append(&((*startRecord)->record))) 
        { 
           sortedFile.AddPage(&page, currentPageNum);
           currentPageNum++;
           page.EmptyItOut();
           page.Append( &((*startRecord)->record));
        } 
        startRecord++;
       }  
    //adding last incomplete page
	sortedFile.AddPage(&page, currentPageNum);
    page.EmptyItOut();
    rmd->endPage = currentPageNum; 
    runmetaDataVec.push_back(rmd); 
    currentPageNum++;
}

//merges the records in runs using priority queue
void BigQ::mergeRecords()
{	

	//cout <<"In Merge Records" << endl;
	int mergedRuns = 0; 
    int counter=0;
	vector<PageWrap*> pageVector; 
	PageWrap *page = NULL; 
	int curPageNum = 0; 

	//adding pagewraps to help merging runs
	for(int i=1; i<=numberRuns; i++) 
	{	
	   curPageNum = (runmetaDataVec[i-1])->startPage; 
	   page = new PageWrap;
	   sortedFile.GetPage( &(page->page), curPageNum); 
	   page->currentPage = curPageNum;
	   pageVector.push_back(page); 
	}

	//adding first record from each run onto the priority queue
	multiset<Records*, recordsCompare> mergeset; 
	Records *temp = NULL; 
	for(int j=0; j<numberRuns; j++)
	{	
	   temp = new Records; 
	   if(((pageVector[j])->page).GetFirst( &(temp->record)) != 0) 
	   {	   
	       temp->runIndex = (j+1);
	       (temp->orderMaker) = (this->sortingOrder); 
	       mergeset.insert(temp);
	   }
       else 
       {
           cerr<<"No first record found "<<endl;
           exit(0);
       } 
    }

	int runIndex;
	Records *tempWrp;
	while( mergedRuns < numberRuns )
    {
		tempWrp = *(mergeset.begin()) ; 
		mergeset.erase(mergeset.begin()); 
		runIndex = tempWrp->runIndex; 
		outputPipe->Insert( &(tempWrp->record)); 
	    counter++;
		if((pageVector[runIndex-1]->page).GetFirst( &(tempWrp->record) ) == 0) 
		{
			pageVector[runIndex-1]->currentPage++; 
			if(pageVector[runIndex-1]->currentPage <= runmetaDataVec[runIndex-1]->endPage ) 
		    {
			    sortedFile.GetPage(&(pageVector[runIndex-1]->page), pageVector[runIndex-1]->currentPage);
			    if( (pageVector[runIndex-1]->page).GetFirst( &(tempWrp->record) ) == 0 ) 
			    	{
			      	cerr<<"Empty page !"<<endl;
			      	exit(0);
			    }
			tempWrp->runIndex = runIndex; 
			mergeset.insert(tempWrp);
			}	
			else 
			{
			  	mergedRuns++; 
			}
		}
		else
		{
		 tempWrp->runIndex = runIndex; 
		 mergeset.insert(tempWrp);
		}
	}
	outputPipe->ShutDown(); 

}



int CompareRecords :: compareRecords (const void *rc1, const void *rc2) {
	CompareRecords *rcd1 = (CompareRecords *)rc1;
	CompareRecords *rcd2 = (CompareRecords *)rc2;
	ComparisonEngine ce;
	int result = ce.Compare(&(rcd1->record), &(rcd2->record), rcd2->orderMaker);
	if(result < 0) {
	        return 1;
	}
	else {
	return 0;
	}
}


