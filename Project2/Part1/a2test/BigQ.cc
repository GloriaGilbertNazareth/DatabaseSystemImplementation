#include "BigQ.h"
#include<stdlib.h>
#include<vector>
#include <algorithm>
#include<map>
#include<cmath>
#include <queue> 

using namespace std;
OrderMaker sort_order;

//sorting using inbuilt function
int BigQ::sortRunlens(vector<Record*>& recVector, OrderMaker* sortorder){
	try{
		std::sort(recVector.begin(),recVector.end(),Sort(sortorder));
	}
	catch(const std:: exception& e){
		return 0;
	}
	return 1;

}
void BigQ::addRecords(vector<Record*>& recVector, vector<Page*>& pageVector){
	Record* temp; // used as a temporary object to add records to record vector
	for(int i=0;i<pageVector.size();i++){
		temp = new(std::nothrow) Record();
		while(pageVector[i]->GetFirst(temp)){
			recVector.push_back(temp);
			temp = new(std::nothrow) Record();
		}
	delete temp;
	delete pageVector[i];
	pageVector[i] = NULL;
	temp = NULL;
	}
}

int BigQ::writeSortedRun(map<int,Page*>& overflow, File& f, vector<Record*> recVector, int runlen, int& pageCount){
	
	int pageCount1=0;
	Page* filePage;
	filePage = new(std::nothrow) Page();
	for(int i=0;i<recVector.size();i++){
		if(!filePage->Append(recVector[i])){
			pageCount1++;
			f.AddPage(filePage,pageCount++);
			filePage->EmptyItOut();
			filePage->Append(recVector[i]);
		}
		delete recVector[i];
		recVector[i] = NULL;			
	}
	if(pageCount1<runlen){
		f.AddPage(filePage,pageCount++);
	}
	else{
		overflow[pageCount-1] = filePage;

	}
	filePage=NULL;
	return pageCount;

}

void BigQ::divideRuns(Pipe *in, int runlen, map<int,Page*>& overflow, OrderMaker* sortorder, File& f, int& pageCount){
	//cout<<"diving data in rulens..."<<endl;
	int count=0; //count for runlens
	Page* page = new(std::nothrow) Page(); //to store records temporarily and add it to vector
	Record rec;
	vector<Record*> recVector; //vector to save records
	vector<Page*> pageVector;//vector to save pages of a run
	Record* temp; // used as a temporary object to add records to record vector
	
	//temporary file to maintain sorted records in a run
	char tempFile[100];
	sprintf(tempFile,"temp%d.bin",rand());
	f.Open(0,tempFile);
	while(true){
		if(in->Remove(&rec) && count<runlen){
			//adding records to pages to count the number pf pages and getting pages in PageVector<=runLen
			if(!page->Append(&rec)){
				pageVector.push_back(page);
				page = new(std::nothrow) Page();
				page->Append(&rec);
				count++; //incrementing run counts
			}
		}
		else{
			 //adding the last incomplete page to vector
			if(count<runlen){
				pageVector.push_back(page);
			}
			//add records from pages in the pageVector to recVector
			addRecords(recVector, pageVector);
			//sorting the runlens
			sortRunlens(recVector, sortorder);
			//writing runs to file
			writeSortedRun(overflow, f, recVector, runlen,pageCount);
			recVector.clear();
			pageVector.clear();
		
			if(count>=runlen){
				page->Append(&rec);
				count =0;
				continue;
			}
			else{
				break;
			}

		}
	}
}
void BigQ::mergeRuns(int pageCount, Pipe* out, int runlen, File& f, OrderMaker* sortorder, map<int,Page*> overflow){
	//cout<<"Merging runslens..."<<endl;
	int numRuns =0;
	if(pageCount!=0){
		numRuns = (std::ceil((float)pageCount/runlen));
	}
	
		int numPagesLastRun = pageCount - (runlen*(numRuns-1));
    	priority_queue <Record*, vector<Record*>, Sort_Merge> pqueue(sortorder);  //used for kway merging
	    map<Record*,int> recRunIndexMap; //will map the records to the run index
	    Page** pageArray=new (std::nothrow) Page*[numRuns];
	    int* pageIndex=new (std::nothrow) int[numRuns];

	    int pageNum =0; //counter to add pages to pageArray
	    //adding first elements from each runlen to the queue
	    for(int i=0;i<numRuns;i++){
	    	pageIndex[i] =1; //as its the first page
	    	pageArray[i] = new (std::nothrow) Page();
	    	f.GetPage(pageArray[i] , pageNum);
	    	Record* rec = new (std::nothrow) Record();
	    	pageArray[i]->GetFirst(rec);
	    	pqueue.push(rec);
	    	recRunIndexMap[rec] = i;
	    	rec =NULL;
	    	pageNum = pageNum+runlen;
	    }

	    while(!pqueue.empty()){
	    	Record* record = pqueue.top();
	    	pqueue.pop();

	    	int runIndex = -1;
	    	runIndex = recRunIndexMap[record];
	    	if(runIndex ==-1){
	    		cout<<"Error in fetching the run index.";
	    		break;
	    	}
	    	Record* nextRecord = new (std::nothrow) Record();
	    	bool flagRecAvailable = true;

	    	if(!pageArray[runIndex]->GetFirst(nextRecord)){
	    		if(((runIndex<numRuns-1 && pageIndex[runIndex]<runlen)||(runIndex==numRuns-1 && pageIndex[runIndex]<numPagesLastRun))){
	    			f.GetPage(pageArray[runIndex],pageIndex[runIndex]+(runIndex*runlen));
	    			pageArray[runIndex]->GetFirst(nextRecord);
	    			pageIndex[runIndex]++;

	    		}
	    		else{
	    			if(pageIndex[runIndex] == runlen){
	    				if(overflow[((runIndex+1)*runlen) -1]){
	    					delete pageArray[runIndex];
	    					pageArray[runIndex] =NULL;
	    					pageArray[runIndex] = overflow[((runIndex+1)*runlen) -1];
	    					overflow[((runIndex+1)*runlen) -1] = NULL;
	    					pageArray[runIndex]->GetFirst(nextRecord);	
	    				}
	    				else{
	    					flagRecAvailable = false;
	    				}
	    			}
	    			else{
	    				flagRecAvailable = false;
	    			}
	    		}
	    	}
	    	if(flagRecAvailable){
	    		pqueue.push(nextRecord);
	    	}
	    	recRunIndexMap[nextRecord] = runIndex;
	    	out->Insert(record);
	    }

}

void *workerRoutine (void* arg) {
	BigQ *bigQ = (BigQ*) arg;
	Pipe* in = bigQ->in;
	Pipe* out = bigQ->out;
	int runlen = bigQ->runlen;
	OrderMaker* sortorder = bigQ->sortorder;

	//temporary file for saving sorted runlen records
	File f;
	int pageCount =0;
	map<int, Page*> overflow;
	bigQ->divideRuns(in, runlen, overflow,sortorder, f, pageCount);

	// construct priority queue over sorted runs and dump sorted data 
    // map for record and page arrayindex
    // i ->current page number
	bigQ->mergeRuns(pageCount, out, runlen, f, sortorder, overflow);
	
	f.Close();
    // finally shut down the out pipe
    out->ShutDown ();
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen){
	// read data from in pipe sort them into runlen pages
	if(runlen<=0){
		out.ShutDown ();
		return;
	}
	pthread_t worker;
	this->in = &in;
	this->out = &out;
	this->sortorder = &sortorder;
	this->runlen = runlen;
	pthread_create (&worker, NULL, workerRoutine, this);

	
	
}
BigQ::BigQ(){

}

BigQ::~BigQ () {
}
