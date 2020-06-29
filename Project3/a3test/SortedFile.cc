#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedFile.h"
#include "Defs.h"
#include <fstream>
#include <iostream>
#include "Pipe.h"
#include "GenericDBFile.h"
#include "BigQ.h"

using namespace std;

SortedFile::SortedFile () 
{	 
	mode=Read;
	sortedFile = new (std::nothrow) File();
	page = new (std::nothrow) Page();
	int pageNumber =0;
	int pageCount=0;
	runLength=0;
	OrderMaker orderMaker;

}

SortedFile::~SortedFile ()
{

}
//creates a file object and also a meta file and 
//writes the type of file, runlength and ordermaker object in it
int SortedFile::Create (char *f_path, fType f_type, void *startup) {
	cout<<"In sorted file create:::::::"<<endl;
	char metaFilePath[200];
    sprintf (metaFilePath, "%s.meta", f_path);
	ofstream out(metaFilePath);
    if(!out ) {
       cout << "Couldn't open file "  << endl;
    }	
    if(f_path==NULL)
		return 0; 
	SortOrderInfo *sortOrder;
	sortOrder = (SortOrder *)startup;
	runLength = sortOrder->runLength;
	orderMaker = *(OrderMaker *)(sortOrder->orderMaker);
	out<<"sorted"<<endl;
	out<<runLength<<endl;
	out.write((char*)&orderMaker, sizeof(orderMaker)); 
	sortedFile->Open(0, f_path); //sorted file
    sortedFile->AddPage(page,0);
    pageNumber=0;
    mode = Write;
    pageCount=1;
    return 1;
}

//opens the meta file and reads the runlength and ordermaker object
//opens the sorted file and moves to the first page
int SortedFile::Open(char *f_path)
{
	char metaFilePath[200];
    sprintf (metaFilePath, "%s.meta", f_path);
    ifstream myfile(metaFilePath);
    if(!myfile){
    	return 0;
    }
    char str[255];
	myfile.getline(str, 255);  // delim defaults to '\n'
	myfile.getline(str, 255);
	runLength = atoi(str);
	myfile.read((char*)&orderMaker, sizeof(orderMaker)); 
	cout<<"runLength is:::::"<<runLength<<endl;
	page->EmptyItOut();
	sortedFile->Open(1,f_path);
	mode=Read;
	pageCount = sortedFile->GetLength();
	MoveFirst();
	//orderMaker.Print();
	return 1;
}

//switches the mode to read and sets the page number to zero and retrives the 0th page
void SortedFile::MoveFirst ()
{
	SwitchMode(Read);
	page->EmptyItOut();
	pageNumber = 0;
	sortedFile->GetPage(page,pageNumber);

}

//switches the mode to read and closes the file
int SortedFile::Close ()
{
    SwitchMode(Read);
	page->EmptyItOut();
	sortedFile->Close();
	return 1;

}

//adds records to in pipe in order to sort them
void *producer (void *arg) {
	ProducerUtil *producerUtil = (ProducerUtil *) arg;
	Record temp;
	int counter = 0;
	if (producerUtil->fpath != NULL) 
	{ //if load, we read from tbl file
		FILE *tableFile = fopen(producerUtil->fpath, "r");
		while(temp.SuckNextRecord(producerUtil->schema, tableFile))
		{
			counter += 1;
			producerUtil->pipe->Insert (&temp);
		}
	}
	else
	{//adding all records from sorted file to pipe to sort properly
		int curPage =0;
		Page* pg = new(std::nothrow) Page();
		producerUtil->sortedFile->GetPage(pg,curPage);
		while(true) {
			if(!pg->GetFirst(&temp))
			{
				if(curPage < producerUtil->sortedFile->GetLength() -2)
				{
					curPage++;
					producerUtil->sortedFile->GetPage(pg,curPage);
					if(!pg->GetFirst(&temp))
					{
						break;
					}
				}
				else
				{
					break;
				}
			}

		producerUtil->pipe->Insert (&temp);
		counter++;
		}
	}
	producerUtil->pipe->ShutDown ();
}

//removes records from the pipe and adds it to the sortedfile
void *consumer (void *arg) 
{

	ProducerUtil *producerUtil = (ProducerUtil *) arg;
	ComparisonEngine ceng;
	Record rec;
    int cnt =0;
    Page* pg = new(std::nothrow) Page();
    int curPage =0;
	while (producerUtil->pipe->Remove (&rec)) 
	{
		cnt++;
		if (!pg->Append(&rec))
		{
			producerUtil->sortedFile->AddPage(pg,curPage);
			pg->EmptyItOut();
			pg->Append(&rec);
			curPage++;
		}
	}
	producerUtil->sortedFile->AddPage(pg,curPage);
	pg->EmptyItOut();
}

//sorts the already sorted and newly added records using BigQ and Pipes
void SortedFile::SortFileAgain() 
{
    int buffsz = 100; // pipe cache size
    Pipe input (buffsz);
    Pipe output (buffsz);
    pthread_t thread_1;
    ProducerUtil inp = {&input, &orderMaker,NULL, sortedFile, NULL  };
    pthread_create (&thread_1, NULL, producer, (void *)&inp);
    pthread_t thread_2;
    ProducerUtil outp = {&output, &orderMaker,NULL, sortedFile, NULL };
    pthread_create (&thread_2, NULL, consumer, (void *)&outp);
    /*cout<<"printing ordermake"<<endl;
    orderMaker.Print();*/
    //cout<<runLength<<endl;
    BigQ bq (input, output, orderMaker, runLength);
    pthread_join (thread_1, NULL);
    pthread_join (thread_2, NULL);
}

//using the perform the required transactions while transiting from read to write modes and vice versa
void SortedFile::SwitchMode(Mode newMode){
	if(mode == newMode)
	{
		return;
	}
	else
	{
		if(newMode == Read)
		{
			sortedFile->AddPage(page, pageNumber);
			SortFileAgain();
			mode = Read;
			return;
		}
		else
		{
			page->EmptyItOut();
			pageNumber = sortedFile->GetLength()-2;
			sortedFile->GetPage(page,pageNumber);
			mode = Write;
			return;
		}
	}	
} 

//Adds records to the file
void SortedFile::Add (Record &rec)
{
	SwitchMode(Write);
	if(!page->Append(&rec))
	{

		sortedFile->AddPage(page, pageNumber);
		page->EmptyItOut();
		pageNumber++;
		pageCount++;
		page->Append(&rec);
	}	
}

//loads the entire table data onto the file for sorting
void SortedFile::Load (Schema &schema, char *fpath) 
{
        
        //cout << "In load function switch called to W actual mode: " << status << endl;  
    SwitchMode(Write);
	int buffsz = 100; // pipe cache size
    Pipe input (buffsz);
    Pipe output (buffsz);
    pthread_t thread_1;
    ProducerUtil inp = {&input, &orderMaker,&schema, sortedFile, fpath  };
    pthread_create (&thread_1, NULL, producer, (void *)&inp);
    pthread_t thread_2;
    ProducerUtil outp = {&output, &orderMaker,&schema, sortedFile, fpath };
    pthread_create (&thread_2, NULL, consumer, (void *)&outp);
    BigQ bq (input, output, orderMaker, runLength);
    pthread_join (thread_1, NULL);
    pthread_join (thread_2, NULL);
	mode=Read;	

}

//Fetches the next record from the sorted file
int SortedFile::GetNext (Record &fetchme) {
	//cout << "In get next function switch called to R actual mode: " << status << endl;
	SwitchMode(Read);
	if(page->GetFirst(&fetchme))
	{
		return 1;
	}
	else
	{
		if(pageNumber < pageCount - 2)
		{
			pageNumber++;
			sortedFile->GetPage(page,pageNumber);
			if(GetNext(fetchme))
			{
				return 1;
			}	
		}
	return 0;
	}	
}

//retrives data in an efficient way from the sorted file
int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) 
{
	//cout << "In get next cnf function switch called to R actual mode: " << status << endl;
	SwitchMode(Read);
        ComparisonEngine comp;
	while(GetNext(fetchme)){
		if (comp.Compare (&fetchme, &literal, &cnf))
		{

			return 1;
		}		
	}
	return 0;
}
