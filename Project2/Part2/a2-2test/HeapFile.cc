#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include<iostream>
#include "HeapFile.h"
#include "Defs.h"
#include<fstream>

using namespace std;

HeapFile::HeapFile(){
	heapFile = new File();
	writePage = new Page();
	readPage = new Page();
	//currentPage =0;
	//readPage = new Page();
}

HeapFile:: ~HeapFile(){
	delete heapFile;/*
	delete readPage;
	delete writePage;*/
}


void HeapFile::AddCurrentPage () {
	int currlen = heapFile->GetLength();
	int whichpage = currlen==0?0:currlen-1;
	heapFile->AddPage(writePage, whichpage);
	writePage->EmptyItOut();

	
}

int HeapFile::Create (char *f_path, fType f_type, void *startup) {
    heapFile->Open(0, f_path);
    return 1;
}
int HeapFile::Open (char *f_path) {
	heapFile->Open(1, f_path);
	//cout<<"heapfile length "<<heapFile->GetLength()<<endl;
    return 1;
}
int HeapFile::Close () {
	AddCurrentPage();
	cout<<"Length"<<heapFile->GetLength()<<endl;
	return heapFile->Close();
}
void HeapFile::Add(Record &record){
	int pageEmpty = writePage->Append(&record);
	if(pageEmpty==0)//page is full
	{
		int currentPageLength = heapFile->GetLength();
		int whichPage = currentPageLength ==0? 0 : currentPageLength-1;
		heapFile->AddPage(writePage,whichPage);
		writePage->EmptyItOut();
		writePage->Append(&record);
	}	
}
void HeapFile::Load (Schema &f_schema, char *loadpath) {
	Record record;
	FILE *tableRecords =fopen(loadpath,"r");
	if(tableRecords){
		while(record.SuckNextRecord(&f_schema, tableRecords) ==1){
			Add(record);
		}
		
	}
}
void HeapFile::MoveFirst () {
	currentPage =0; // set currentPage to 0, ie first page of the file.

}

int HeapFile::GetNext (Record &fetchme) {
	//cout<<"In GetNext "<<currentPage<<" "<< heapFile->GetLength()<<endl;
	if(readPage->GetFirst(&fetchme) ==0){ //no records in write page i.e current page copied to this page 
		if(currentPage<=heapFile->GetLength()-2){
			heapFile->GetPage(readPage,currentPage); //copying contents of current page to write page
			currentPage++;

		}
		else{
			return 0; //read last record of last page;;
		}
		readPage->GetFirst(&fetchme);
	}
	else{
		return 1;
	}
	
}

int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	int value =0;
	ComparisonEngine comp;
	do{
		if(GetNext(fetchme)==1){
			value = comp.Compare(&fetchme, &literal, &cnf);
		}
		else 
			return 0;

	}while(value==0);

	return value;
}
