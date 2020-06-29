#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include<iostream>
#include<fstream>
#include<stdlib.h>
#include<string.h>
#include "Defs.h"
#include "GenericDBFile.h"
#include "HeapFile.h"


// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {

}
DBFile::~DBFile(){
	//cout << "DBFile DESTRUCTOR" << endl;
	//delete gDBFile;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
	if(f_type!=heap){
		cout<<"Invalid file type.\n";
		return 0;
	}
	char metaFilePath[200];
    sprintf (metaFilePath, "%s.meta", f_path);
	ofstream out(metaFilePath);
    if(!out ) {
       cout << "Couldn't open file "  << endl;
    }	
    if(f_path==NULL)
		return 0;
	
	out << "heap";
	gDBFile = new HeapFile();
	gDBFile->Create(strdup(f_path), f_type, startup);
	cout<<"File "<< f_path <<" created successfully"<<endl;
	return 1;

}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
	gDBFile->Load(f_schema, strdup(loadpath));
}

int DBFile::Open (const char *f_path) {
	char metaFilePath[200];
    sprintf (metaFilePath, "%s.meta", f_path);
    ifstream myfile(metaFilePath);
    if(!myfile){
    	return 0;
    }
	gDBFile =new HeapFile();
	gDBFile->Open(strdup(f_path));
				
	return 1;
}

int DBFile::Close () {
	return gDBFile->Close();
}

void DBFile::MoveFirst () {
	gDBFile->MoveFirst();
}


void DBFile::Add (Record &rec) {
	gDBFile->Add(rec);	
}

int DBFile::GetNext (Record &fetchme) {
	gDBFile->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	gDBFile->GetNext(fetchme, cnf, literal);
}
