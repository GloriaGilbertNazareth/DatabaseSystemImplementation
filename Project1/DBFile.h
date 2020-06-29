#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "HeapFile.h"
#include "GenericDBFile.h"
// stub DBFile header..replace it with your own DBFile.h 

class DBFile {
private:
	GenericDBFile *gDBFile;
public:
	DBFile ();  // constructor
	~DBFile(); //destructor
	/*
	This function will create a bin file for the heap database file and will also create
	a meta file containing the type of file it is
	*/
	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();
	void Load (Schema &myschema, const char *loadpath);
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif