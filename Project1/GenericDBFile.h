#ifndef GENERICDBFILE_H
#define GENERICDBFILE_H
#include <iostream>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"


typedef enum {heap, txt} fType;

class GenericDBFile {

public:
    //Class Constructor
	GenericDBFile ();
    //Class Destructor
    ~GenericDBFile (); 

virtual	int Create (char *fpath, fType file_type, void *startup);
virtual	int Open (char *fpath);
virtual	int Close ();
virtual	void Load (Schema &myschema, char *loadpath);
virtual	void Add (Record &addme);
virtual	void MoveFirst ();
virtual	int GetNext (Record &fetchme);
virtual	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif