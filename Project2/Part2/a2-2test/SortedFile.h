#ifndef SORTEDFILE_H
#define SORTEDFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <fstream>
#include "GenericDBFile.h"
#include "Pipe.h"
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "DBFile.h"

using namespace std;

typedef struct {
        Pipe *pipe;
        OrderMaker *order;
        Schema *schema;
        File *sortedFile;
        char* fpath;


}ProducerUtil;


typedef struct SortOrderInfo{
    OrderMaker *orderMaker;
    int runLength;
}SortOrder; 

class SortedFile :virtual public  GenericDBFile {
	public:

        Mode mode;
		File* sortedFile;
		Page* page;
		int pageNumber;
		int pageCount;
		int runLength;
		OrderMaker orderMaker;


        SortedFile ();
        ~SortedFile();
        //creates a file object and also a meta file and 
        //writes the type of file, runlength and ordermaker object in it
        int Create (char *fpath, fType file_type, void *startup);
        
        //opens the meta file and reads the runlength and ordermaker object
        //opens the sorted file and moves to the first page
        int Open (char *fpath);

        //switches the mode to read and closes the file
        int Close ();

        //loads the entire table data onto the file for sorting
        void Load (Schema &schema, char *fpath);

        //using the perform the required transactions while transiting from read to write modes and vice versa
		void SwitchMode(Mode mode);

        //sorts the already sorted and newly added records using BigQ and Pipes
        void SortFileAgain();

        //switches the mode to read and sets the page number to zero and retrives the 0th page
        void MoveFirst ();

        //Adds records to the file
        void Add (Record &addme);

        //Fetches the next record from the sorted file
        int GetNext (Record &fetchme);

        //retrives data in an efficient way from the sorted file
        int GetNext (Record &fetchme, CNF &cnf, Record &literal);
        
       
};
#endif
