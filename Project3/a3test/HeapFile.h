#ifndef HEAPFILE_H
#define HEAPFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"


class HeapFile : virtual public GenericDBFile{

	private:
    File* heapFile;//file of type file general 
    Page* writePage;
    fType fileType;	 
    int currentPage;
    Page* readPage;
    public:
    	 //Constructor
	HeapFile();
    //Destructor
    ~HeapFile(); 

    /*
    This function will create the binary Heap file in the bin/DBFiles folder. 
    It will return 1 if it successfully does it
    else will return 0*/
    int Create (char *fpath, fType file_type, void *startup);
    /*
    This function will open the file object based on the data in the meta file.
    If its heap, it will open a heapFile object
    */
    int Open (char *f_path);
    /*
    This function will simply call the close file function of the File class and close
    the opened file*/
    int Close ();
    /*
    This function will add a record to the page. If the page is full, it will append that 
    page to the file, empty the existing page, and add the record to it*/
    void Add (Record &record);
    /*
    This function loads the data from the file path into the heap file based on the schema
    selected. It will read the records calling SuckNextRecord of the record class and add 
    records to the pages calling Add function declared above. It will at last add the last page
    (partially filled) to the file.*/
    void Load (Schema &f_schema, char *loadpath);
    /*This function will set the current page to 0 i.e the first page of the file*/
    void MoveFirst ();
    /**/
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);

    void AddCurrentPage ();

};
#endif
