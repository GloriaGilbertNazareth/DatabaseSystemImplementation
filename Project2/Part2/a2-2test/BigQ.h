#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <vector>
#include <string>
#include <algorithm>
using namespace std;

class Records {
public:

    Record record;
    int runIndex;
    OrderMaker *orderMaker;

};

class CompareRecords {
public:
    static int compareRecords(const void *rc1, const void *rc2);
    Record record;
    OrderMaker *orderMaker;
};

class RunMetaData {
public:
    int startPage, endPage;
};

class BigQ {

    Pipe *inputPipe, *outputPipe;
    OrderMaker *sortingOrder;
    int runLength;

public:
    vector<pair<int,int> > pageBegin;
    vector<RunMetaData*> runmetaDataVec;
    File sortedFile;
    char *fileName;
    int currentPageNum, numberRuns;
    BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
    ~BigQ ();
    //worker thread to perform TPMMS
    static void* Worker(void* threadid);

    //divides the data into runs and sorts the records using inbuilt sorting
    //it then adds sorted runs to file amd calls merge 
    void sortRecords();

    //writing the sorted runs into pages in a temp file and 
    //keeping record of start and end page of each run
    void writeInFile(vector<CompareRecords*> rcVector);

    //merges the records in runs using priority queue
    void mergeRecords();
    
};

class PageWrap {
public:
    Page page; 
    int currentPage; 
};
#endif