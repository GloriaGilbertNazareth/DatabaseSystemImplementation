#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include<vector>
#include<map>

using namespace std;

typedef struct {
	
}bigqUtil;

class Sort{

OrderMaker *sortOrder;
public:

	Sort(OrderMaker *orderMaker)
	{
		sortOrder = orderMaker;
	}

	bool operator()(Record* r1,Record* r2) {
		ComparisonEngine comp;
		if(comp.Compare(r1,r2,sortOrder)<0)
			return true;
		else return false;
	}
};
class Sort_Merge{

	OrderMaker *sort_order;
public:

	Sort_Merge(OrderMaker *abc)
	{
		sort_order = abc;
	}

bool operator()(Record* r1,Record* r2) {
	ComparisonEngine comp;
	if(comp.Compare(r1,r2,sort_order)<0) return false;
	else return true;
}
};
class BigQ {
	
public:
	Pipe *in;
	Pipe *out;
	OrderMaker *sortorder;
	int runlen;
	BigQ();
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	int  writeSortedRun(map<int,Page*>& overflow, File& f, vector<Record*> recVector, int runlen, int& pageCount); 
	int sortRunlens(vector<Record*>& recVector, OrderMaker* sortorder);
	void addRecords(vector<Record*>& recVector, vector<Page*>& pageVector);
	void divideRuns(Pipe *in, int runlen, map<int,Page*>& overflow, OrderMaker* sortorder, File& f, int& pageCount);
	void mergeRuns(int pageCount, Pipe* out, int runlen, File& f, OrderMaker* sortorder, map<int,Page*> overflow);
	
	~BigQ ();
};

#endif
