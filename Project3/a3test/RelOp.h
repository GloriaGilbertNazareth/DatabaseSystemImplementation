#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#define DEFAULT_RUNLEN 10
class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

	private:
	pthread_t threadSelectFile;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	DBFile *inFile;
	// Record *buffer;

	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	static void* select(void*);
	void* performOperation();
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {
	private:
	pthread_t threadSelectPipe;
	Pipe *inPipe;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	// Record *buffer;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	static void* select(void*);
	void* performOperation();
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class Project : public RelationalOp { 
	private:
	pthread_t threadProject;
	Pipe *inPipe;
	Pipe *outPipe;
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	static void* project(void*);
	void* performOperation();
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Join : public RelationalOp { 
	private:
	pthread_t threadJoin;
	Pipe *inPipeL;
	Pipe *inPipeR;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	int runLength;
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	static void* join(void*);
	void* performOperation();
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};
class DuplicateRemoval : public RelationalOp {
	private:
	pthread_t threadDuplicateRemoval;
	Pipe *inPipe;
	Pipe *outPipe;
	Schema *mySchema;
	int runLength;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	static void* removeDuplicate(void*);
	void* performOperation();
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Sum : public RelationalOp {	
	private:
	pthread_t threadSum;
	Pipe *inPipe;
	Pipe *outPipe;
	Function *computeMe;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	static void* calculateSum(void*);
	void* performOperation();
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class GroupBy : public RelationalOp {
	private: 
	pthread_t threadGroupBy;
	Pipe *inPipe;
	Pipe *outPipe;
	OrderMaker *groupAtts;
	Function *computeMe;
	int runLength;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	static void* groupRecords(void*);
	void* performOperation();
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class WriteOut : public RelationalOp {
	private:
	pthread_t threadWriteOut;
	Pipe *inPipe;
    FILE *outFile;
	Schema *mySchema;
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	static void* writeToFile(void*);
	void* performOperation();
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
#endif
