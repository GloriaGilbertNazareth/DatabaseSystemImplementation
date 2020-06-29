#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"
#include "Defs.h"
using namespace std;

/*
This file is the path way to heap and other file systems to be implemented
*/
GenericDBFile::~GenericDBFile(){
}
GenericDBFile::GenericDBFile () {
}

int GenericDBFile::Create (char *f_path, fType f_type, void *startup) {
    return 1;
}
int GenericDBFile::Open (char *f_path) {
    return 1;
}
int GenericDBFile::Close () {
	return 1;
}
void GenericDBFile::Load (Schema &f_schema, char *loadpath) {
}
void GenericDBFile::Add (Record &record) {
}
void GenericDBFile::MoveFirst () {
}
int GenericDBFile::GetNext (Record &fetchme) {
	return 0;
}
int GenericDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	return 0;
}