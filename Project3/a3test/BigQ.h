#ifndef BIGQ_H
#define BIGQ_H
#pragma once
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <vector>
#include <stdio.h>
#include "ComparisonEngine.h"
#include <algorithm>
#include "DBFile.h"
#include <string>
#include <queue>

class DBFile; 

using namespace std;

class BigQ {
	
public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
	static void * sortRecords(void *bigQ);

private:
	Pipe &inputPipe, &outputPipe;
	pthread_t worker;
	DBFile * db;
	OrderMaker &order;
	int runLength;
	string temp_file;
	File file;
	std::string randString( size_t length )
	{
		auto random_char = []() -> char
		{
			const char character_Set[] =
			"0123456789"
			"qwertyuiop";
			const size_t index = (sizeof(character_Set) - 1);
			return character_Set[ rand() % index ];
		};
		std::string str(length,0);
		std::generate_n( str.begin(), length, random_char );
		return str;
	}
};




#endif
