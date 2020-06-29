#define PAGE_SIZE 131072

#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "File.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "Defs.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

int main () {

	// try to parse the CNF
	/*cout << "Enter in your CNF: ";
  	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
		exit (1);
	}

	// suck up the schema from the file
	Schema lineitem ("catalog", "lineitem"); //object creation of type Schema, catalog -> fileName, lineitem -> relName ie. one of the tables in catalog

	// grow the CNF expression from the parse tree 
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree (final, &lineitem, literal);
	
	// print out the comparison to the screen
	myComparison.Print ();

	// now open up the text file and start procesing it
        FILE *tableFile = fopen ("/media/gloria/Work/UniversityOfFlorida/Study/SemII/DBI/Projects/Project1/git/tpch-dbgen/lineitem.tbl", "r");

        Record temp;
        Schema mySchema ("catalog", "lineitem");

	//char *bits = literal.GetBits ();
	//cout << " numbytes in rec " << ((int *) bits)[0] << endl;
	//literal.Print (&supplier);

        // read in all of the records from the text file and see if they match
	// the CNF expression that was typed in
	int counter = 0;
	ComparisonEngine comp;
        while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
		counter++;
		if (counter % 10000 == 0) {
			cerr << counter << "\n";
		}

		if (comp.Compare (&temp, &literal, &myComparison))
                	temp.Print (&mySchema);

        }
*/

	Schema lineitem ("catalog", "region");
	cout<<"Schema Created"<<"\n";

	Page* page = new Page();
	cout<<"Page created"<<"\n";
	FILE* fileName = fopen("/media/gloria/Work/UniversityOfFlorida/Study/SemII/DBI/Projects/Project1/git/tpch-dbgen/region.tbl","r");
	File* file = new File();
	char* fName = "/media/gloria/Work/UniversityOfFlorida/Study/SemII/DBI/Projects/Project1/git/tpch-dbgen/regionNew.txt";
	cout<<"Files created"<<"\n";
	file->Open(0,fName);
	cout<<"File Opened"<<"\n";

	Record rec;
	if(fName){

		while(rec.SuckNextRecord(&lineitem, fileName) ==1){
			//cout<<"rec:::::::::::::"<<rec<<"\n";
			cout<<"In"<<"\n";
			int val = page->Append(&rec);
							int currlen = file->GetLength();

							cout<<"currlen::::"<<currlen<<"\n";

			if(val ==0)
			{
				currlen = file->GetLength();
				cout<<"currlen::::"<<currlen<<"\n";
				int whichPage = currlen==0?0:currlen-1;
				file->AddPage(page, whichPage);
				page->EmptyItOut();
				page->Append(&rec);
			}
		}
	}
	int currlen = file->GetLength();

	int whichpage = currlen==0?0:currlen-1;
	file->AddPage(page, whichpage);

	Page* rpage = new Page();
	int currlen1 = file->GetLength();
	cout<<"currlen::::::::::::"<<currlen1<<"\n";

	file->GetPage(rpage, (off_t)0);


	                         

	//file.GetPage(page, (off_t)-1);
	//page->ToBinary(bits);
	//cout<<bits<<"\n";
	

}


void Add(Record* rec, Page* pg, File* file, int* pgIndex){
	 
}