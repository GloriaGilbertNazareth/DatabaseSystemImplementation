#include <gtest/gtest.h>
#include<gtest/gtest-death-test.h>
#include <iostream>
#include "DBFile.h"
#include"File.h"
#include "test.h"
#include"BigQ.h"
#include<map>

#include<cmath>
#include <queue> 

 OrderMaker sortorder;
 //this function will read records from a file, write it to a record vector.
// It tests the writeSortedRun function. Here we check it for a file with records less than a single page
TEST(writeSortedRuns,test1)  
{ 
    DBFile dbfile;

    char sortedFile[100];
    sprintf (sortedFile, "%s.bin", "gtestFiles/sortedFile");
    int a = dbfile.Open (sortedFile);
    dbfile.MoveFirst ();
    vector<Record*> sortedRecords;
    Record* temp = new Record();
    while(dbfile.GetNext(*temp)==1){
        Record* rec = new Record();
        rec->Copy(temp);
        sortedRecords.push_back(rec);
    }
    dbfile.Close();
        int runlen =16;
    BigQ* bq = new BigQ();      
    File f;
    char tempFile[200];
    sprintf(tempFile,"test%d.bin",rand());
    f.Open(0,tempFile);
    int pageCount =0;
    map<int, Page*> overflow;
    int result = bq->writeSortedRun(overflow, f, sortedRecords, runlen ,pageCount);
    ASSERT_EQ(1, result);
}
TEST(writeSortedRunsMultiple,test2)  
{ 
    DBFile dbfile;

    char sortedFile[100];
    sprintf (sortedFile, "%s.bin", "gtestFiles/lineitem");
    int a = dbfile.Open (sortedFile);
    dbfile.MoveFirst ();
    vector<Record*> sortedRecords;
    Record* temp = new Record();
    while(dbfile.GetNext(*temp)==1){
        Record* rec = new Record();
        rec->Copy(temp);
        sortedRecords.push_back(rec);
    }
    dbfile.Close();
        int runlen =16;
    BigQ* bq = new BigQ();      
    File f;
    char tempFile[200];
    sprintf(tempFile,"test%d.bin",rand());
    f.Open(0,tempFile);
    int pageCount =0;
    map<int, Page*> overflow;
    int result = bq->writeSortedRun(overflow, f, sortedRecords, runlen ,pageCount);
    ASSERT_EQ(98, result);
}

TEST(sortRun, test3){
    DBFile dbfile;
    char unsortedFile[100];
    sprintf (unsortedFile, "%s.bin", "gtestFiles/unsortedFile");
    int a = dbfile.Open (unsortedFile);
    dbfile.MoveFirst ();
    vector<Record*> unsortedRecords;
    Record* temp = new Record();
    while(dbfile.GetNext(*temp)==1){
        Record* rec = new Record();
        rec->Copy(temp);
        unsortedRecords.push_back(rec);
    }
    dbfile.Close();

    char sortedFile[100];
    sprintf (sortedFile, "%s.bin", "gtestFiles/sortedFile");
    a = dbfile.Open (sortedFile);
    dbfile.MoveFirst ();
    vector<Record*> sortedRecords;
    temp = new Record();
    while(dbfile.GetNext(*temp)==1){
        Record* rec = new Record();
        rec->Copy(temp);
        sortedRecords.push_back(rec);
    }
    dbfile.Close();

    OrderMaker sortorder;
    BigQ* bq = new BigQ();
    bq->sortRunlens(unsortedRecords,&sortorder);

    ComparisonEngine comp;
    int result =0;
    for(int i=0;i<unsortedRecords.size();i++){
         if(comp.Compare(sortedRecords[i],unsortedRecords[i],&sortorder)!=0){
            result=1;
            break;
         }
    }

    ASSERT_EQ(0, result);
}
TEST(checkPriorityQueue, test4){
    DBFile dbfile;
    char unsortedFile[100];
    sprintf (unsortedFile, "%s.bin", "gtestFiles/unsortedFile");
    int a = dbfile.Open (unsortedFile);
    dbfile.MoveFirst ();
    vector<Record*> unsortedRecords;
    Record* temp = new Record();
    while(dbfile.GetNext(*temp)==1){
        Record* rec = new Record();
        rec->Copy(temp);
        unsortedRecords.push_back(rec);
    }
    dbfile.Close();

    char sortedFile[100];
    sprintf (sortedFile, "%s.bin", "gtestFiles/sortedFile");
    a = dbfile.Open (sortedFile);
    dbfile.MoveFirst ();
    vector<Record*> sortedRecords;
    temp = new Record();
    while(dbfile.GetNext(*temp)==1){
        Record* rec = new Record();
        rec->Copy(temp);
        sortedRecords.push_back(rec);
    }
    dbfile.Close();

   
    
    

    priority_queue <Record*, vector<Record*>, Sort_Merge> pqueue(&sortorder);  //used for kway merging
    for(int i=0;i<unsortedRecords.size();i++){
        pqueue.push(unsortedRecords[i]);
    }

    ComparisonEngine comp;
    int result =0;
    for(int i=0;i<unsortedRecords.size();i++){
        Record* record = pqueue.top();
        pqueue.pop(); 
         if(comp.Compare(sortedRecords[i],record,&sortorder)!=0){
            result=1;
            break;
         }
    }

    ASSERT_EQ(0, result);
}
/*TEST(createFileFailure,test2) 
{ 
    DBFile dbfile;
    relation* rel = new relation (nation, new Schema (catalog_path, nation), dbfile_dir);
    ASSERT_EQ(0,dbfile.Create (rel->path(), txt , NULL));
}
TEST(openFile, test3) 
{ 
	DBFile dbfile;
	relation* rel = new relation (lineitem, new Schema (catalog_path, lineitem), dbfile_dir);
    ASSERT_EQ(1, dbfile.Open (rel->path()));
    dbfile.Close();
    delete rel;
}
TEST(CloseFile, test4) 
{ 
	DBFile dbfile;
    relation* rel = new relation (nation, new Schema (catalog_path, nation), dbfile_dir);
    dbfile.Create (rel->path(), heap, NULL);
    char tbl_path[100]; // construct path of the tpch flat text file
    sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name()); 
    dbfile.Load (*(rel->schema ()), tbl_path);    
    ASSERT_EQ(2, dbfile.Close());
}
TEST(openFileWithoutCreate, test5) 
{ 
    DBFile dbfile;
    relation* rel = new relation (orders, new Schema (catalog_path, orders), dbfile_dir);
    ASSERT_EQ(0,dbfile.Open (rel->path()));
}
TEST(closeFileFailure, test6){
    DBFile dbfile;
    relation* rel = new relation (nation, new Schema (catalog_path, nation), dbfile_dir);
    dbfile.Create (rel->path(), heap, NULL);
    ASSERT_EQ(0, dbfile.Close());
}*/
int main(int argc, char **argv) 
{
    setup ();
    r->get_sort_order (sortorder);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}