#include <gtest/gtest.h>
#include<gtest/gtest-death-test.h>
#include <iostream>
#include "DBFile.h"
#include "SortedFile.h"
#include"File.h"
#include "test.h"
#include"BigQ.h"
#include<map>

#include<cmath>
#include <queue> 

 OrderMaker sortorder;

TEST(createFile,test1) 
{ 
    DBFile dbfile;
    struct {OrderMaker *o; int l;} startup = {&sortorder, 1};
    ASSERT_EQ(1, dbfile.Create (r->path(), sorted, &startup));
    dbfile.Close();
}

TEST(createFileFailure,test2) 
{ 
    DBFile dbfile;
    relation* rel = new relation (nation, new Schema (catalog_path, nation), dbfile_dir);
    ASSERT_EQ(0,dbfile.Create (rel->path(), txt , NULL));
}

TEST(openFile, test3) 
{ 
	DBFile dbfile;
	relation* rel = new relation (region, new Schema (catalog_path, region), dbfile_dir);
    ASSERT_EQ(1, dbfile.Open (rel->path()));
    dbfile.Close();
    delete rel;
}

TEST(CloseFile, test4) 
{ 
	DBFile dbfile;
    struct {OrderMaker *o; int l;} startup = {&sortorder, 1};

    dbfile.Create (r->path(), sorted, &startup);
    char tbl_path[100]; // construct path of the tpch flat text file
    sprintf (tbl_path, "%s%s.tbl", tpch_dir, r->name()); 
    dbfile.Load (*(r->schema ()), tbl_path);    
    ASSERT_EQ(1, dbfile.Close());
}

int main(int argc, char **argv) 
{   
    setup();   
    r->get_sort_order (sortorder);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}