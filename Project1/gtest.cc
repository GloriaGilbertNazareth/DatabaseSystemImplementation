#include <gtest/gtest.h>
#include<gtest/gtest-death-test.h>
#include <iostream>
#include "DBFile.h"
#include "test.h"


// make sure that the file path/dir information below is correct

TEST(createFile,test1) 
{ 
    DBFile dbfile;
	relation* rel = new relation (lineitem, new Schema (catalog_path, lineitem), dbfile_dir);
    ASSERT_EQ(1, dbfile.Create (rel->path(), heap, NULL));
    delete rel;
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
}
int main(int argc, char **argv) 
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}