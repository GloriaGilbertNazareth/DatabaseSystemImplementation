#include <gtest/gtest.h>
#include<gtest/gtest-death-test.h>
#include <iostream>
#include "test.h"
#include "Record.h"
#include "RelOp.h"
int pipesz =100;
Attribute IA = {"int", Int};
SelectFile SF_ps, SF_p, SF_s, SF_o, SF_li, SF_c, SF_n;
DBFile dbf_ps, dbf_p, dbf_s, dbf_o, dbf_li, dbf_c, dbf_n;
Pipe _ps (pipesz), _p (pipesz), _s (pipesz), _o (pipesz), _li (pipesz), _c (pipesz), _in(pipesz);
CNF cnf_ps, cnf_p, cnf_s, cnf_o, cnf_li, cnf_c, cnf_n;
Record lit_ps, lit_p, lit_s, lit_o, lit_li, lit_c, lit_n;
Function func_ps, func_p, func_s, func_o, func_li, func_c;
SelectPipe SP_n;
//this test selects data from the nation bin file and filters for records whose region key is <3 and returns output  
//the count of records in the output is matched in the assert. 
TEST(selectFile,test1) 
{ 
    char *pred_nation = "(n_regionkey < 3)";
    dbf_n.Open (n->path());
    get_cnf (pred_nation, n->schema (), cnf_n, lit_n);
    SF_n.Use_n_Pages (100);
    SF_n.Run (dbf_n, _o, cnf_n, lit_n);
    SF_n.WaitUntilDone ();
    int count =15; 
    int recordedcnt = 0;
    Record rec;
    while (_o.Remove (&rec)) {
       
        recordedcnt++;
    }
    ASSERT_EQ(count, recordedcnt);
}

//this test selects data from the nation bin file put it in pipe then filters for records whose region key is >3 and returns output  
//the count of records in the output is matched in the assert.  
TEST(selectPipe,test2) 
{ 
    char *pred_nation = "(n_regionkey > 3)";
    dbf_n.Open (n->path());
    dbf_n.MoveFirst();
    Record rec;

    while(dbf_n.GetNext(rec)){
        _in.Insert(&rec);
    }
    _in.ShutDown();
    get_cnf (pred_nation, n->schema (), cnf_n, lit_n);
    SP_n.Use_n_Pages (100);
    SP_n.Run (_in, _o, cnf_n, lit_n);
    SP_n.WaitUntilDone ();
    int count =5; 
    int recordedcnt = 0;
    while (_o.Remove(&rec)) {
        recordedcnt++;
    }
    ASSERT_EQ(count, recordedcnt);
}

//this test selects data from the nation bin file then filters for records whose region key is >3, calculates the sum and returns output   
TEST(sum,test3) 
{ 
    char *pred_nation = "(n_regionkey > 3)";
    dbf_n.Open (n->path());
    
    get_cnf (pred_nation, n->schema (), cnf_n, lit_n);
    SF_n.Use_n_Pages (100);
    SF_n.Run (dbf_n, _o, cnf_n, lit_n);
    SF_n.WaitUntilDone ();

    Sum T;
        // _s (input pipe)
        Pipe _out (1);
        Function func;
        char *str_sum = "(n_regionkey)";
        get_cnf (str_sum, n->schema (), func);
        func.Print ();
    T.Use_n_Pages (1);
    T.Run (_o, _out, func);
    T.WaitUntilDone ();

    Schema out_sch ("out_sch", 1, &IA);
    Record rec;
    int count =1; 
    int recordedcnt = 0;
   while (_out.Remove (&rec)) {
        rec.Print (&out_sch);
        recordedcnt++;
    }
    ASSERT_EQ(count, recordedcnt);
}

//checks for unique values of n_regionkey present in the table and adds it in the __ps pipe.
//The count of the records is matched in the assert function
TEST(duplicateRemoval,test4) 
{ 
    dbf_n.Open (n->path());
    SF_n.Use_n_Pages (100);
    SF_n.Run (dbf_n, _o, cnf_n, lit_n);
    SF_n.WaitUntilDone ();
    DuplicateRemoval D;
        // inpipe = __ps
        Pipe ___ps (pipesz);
        Schema __ps_sch ("_o", 2, &IA);
       D.Run (_o, ___ps,__ps_sch);

    int count =5;  
    int recordedcnt = 0;
    Record rec;
    while (___ps.Remove (&rec)) {
       
        recordedcnt++;
    }
    ASSERT_EQ(count, recordedcnt);
}

int main(int argc, char **argv) 
{   
    setup();   
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}