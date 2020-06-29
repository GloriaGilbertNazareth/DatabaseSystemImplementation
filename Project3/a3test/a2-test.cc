
#include "test.h"

void initializeDBFiles(relation* rel){
	DBFile dbfile;
	cout<<"Creating dbFile:"<<endl;
	dbfile.Create (rel->path(), heap, NULL);
	char tbl_path[200]; // construct path of the tpch flat text file //changed tp 200 as file path too long
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name()); 
	cout<<"tblpath "<<tbl_path <<endl;
	dbfile.Load (*(rel->schema ()), tbl_path);
	dbfile.Close ();

}

int main (int argc, char *argv[]) {

	setup ();
	relation *rel_ptr[] = {n, r, c, p, ps, s, o, li};
	for(int i=0;i<8;i++){
		initializeDBFiles(rel_ptr[i]);
	}
}
