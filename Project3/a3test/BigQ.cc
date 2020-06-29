#include "BigQ.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen): inputPipe(in),outputPipe(out), order(sortorder),runLength(runlen)  {
	temp_file=randString(10)+".bin";
	file.Open(0,const_cast<char *>(temp_file.c_str()));
	pthread_create(&worker, NULL, sortRecords, (void *)this);
	
}

void * BigQ::sortRecords(void *Big){
	BigQ *bigQ= (BigQ *)Big;
	OrderMaker order1 = bigQ->order;
	OrderMaker order2 = bigQ->order;
	auto comparisionEngine = [&](Record *record1,Record *record2){
		ComparisonEngine comp;
		return comp.Compare(record1,record2,&order1)<0?true:false;
	};

	auto comparisionEngine1 = [&](pair<int,Record *>record1,pair<int,Record *>record2){
		ComparisonEngine comp;
		return comp.Compare(record1.second,record2.second,&order2)<0?false:true;
	};
	Record record;
	Page *writePage = new Page();
	size_t currentLength = 0;
	int pageCount = 0;
	vector<Record *> sortedVector; //this will be used to sort the values internally within reach run;
	vector<int> pageVector;
	int count = 0;
	while((bigQ->inputPipe).Remove(&record)){
		char * totalBits = record.GetBits();
		if(currentLength+((int *)totalBits)[0] < (PAGE_SIZE)*(bigQ->runLength)){
			Record *rec = new Record();
			rec->Consume(&record);
			sortedVector.push_back(rec);
			currentLength += ((int *)totalBits)[0];
		}
		else{
			std::sort(sortedVector.begin(),sortedVector.end(),comparisionEngine);
			pageVector.push_back(pageCount);
			for(auto i:sortedVector)
			{	
				if(!writePage->Append(i))
    			{
        			int index = bigQ->file.GetLength();
					index=(index==0?0:(index-1)); 
        			bigQ->file.AddPage(writePage,index);
        			writePage->EmptyItOut();
        			writePage->Append(i);
					pageCount++;
    			}
			}
			if(!writePage->empty()){
				int value = bigQ->file.GetLength();
				value=(value==0?0:(value-1)); 
				bigQ->file.AddPage(writePage,value);
				writePage->EmptyItOut();
				pageCount++;
			}
			for(auto i:sortedVector)
				delete i;
			sortedVector.clear();
			Record *rec = new Record();
			rec->Consume(&record);
			sortedVector.push_back(rec);
			currentLength = ((int *)totalBits)[0];
			//this is done to read one of the record from the file
		}
	}
	if(!sortedVector.empty()){
		//this is in case last record is still not empty we will write these records on file
		std::sort(sortedVector.begin(),sortedVector.end(),comparisionEngine);
		pageVector.push_back(pageCount);
		for(auto i:sortedVector){
			if(!writePage->Append(i))
			{
				int index = bigQ->file.GetLength();
				index=(index==0?0:(index-1)); 
				bigQ->file.AddPage(writePage,index);
				writePage->EmptyItOut();
				writePage->Append(i);
				pageCount++;
			}
		}
		if(!writePage->empty()){
			int index = bigQ->file.GetLength();
			index=(index==0?0:(index-1)); 
			bigQ->file.AddPage(writePage,index);
			writePage->EmptyItOut();
			pageCount++;
		}
		for(auto i:sortedVector)
			delete i;
		sortedVector.clear();
	}
	pageVector.push_back(pageCount);
	vector<Page *> pageKeeper;
	priority_queue<pair<int, Record*>, vector<pair<int, Record*>>,decltype( comparisionEngine1 ) > priorityQ(comparisionEngine1);
	for(int i=0;i<pageVector.size()-1;i++ ){
		Page *tempPage = new Page();
		bigQ->file.GetPage(tempPage,pageVector[i]);
		Record *record = new Record();
 		tempPage->GetFirst(record);
		priorityQ.push(make_pair(i,record));
		pageKeeper.push_back(tempPage);
	}
	vector<int> pageChecker(pageVector);
	while(!priorityQ.empty()){
		auto top = priorityQ.top();
		bigQ->outputPipe.Insert(top.second);
		priorityQ.pop();
		Record *tempRecord=new Record();
		if(!pageKeeper[top.first]->GetFirst(tempRecord)){
			if(++pageChecker[top.first]<pageVector[top.first+1]){
				pageKeeper[top.first]->EmptyItOut();
 				bigQ->file.GetPage(pageKeeper[top.first], pageChecker[top.first]);
 				pageKeeper[top.first]->GetFirst(tempRecord);
				priorityQ.push(make_pair(top.first,tempRecord));
 			}
		}
		else{
			priorityQ.push(make_pair(top.first,tempRecord));
		}
	}
	for(auto i:pageKeeper)
		delete i;
	bigQ->outputPipe.ShutDown ();
	bigQ->file.Close();
	remove((bigQ->temp_file).c_str());
	remove((bigQ->temp_file+".meta").c_str());
}

BigQ::~BigQ () {
}