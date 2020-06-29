#include "RelOp.h"
#include "BigQ.h"
#include <sstream>


void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	this->inFile = &inFile;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;

	pthread_create(&threadSelectFile, NULL, select, (void*) this);
}

void* SelectFile::select(void* arg){
	((SelectFile *)arg)->performOperation();
}

//reads the records from the file and compares it to the literal based on the CNF. 
//If it matches it adds it to the output pipe. 
//Incase the CNF is empty, all records are added to the output file
void* SelectFile::performOperation(){
    int count =0;
	Record rec;
    inFile->MoveFirst();
    ComparisonEngine comparisonEngine;
        
    while(inFile->GetNext(rec))
    {
        if(selOp!=NULL) //Checking if CNF is not empty
        {
        if(comparisonEngine.Compare(&rec,literal,selOp))
            {
            count++;
            outPipe->Insert(&rec);            
            }
        }
        else //CNF is empty
        {
               count++;
               outPipe->Insert(&rec);                 
        }
    }
    cout<<"number of records inserted::"<<count<<endl;
    outPipe->ShutDown();

}	
//waiting till the threadSelectFile has completed
void SelectFile::WaitUntilDone () {
	pthread_join (threadSelectFile, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
	return;
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;

	pthread_create(&threadSelectPipe, NULL, select, (void*) this);
}

void* SelectPipe::select(void* arg){
	((SelectPipe *)arg)->performOperation();
}
//reads the records from the inPipe and compares it to the literal based on the CNF. 
//If it matches it adds it to the output pipe. 
//Incase the CNF is empty, all records are added to the output file
void* SelectPipe::performOperation(){
	Record rec;
    ComparisonEngine comparisonEngine;
    int count =0;
    while(inPipe->Remove(&rec))
    {
        if(selOp!=NULL)
        {
        if(comparisonEngine.Compare(&rec,literal,selOp))
            {
            count++;
            outPipe->Insert(&rec);            
            }
        }
        else
        {
            count++;
               outPipe->Insert(&rec);                 
        }
    }    
        cout<<"number of records inserted::"<<count<<endl;

    outPipe->ShutDown();

}	

//waits till threadSelectPipe has completed
void SelectPipe::WaitUntilDone () {
	pthread_join (threadSelectPipe, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {
	return;
}


void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    this->inPipe = &inPipe;
    this->outPipe = &outPipe;
    this->keepMe = keepMe;
    this->numAttsInput = numAttsInput;
    this->numAttsOutput=numAttsOutput;

    pthread_create(&threadProject, NULL, project, (void*) this);
}

void* Project::project(void* arg){
    ((Project *)arg)->performOperation();
}

//this function selects only certain columns present in keepMe array, makes a new record and adds it to the output Pipe
void* Project::performOperation(){
    Record rec;
    ComparisonEngine comparisonEngine;
        
    while(inPipe->Remove(&rec))
    {
        rec.Project(keepMe, numAttsOutput, numAttsInput);
        outPipe->Insert(&rec);                 
        
    }    
    outPipe->ShutDown();

}   

//waiting for the threadProject to complete
void Project::WaitUntilDone () {
    pthread_join (threadProject, NULL);
}

void Project::Use_n_Pages (int runlen) {
    return;
}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    runLength = DEFAULT_RUNLEN;
    this->inPipe = &inPipe;
    this->outPipe = &outPipe;
    this->mySchema = &mySchema;

    pthread_create(&threadDuplicateRemoval, NULL, removeDuplicate, (void*) this);
}

void* DuplicateRemoval::removeDuplicate(void* arg){
    ((DuplicateRemoval *)arg)->performOperation();
}

//Removes duplicate elements by first sorting the records using BigQ
//Comparing CNF with the current and previous records, if they are not same we add to outpipe else we don't 

void* DuplicateRemoval::performOperation(){
    Pipe *sortedPipe = new Pipe(100);
    OrderMaker sortOrder(mySchema);
    ComparisonEngine comparisonEngine;
    BigQ sorting(*inPipe, *sortedPipe, sortOrder,runLength);
    Record previous;
    Record current;
    sortedPipe->Remove(&previous);
    while(sortedPipe->Remove(&current))
    {
        if(comparisonEngine.Compare(&previous, &current,&sortOrder)!=0){
            outPipe->Insert(&previous);
            previous.Consume(&current);
        }               
        
    }        
    outPipe->Insert(&previous);
    outPipe->ShutDown();

}   

void DuplicateRemoval::WaitUntilDone () {
    pthread_join (threadDuplicateRemoval, NULL);
}

void DuplicateRemoval::Use_n_Pages (int runlen) {
    runLength=runlen;
    return;
}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
    this->inPipe = &inPipe;
    this->outPipe = &outPipe;
    this->computeMe = &computeMe;

    pthread_create(&threadSum, NULL, calculateSum, (void*) this);
}

void* Sum::calculateSum(void* arg){
    ((Sum *)arg)->performOperation();
}

//Calculates the sum of all the records.
//checks if the column whose sum is to be calculated is int or double
//based on it adds to the respective value,
//creates a new record and adds it to the output pipe
void* Sum::performOperation(){
    int intSum =0;
    double doubleSum =0;
    int intCurrValue;
    double doubleCurrValue;
    Record current;
    Type type;
    while(inPipe->Remove(&current))
    {
        type=computeMe->Apply(current, intCurrValue, doubleCurrValue);
        if(type==Int){
            intSum = intSum + intCurrValue;
        }
        else if(type==Double){
            doubleSum = doubleSum + doubleCurrValue;
        }        
    }

    Record *record = new Record();
    Attribute attribute = {(char*)"sum", type};
    Schema sum_schema((char*)"sumschema",1,&attribute);
    char sumString[30];
    if(type==Int)
        sprintf(sumString, "%d|",intSum);
    else if(type==Double)
        sprintf(sumString, "%f|",doubleSum);

    record->ComposeRecord(&sum_schema,sumString);
    outPipe->Insert(record);
    outPipe->ShutDown();

}   

void Sum::WaitUntilDone () {
    pthread_join (threadSum, NULL);
}

void Sum::Use_n_Pages (int runlen) {
    return;
}

/*Steps:
1. Sort the records based on grouping attribute
2. Make groups using comparision engine and calculate sum of groups
3. Make sum record and group record, merge and insert them in pipe when current record not in previous records group
*/
void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
    this->inPipe = &inPipe;
    this->outPipe = &outPipe;
    this->groupAtts = &groupAtts;
    this->computeMe = &computeMe;
    this->runLength = DEFAULT_RUNLEN;
    pthread_create(&threadGroupBy, NULL, groupRecords, (void*) this);
}

void* GroupBy::groupRecords(void* arg){
    ((GroupBy *)arg)->performOperation();
}

void* GroupBy::performOperation(){
    Pipe sortedPipe(100);
    BigQ *sortedQ = new BigQ(*(this->inPipe),sortedPipe, *(this->groupAtts),runLength);
   
    int intCurrValue;
    double doubleCurrValue;
    Type type;
    

    ComparisonEngine comparisonEngine;
    Record *record =new Record();

    if(sortedPipe.Remove(record)){
        bool moreRecordsLeft = true;
        while(moreRecordsLeft){
            moreRecordsLeft = false;
            type  = computeMe->Apply(*record, intCurrValue, doubleCurrValue);
            double sum =0;
            sum = sum+ (intCurrValue+doubleCurrValue);
            Record *current = new Record();
            Record *previous = new Record;
            previous->Copy(record);
            while(sortedPipe.Remove(current)){
                if(!comparisonEngine.Compare(previous,current,groupAtts)){
                    type = computeMe->Apply(*current, intCurrValue,doubleCurrValue);
                    sum=sum+intCurrValue+doubleCurrValue;
                }
                else{
                    record->Copy(current);
                    moreRecordsLeft = true;
                    break;
                }
            }
            Record *resultRecord = new Record();

            Attribute att = {(char*)"sum", type};
            Schema sum_schema((char*)"sumschema",1,&att);
            char sumstring[30];
            if(type==Int)
                sprintf(sumstring, "%d|", sum);
            else if(type==Double)
                sprintf(sumstring, "%f|", sum);

            resultRecord->ComposeRecord(&sum_schema,sumstring);
        Record resultRec;
        int numsumAtts = groupAtts->numAtts+1;
        int sumAtts[numsumAtts];
        sumAtts[0]=0;
        for(int i=1;i<numsumAtts;i++)
        {
            sumAtts[i]=groupAtts->whichAtts[i-1];
        }
        resultRec.MergeRecords(resultRecord,record,1,numsumAtts-1,sumAtts,numsumAtts,1);

            outPipe->Insert(&resultRec);
        }

    }

    outPipe->ShutDown();

}   

//waits for the threadGroupBy to complete
void GroupBy::WaitUntilDone () {
    pthread_join (threadGroupBy, NULL);
}

void GroupBy::Use_n_Pages (int runlen) {
    this->runLength = runlen;
    return;
}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    this->inPipeL = &inPipeL;
    this->inPipeR = &inPipeR;
    this->outPipe = &outPipe;
    this->selOp = &selOp;
    this->literal = &literal;
    this->runLength = DEFAULT_RUNLEN;
    pthread_create(&threadJoin, NULL, join, (void*) this);
}

void* Join::join(void* arg){
    ((Join *)arg)->performOperation();
}

void* Join::performOperation(){
    OrderMaker leftOrder;
    OrderMaker rightOrder;
    selOp->GetSortOrders(leftOrder,rightOrder);
    //cout<<"Number atts in left"<<leftOrder.numAtts<<" right"<<rightOrder.numAtts<<" run"<<runLength<< endl;
    //equi join check
    if(leftOrder.numAtts && rightOrder.numAtts && leftOrder.numAtts == rightOrder.numAtts){
        Pipe *sortedLeftPipe = new Pipe(100);
        Pipe *sortedRightPipe = new Pipe(100);
        //sort all records in left table
        BigQ *sortedLeftQ = new BigQ(*(this->inPipeL), *sortedLeftPipe, leftOrder,runLength);
        //sort all records in right table
        BigQ *sortedRightQ = new BigQ(*(this->inPipeR), *sortedRightPipe, rightOrder,runLength);
        vector<Record *> leftRecordsVector;
        Record *leftRecord = new Record();
        vector<Record *> rightRecordsVector;
        Record *rightRecord = new Record();
        ComparisonEngine comparisonEngine;
        if(sortedLeftPipe->Remove(leftRecord) && sortedRightPipe->Remove(rightRecord)){
            int numberLeftAttributes = ((int *) leftRecord->bits)[1]/sizeof(int) -1;
            int numberRightAttributes = ((int *) rightRecord->bits)[1]/sizeof(int) -1;
            int totalAttributes = numberLeftAttributes+numberRightAttributes;
            //cout<<"total attributes created" <<numberRightAttributes <<" "<<numberLeftAttributes<<" "<<totalAttributes<<endl;

            int attributes[totalAttributes];
            for(int i=0;i<numberLeftAttributes;i++)
                attributes[i] = i;
            for(int i=0;i<numberRightAttributes;i++)
                attributes[numberLeftAttributes+i] = i;
            int count=0; //counts the records joined
            bool leftRecordValid = true; 
            bool rightRecordValid = true;

            while(leftRecordValid && rightRecordValid){
                leftRecordValid = false;
                rightRecordValid = false;
                int value = comparisonEngine.Compare(leftRecord, &leftOrder, rightRecord, &rightOrder);
                if(value ==0){ //if they are equal create new record for all equal and insert in putpipe
                    Record *tempLeftRecord = new Record();
                    tempLeftRecord->Consume(leftRecord);
                    Record *tempRightRecord = new Record();
                    tempRightRecord->Consume(rightRecord);
                    leftRecordsVector.push_back(tempLeftRecord);
                    rightRecordsVector.push_back(tempRightRecord);
                    //check for equal consecutive left records
                    while(sortedLeftPipe->Remove(leftRecord)){
                        if(comparisonEngine.Compare(leftRecord, tempLeftRecord , &leftOrder) ==0){
                            Record *currentLeftRecord = new Record();
                            currentLeftRecord->Consume(leftRecord);
                            leftRecordsVector.push_back(currentLeftRecord);
                        }
                        else{
                            //consecutive not matching hence need to check with next right
                            leftRecordValid = true;
                            break;
                        }
                    }
                    //check for equal consecutive right records
                    while(sortedRightPipe->Remove(rightRecord)){
                        if(comparisonEngine.Compare(rightRecord, tempRightRecord, &rightOrder) ==0){
                            Record *currentRightRecord = new Record();
                            currentRightRecord->Consume(rightRecord);
                            rightRecordsVector.push_back(currentRightRecord);
                        }
                        else{
                            //consecutive not matching hence need to check with next left
                            rightRecordValid = true;
                            break;
                        }
                    }

                    //merging records from both vectors
                    Record *left = new Record();
                    Record *right = new Record();
                    Record *joined = new Record();
                    for(auto recL:leftRecordsVector){
                        left->Consume(recL);
                        for(auto recR: rightRecordsVector){
                            if(comparisonEngine.Compare(left, recR, literal, selOp)){
                                count++;
                                right->Copy(recR);
                                joined->MergeRecords(left, right, numberLeftAttributes,numberRightAttributes, attributes, numberLeftAttributes+numberRightAttributes, numberLeftAttributes);
                                outPipe->Insert(joined);
                            }
                        }
                    }
                    for(auto recL:leftRecordsVector){
                        if(!recL){
                            delete recL;
                        }
                    }
                    leftRecordsVector.clear();
                    for(auto recR:rightRecordsVector){
                        if(!recR){
                            delete recR;
                        }
                    }
                    rightRecordsVector.clear();

                }
                else if(value == 1){
                    leftRecordValid = true;
                    if(sortedRightPipe->Remove(rightRecord)){
                        rightRecordValid = true;
                    }

                }
                else{
                    rightRecordValid = true;
                    if(sortedLeftPipe->Remove(leftRecord)){
                        leftRecordValid = true;
                    }                
                }
            }

        }
    }
    else{ //cross product of all records
        int pageCount = 10;
        Record *leftRecord = new Record;
        Record *rightRecord = new Record;
        Page rightPage;
        DBFile leftDBFile;
        leftDBFile.Create((char*)"tmpL", heap, NULL);
        leftDBFile.MoveFirst();

        int numberLeftAttribute, numberRightAttributes, totalAttributes, *attributes;

        if(inPipeL->Remove(leftRecord) && inPipeR->Remove(rightRecord)) {
            numberLeftAttribute = ((int *) leftRecord->bits)[1] / sizeof(int) -1;
            numberRightAttributes = ((int *) rightRecord->bits)[1] / sizeof(int) -1;
            totalAttributes = numberLeftAttribute + numberRightAttributes;
            attributes = new int[totalAttributes];
            for(int i = 0; i< numberLeftAttribute; i++)
                attributes[i] = i;
            for(int i = 0; i< numberRightAttributes; i++)
                attributes[i+numberLeftAttribute] = i;
            do {
                leftDBFile.Add(*leftRecord);
            }while(inPipeL->Remove(leftRecord));
            vector<Record *> rightRecordsVector;
            ComparisonEngine comparisonEngine;

            bool moreRecordsLeft = true;
            int count =0;
            while(moreRecordsLeft) {
                Record *record = new Record();
                record->Copy(rightRecord);
                rightPage.Append(rightRecord);
                rightRecordsVector.push_back(record);
                int rightPages = 0;
                moreRecordsLeft = false;
                while(inPipeR->Remove(rightRecord)) {
                    Record *temp = new Record();
                    temp->Copy(rightRecord);
                    if(!rightPage.Append(rightRecord)) {
                        rightPages += 1;
                        if(rightPages >= pageCount -1) {
                            moreRecordsLeft = true;
                            break;
                        }
                        else {
                            rightPage.EmptyItOut();
                            rightPage.Append(rightRecord);
                            rightRecordsVector.push_back(temp);
                        }
                    } else {
                        rightRecordsVector.push_back(temp);
                    }
                }
                leftDBFile.MoveFirst();
                while(leftDBFile.GetNext(*leftRecord)) {
                    for(auto it:rightRecordsVector) {
                        if(comparisonEngine.Compare(leftRecord, it, literal, selOp)) {
                            count++;
                            Record *joinRec = new Record();
                            Record *rightRecord = new Record();
                            rightRecord->Copy(it);
                            joinRec->MergeRecords(leftRecord, rightRecord, numberLeftAttribute, numberRightAttributes, attributes, numberLeftAttribute+numberRightAttributes, numberLeftAttribute);
                            outPipe->Insert(joinRec);
                        }
                    }
                }
                for(auto it : rightRecordsVector)
                    if(!it)
                        delete it;
                rightRecordsVector.clear();
            }
            leftDBFile.Close();
        }

    }
    //cout<<"join complwtwd"<<endl;
    outPipe->ShutDown();
}   

//watitng for threadWriteOut to complete
void Join::WaitUntilDone () {
    pthread_join (threadJoin, NULL);
}

void Join::Use_n_Pages (int runlen) {
    runLength = runlen;
    return;
}


/*Steps:
1. Removing records from the input pipe
2. Reading its bits, and finding the type and writing the datatype and value to file in the tbl data format*/
void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    this->inPipe = &inPipe;
    this->outFile = outFile;
    this->mySchema = &mySchema;

    pthread_create(&threadWriteOut, NULL, writeToFile, (void*) this);
}

void* WriteOut::writeToFile(void* arg){
    ((WriteOut *)arg)->performOperation();
}

void* WriteOut::performOperation(){
    Record rec;
    long count =0; //count for the number of records written
    int n = mySchema->GetNumAtts();
    Attribute *attributes = mySchema->GetAtts();
    while(inPipe->Remove(&rec)){
        count++;
        for(int i=0;i<n;i++){
            fprintf(outFile, "%s\n", attributes[i].name);
            int pointer = ((int *) rec.bits)[i + 1];
            fprintf(outFile, "%c\n", '[');
            if (attributes[i].myType == Int) {
                int *myInt = (int *) &(rec.bits[pointer]);
                fprintf(outFile,"%d",*myInt);
            } 
            else if (attributes[i].myType == Double) {
                double *myDouble = (double *) &(rec.bits[pointer]);
                fprintf(outFile,"%f",*myDouble);
            } 
            else if (attributes[i].myType == String) {
                char *myString = (char *) &(rec.bits[pointer]);
                fprintf(outFile,"%s",myString);
            }
            fprintf(outFile,"%c",']');
            if (i != n - 1) {
                fprintf(outFile,"%c",',');
            }

        }
        fprintf(outFile,"%c",'\n');
    }
    fclose(outFile);
    cout<<"\nRecords written: "<<count<<"\n";
}   

//watitng for threadWriteOut to complete
void WriteOut::WaitUntilDone () {
    pthread_join (threadWriteOut, NULL);
}

void WriteOut::Use_n_Pages (int runlen) {
    return;
}
