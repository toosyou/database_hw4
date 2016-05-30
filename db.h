#ifndef DB_H
#define DB_H

#include <string>
#include <dirent.h>
#include <iostream>
#include <fstream>
//#include <cstring>
#include <vector>
#include <algorithm>

using namespace std;

struct record{
    int ArrDelay;
    string Origin;
    string Dest;
};

bool cmp_record(record a, record b);

class db{

    vector<record> records_;
    string address_tmp_dir_;

public:
    void init();                                     //Do your db initialization.
    void setTempFileDir(string dir);                //All the files that created by your program should be located under this directory.
    void import(string csvDir);                        //Inport csv files under this directory.
    void createIndex();                              //Create index on one or two columns.
    double query(string origin, string dest);          //Do the query and return the average ArrDelay of flights from origin to dest.
    void cleanup();                                  //Release memory, close files and anything you should do to clean up your db class.
};

#endif
