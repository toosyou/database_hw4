#ifndef DB_H
#define DB_H

#include <string>
#include <dirent.h>
#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include <algorithm>
#include <unistd.h>
#include <sys/stat.h>
#include <map>

#define ADDRESS_DB "airline.db"
#define SIZE_ARRDELAY 4
#define SIZE_ORIGIN 3
#define SIZE_DEST 3
#define SIZE_RECORD 10

using namespace std;

bool is_number(const string& input);

struct record{
    int ArrDelay;
    char Origin[4];
    char Dest[4];

    int parse_from_buffer(string &input){
        size_t find_pos = -1;
        for(int i=0;i<14;++i)
            find_pos = input.find(',', find_pos+1);

        //15 : ArrDelay
        string str_arrdelay = input.substr(find_pos+1, input.find(',', find_pos+1)-find_pos-1);
        if(is_number(str_arrdelay) == false)//NA, null
            return -1;
        this->ArrDelay = atoi(str_arrdelay.c_str());

        //17 : Origin
        find_pos = input.find(',', find_pos+1);
        find_pos = input.find(',', find_pos+1);
        string str_origin = input.substr(find_pos+1, input.find(',', find_pos+1)-find_pos-1);
        strcpy( this->Origin, str_origin.c_str());

        //18 : Dest
        find_pos = input.find(',', find_pos+1);
        string str_dest = input.substr(find_pos+1, input.find(',', find_pos+1)-find_pos-1);
        strcpy( this->Dest, str_dest.c_str() );

        return 0;
    }
    void encode_to_db_app(fstream &out_db){
        //encode to file

        out_db.write( (char*)&(this->ArrDelay), SIZE_ARRDELAY );
        out_db.write( this->Origin, SIZE_ORIGIN );
        out_db.write( this->Dest, SIZE_DEST );

        return;
    }
    void decode_from_db(char *input){
        this->ArrDelay = *((int*)input);
        memcpy(this->Origin, input+SIZE_ARRDELAY, SIZE_ORIGIN);
        this->Origin[4] = '\0';
        memcpy(this->Dest, input+SIZE_ARRDELAY+SIZE_ORIGIN, SIZE_DEST);
        this->Dest[4] = '\0';
    }
    void decode_from_db_only_arrdelay(char *input){
        this->ArrDelay = *((int*)input);
    }
};

bool cmp_record(const record &a,const record &b);
bool cmp_charstring(const char *a, const char *b);

class db{

    //vector<record> records_;
    string address_tmp_dir_;
    string address_db_;
    bool indexed_;
    map<string, vector<int> > index_; //origin_dest to position

public:
    void init();                                     //Do your db initialization.
    void setTempFileDir(string dir);                //All the files that created by your program should be located under this directory.
    void import(string address_csv);                        //Inport csv files under this directory.
    void createIndex();                              //Create index on one or two columns.
    double query(string origin, string dest);          //Do the query and return the average ArrDelay of flights from origin to dest.
    void cleanup();                                  //Release memory, close files and anything you should do to clean up your db class.
};

#endif
