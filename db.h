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
#include <sys/types.h>
#include <map>

#define ADDRESS_DB "airline.db"
#define SIZE_ARRDELAY 4
#define SIZE_ORIGIN 3
#define SIZE_DEST 3
#define SIZE_RECORD 10

using namespace std;

bool is_number(const string& input);
bool is_number(const char* input);

struct map_index{
    char origin_dest[7];
};

struct cmp_mapindex{
    bool operator()(const map_index &a, const map_index &b) const{
        return strcmp(a.origin_dest, b.origin_dest) < 0;
    }
};

struct record{
    int ArrDelay;
    char Origin[4];
    char Dest[4];

    int parse_from_buffer(char* input){
        char* tok = strtok(input, ",");
        for(int i=0;i<14;++i){
            tok = strtok(NULL, ",");
        }
        //15 : ArrDelay
        if( is_number(tok) == false )
            return -1;
        this->ArrDelay = atoi(tok);

        //17 : Origin
        tok = strtok(NULL, ",");
        tok = strtok(NULL, ",");
        strcpy( this->Origin, tok);

        //18 : Dest
        tok = strtok(NULL, ",");
        strcpy( this->Dest, tok);

        return 0;
    }
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
    void encode_to_db_app(FILE *file_db){
        //encode to file
        size_t size_char = sizeof(char);
        fwrite((char*)&this->ArrDelay, size_char, SIZE_ARRDELAY, file_db);
        fwrite( this->Origin, size_char, SIZE_ORIGIN, file_db);
        fwrite( this->Dest, size_char, SIZE_DEST, file_db);
        return;
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
        this->Origin[3] = '\0';
        memcpy(this->Dest, input+SIZE_ARRDELAY+SIZE_ORIGIN, SIZE_DEST);
        this->Dest[3] = '\0';
    }
    void decode_from_db_origin_dest(char *input){
        memcpy(this->Origin, input+SIZE_ARRDELAY, SIZE_ORIGIN);
        this->Origin[3] = '\0';
        memcpy(this->Dest, input+SIZE_ARRDELAY+SIZE_ORIGIN, SIZE_DEST);
        this->Dest[3] = '\0';
    }
    void decode_from_db_only_arrdelay(char *input){
        this->ArrDelay = *((int*)input);
    }
};

bool cmp_record(const record &a,const record &b);
bool cmp_charstring(const char *a, const char *b);

class db{

    string address_tmp_dir_;
    string address_db_;
    bool indexed_;
    map<map_index, vector<int>, cmp_mapindex> index_; //origin_dest to position

public:
    void init();
    void setTempFileDir(string dir);
    void import(string address_csv);
    void createIndex();
    double query(string &origin, string &dest);
    double query(const char* origin, const char* dest);
    void cleanup();
};

#endif
