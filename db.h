#pragma GCC push_options
#pragma GCC optimize (2)
#define NDEBUG

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
#include <sys/mman.h>
#include <fcntl.h>
#include <map>
#include <cstdio>

#define ADDRESS_DB "airline.db"
#define SIZE_ARRDELAY 4
#define SIZE_ORIGIN 3
#define SIZE_DEST 3
#define SIZE_RECORD 10

using namespace std;

bool is_place(const char* input);
bool is_number(const char* input);
bool ODcmp(const char* origin_a, const char *origin_b, const char *dest_a, const char *dest_b);

struct map_index{
    char origin_dest[7];
    void decode_from_db(char *input){
        memcpy(this->origin_dest, input+SIZE_ARRDELAY, SIZE_ORIGIN+SIZE_DEST);
        this->origin_dest[6] = '\0';
        return;
    }
};

struct block_position{
    int offset;
    int size;
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
        int index_column = 0;
        int length_column = 0;
        const int length_input = strlen(input);
        char column_buffer[200] = {0};

        for(int i=0;i<length_input;++i){
            if(input[i] == ','){
                column_buffer[length_column++]='\0';

                if(index_column == 14){
                    if( is_number(column_buffer) == false)
                        return -1;
                    this->ArrDelay = atoi(column_buffer);
                }
                else if(index_column == 16){
                    if( is_place(column_buffer) == false)
                        return -1;
                    strcpy(this->Origin, column_buffer);
                }
                else if(index_column == 17){
                    if( is_place(column_buffer) == false)
                        return -1;
                    strcpy(this->Dest, column_buffer);
                }
                else if(index_column > 17)
                    break;

                length_column = 0;
                index_column++;
                continue;
            }
            column_buffer[length_column++] = input[i];
        }

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
    void decode_from_db(char *input){
        this->ArrDelay = *((int*)input);
        memcpy(this->Origin, input+SIZE_ARRDELAY, SIZE_ORIGIN);
        memcpy(this->Dest, input+SIZE_ARRDELAY+SIZE_ORIGIN, SIZE_DEST);
        this->Origin[3] = '\0';
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

class db{

    string address_tmp_dir_;
    string address_db_;
    bool indexed_;
    map<map_index, vector<int>, cmp_mapindex> pre_index_; //origin_dest to position
    map<map_index, block_position, cmp_mapindex> index_;

public:
    void init();
    void setTempFileDir(string dir);
    void import(string address_csv);
    void createIndex();
    double query(string &origin, string &dest);
    double query(const char* origin, const char* dest);
    void cleanup();

    void all();
};

#endif
