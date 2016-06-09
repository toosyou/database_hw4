#pragma GCC push_options
#pragma GCC optimize (2)
#define NDEBUG

#include "db.h"

bool is_number(const char* input){
    int length_input = strlen(input);
    if( length_input == 0)
        return false;
    for(int i=1;i<length_input;++i){
        if( isdigit(input[i]) == false)
            return false;
    }
    return true;
}

bool ODcmp(const char* origin_a, const char *origin_b, const char *dest_a, const char *dest_b){
    if( origin_a[0] == origin_b[0] &&
        dest_a[0] == dest_b[0] &&
        origin_a[1] == origin_b[1] &&
        dest_a[1] == dest_b[1] &&
        origin_a[2] == origin_b[2] &&
        dest_a[2] == dest_b[2] )
        return true;
    return false;
}

bool cmp_record(record a, record b){ // a <= b
    int first_compare = strcmp(a.Origin, b.Origin);
    if( first_compare == 0){ // equal
        return strcmp(a.Dest, b.Dest) < 0 ? true : false;
    }else{
        return first_compare < 0 ? true : false;
    }
}


void db::init(){
	//Do your db initialization.
    this->indexed_ = false;
    this->address_db_ = string(ADDRESS_DB);

    return;
}

void db::setTempFileDir(string dir){
	//All the files that created by your program should be located under this directory.
    this->address_tmp_dir_ = dir;
    this->address_db_ = this->address_tmp_dir_ + string("/") + this->address_db_;
    mkdir(this->address_tmp_dir_.c_str(), 0755);

    //remove old database
    remove(address_db_.c_str());

    return;
}

void db::import(string address_csv){

    cerr << "importing " << address_csv << " ..." ;
    cerr.flush();

    FILE *file_csv = fopen( address_csv.c_str(), "r" );
    FILE *file_db = fopen( this->address_db_.c_str(), "ab" );

    char buffer[2000] = {0};
    record tmp_record;

    //first line
    fgets(buffer, 2000, file_csv);
    //real data
    while( fgets(buffer, 2000, file_csv) != NULL ){
        if(tmp_record.parse_from_buffer(buffer) == -1)
            continue;
        tmp_record.encode_to_db_app(file_db);
    }

    fclose(file_csv);
    fclose(file_db);

    this->index_.clear();
    this->indexed_ = false;

    cerr << "\tdone!" <<endl;
    return;
}

void db::createIndex(){
	//Create index.

    if(this->indexed_ == true)
        return;
    this->index_.clear();

    int position = 0;
    char buffer[SIZE_RECORD*1024+1];
    const size_t size_char = sizeof(char);
    map_index tmp_index;
    FILE *file_db = fopen( this->address_db_.c_str(), "rb");

    int number_record = 0;
    while( (number_record = fread(buffer, SIZE_RECORD, 1024, file_db)) != 0 ){
        for(int i=0;i<number_record;i++){
            tmp_index.decode_from_db(buffer+i*SIZE_RECORD);
            this->index_[tmp_index].push_back(position);
            position += SIZE_RECORD;
        }
    }

    //data rerange

    fclose(file_db);
    this->indexed_ = true;

    return;
}

double db::query(string &origin, string &dest){
    return this->query(origin.c_str(), dest.c_str());
}

double db::query(const char* origin, const char* dest){
	//Do the query and return the average ArrDelay of flights from origin to dest.
	//This method will be called multiple times.
    int total_arrdelay = 0.0;
    int number_record = 0;
    char buffer[SIZE_RECORD*1024+1];
    record tmp;
    const size_t size_char = sizeof(char);
    //fstream in_db(this->address_db_, fstream::in | fstream::binary);
    FILE *file_db = fopen(this->address_db_.c_str(), "rb");

    //binary search using lower_bound for indexed_ data
    if(this->indexed_){
        map_index target;
        target.origin_dest[0] = origin[0];
        target.origin_dest[1] = origin[1];
        target.origin_dest[2] = origin[2];
        target.origin_dest[3] = dest[0];
        target.origin_dest[4] = dest[1];
        target.origin_dest[5] = dest[2];
        vector<int> &position = this->index_[target];

        number_record = position.size();
        for(int i=0;i<number_record;++i){
            fseek(file_db, position[i], SEEK_SET);
            fread(buffer, SIZE_ARRDELAY, 1, file_db);
            total_arrdelay += *(int*)buffer;
        }

    }else{
        int number_record_read = 0;
        while( (number_record_read = fread(buffer, SIZE_RECORD, 1024, file_db)) != 0 ){
            for(int i=0;i<number_record_read;i++){
                char *position = buffer+i*SIZE_RECORD;

                if( memcmp(origin, position+SIZE_ARRDELAY, SIZE_ORIGIN) == 0 &&
                    memcmp(dest, position+SIZE_ARRDELAY+SIZE_ORIGIN, SIZE_DEST) == 0){
                    total_arrdelay += *(int*)position;
                    number_record++;
                }
            }
        }
    }

    //in_db.close();
    fclose(file_db);
	return (double)total_arrdelay / (double)number_record; //Remember to return your result.
}

void db::cleanup(){
	//Release memory, close files and anything you should do to clean up your db class.
    this->index_.clear();

    return;
}
