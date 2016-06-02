#include "db.h"

bool is_number(const string& input){
    if(input.size() == 0)
        return false;

    for(int i=1;i<input.size();++i){
        if( isdigit(input[i]) == false )
            return false;
    }
    return true;
}

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

bool cmp_record(record a, record b){ // a <= b
    int first_compare = strcmp(a.Origin, b.Origin);
    if( first_compare == 0){ // equal
        return strcmp(a.Dest, b.Dest) < 0 ? true : false;
    }else{
        return first_compare < 0 ? true : false;
    }
}

bool cmp_charstring(const char *a, const char *b){
    return strcmp(a, b) < 0;
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
    mkdir(this->address_tmp_dir_.c_str(), 755);

    //remove old database
    remove(address_db_.c_str());

    return;
}

void db::import(string address_csv){

    cerr << "importing " << address_csv << " ..." ;
    cerr.flush();

    FILE *file_csv = fopen( address_csv.c_str(), "r" );
    FILE *file_db = fopen( this->address_db_.c_str(), "ab" );

    char buffer[1000] = {0};
    record tmp_record;

    //first line
    fgets(buffer, 1000, file_csv);
    //real data
    while( fgets(buffer, 1000, file_csv) != NULL ){
        tmp_record.parse_from_buffer(buffer);
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

    fstream in_db( this->address_db_.c_str(), fstream::in | fstream::binary);

    int position = 0;
    char buffer[SIZE_RECORD+1];
    while( in_db.read(buffer, SIZE_RECORD) ){
        record tmp;
        char origin_dest[SIZE_ORIGIN+SIZE_DEST+1];

        tmp.decode_from_db(buffer);
        memcpy(origin_dest, tmp.Origin, SIZE_ORIGIN);
        memcpy(origin_dest+SIZE_ORIGIN, tmp.Dest, SIZE_DEST);
        origin_dest[SIZE_ORIGIN+SIZE_DEST] = '\0';
        this->index_[string(origin_dest)].push_back(position);

        position += SIZE_RECORD;
    }

    in_db.close();
    this->indexed_ = true;

    return;
}

double db::query(string origin, string dest){
	//Do the query and return the average ArrDelay of flights from origin to dest.
	//This method will be called multiple times.
    double total_arrdelay = 0.0;
    int number_record = 0;
    char buffer[SIZE_RECORD+1];
    record tmp;

    fstream in_db(this->address_db_, fstream::in | fstream::binary);

    //binary search using lower_bound for indexed_ data
    if(this->indexed_){
        char target[SIZE_ORIGIN + SIZE_DEST + 1];

        memcpy(target, origin.c_str(), SIZE_ORIGIN);
        memcpy(target+SIZE_ORIGIN, dest.c_str(), SIZE_DEST);
        target[SIZE_ORIGIN + SIZE_DEST] = '\0';
        vector<int> &position = this->index_[string(target)];

        for(int i=0;i<position.size();++i){
            in_db.seekp(ios_base::beg + position[i]);
            in_db.read(buffer, SIZE_RECORD);
            tmp.decode_from_db_only_arrdelay(buffer);
            total_arrdelay += (double)tmp.ArrDelay;
        }
        number_record += position.size();

    }else{
        while( in_db.read(buffer, SIZE_RECORD) ){
            tmp.decode_from_db(buffer);
            if(strcmp(origin.c_str(), tmp.Origin) == 0 &&
                strcmp(dest.c_str(), tmp.Dest) == 0){
                total_arrdelay += (double)tmp.ArrDelay;
                number_record++;
            }
        }
    }

    in_db.close();
	return total_arrdelay / (double)number_record; //Remember to return your result.
}

void db::cleanup(){
	//Release memory, close files and anything you should do to clean up your db class.
    this->index_.clear();

    return;
}
