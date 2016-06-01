#include "db.h"

bool is_number(const string& input){
    if(input.size() == 0)
        return false;

    for(int i=0;i<input.size();++i){
        if( isdigit(input[i]) == false )
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
    char original_directory[100] = {0};
    getcwd(original_directory, 100);
    chdir(dir.c_str());

    remove(address_db_.c_str());
    
    chdir(original_directory);
    return;
}

void db::import(string address_csv){

    cerr << "importing " << address_csv << " ..." ;
    cerr.flush();

    fstream in_csv(address_csv.c_str());
    fstream out_db( this->address_db_.c_str(),
                        fstream::out | fstream::binary | fstream::app);

    string buffer;
    //first line
    getline(in_csv, buffer);
    //real data
    while( getline(in_csv, buffer) ){
        record tmp;

        //read from buffer
        if( tmp.parse_from_buffer(buffer) == -1)
            continue;
        //to db
        tmp.encode_to_db_app( out_db );
    }
    out_db.close();
    in_csv.close();

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
            total_arrdelay += tmp.ArrDelay;
        }
        number_record += position.size();

    }else{
        while( in_db.read(buffer, SIZE_RECORD) ){
            tmp.decode_from_db(buffer);
            if(strcmp(origin.c_str(), tmp.Origin) == 0 &&
                strcmp(dest.c_str(), tmp.Dest) == 0){
                total_arrdelay += tmp.ArrDelay;
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
