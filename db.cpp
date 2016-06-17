#pragma GCC push_options
#pragma GCC optimize (2)
#define NDEBUG

#include "db.h"

bool is_place(const char* input){
    int length_input = strlen(input);
    if(length_input == 0)
        return false;
    for(int i=0;i<length_input;++i){
        if(input[i]<'A' || input[i]>'Z')
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

void db::all(){
    map<map_index, block_position, cmp_mapindex>::iterator it;
    for(it = this->index_.begin(); it != this->index_.end(); ++it){
        cout << this->query( it->first.origin_dest, it->first.origin_dest+3 ) <<endl;
    }

    return;
}

void db::createIndex(){
	//Create index.

    if(this->indexed_ == true)
        return;
    this->pre_index_.clear();
    this->index_.clear();

    map_index tmp_index;

    char *db_mmap ;
    int fd_db = open(this->address_db_.c_str(), O_RDONLY);
    struct stat sb_db;
    fstat(fd_db, &sb_db);
    db_mmap = (char*)mmap(NULL, sb_db.st_size, PROT_READ, MAP_SHARED, fd_db, 0);

    for(int i=0, position=0;i<sb_db.st_size;i+=SIZE_RECORD){
        tmp_index.decode_from_db(db_mmap+i);
        this->pre_index_[tmp_index].push_back(position);
        position += SIZE_RECORD;
    }

    //make it continuous
    FILE *file_new_db = fopen( (this->address_db_+string("_new") ).c_str(), "wb");

    int offset = 0;
    map<map_index, vector<int>, cmp_mapindex>::iterator it_index;
    for(it_index = this->pre_index_.begin(); it_index != this->pre_index_.end(); ++it_index){
        vector<int> &positions = it_index->second;

        //make the real index_
        block_position tmp;
        tmp.offset = offset;
        tmp.size = positions.size();
        this->index_[it_index->first] = tmp;

        //write continuously in the new file
        for(int i=0;i<positions.size();++i){
            fwrite( db_mmap+positions[i], SIZE_RECORD, 1, file_new_db );
            offset+=SIZE_RECORD;
        }
    }

    fclose(file_new_db);

    //close mmap
    munmap(db_mmap, sb_db.st_size);
    close(fd_db);

    //file renaming
    remove(this->address_db_.c_str());
    rename( (this->address_db_+string("_new")).c_str(), this->address_db_.c_str() );

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

    char *db_mmap ;
    int fd_db = open(this->address_db_.c_str(), O_RDONLY);
    struct stat sb_db;
    fstat(fd_db, &sb_db);
    db_mmap = (char*)mmap(NULL, sb_db.st_size, PROT_READ, MAP_SHARED, fd_db, 0);

    if(this->indexed_){
        map_index target;
        target.origin_dest[0] = origin[0];
        target.origin_dest[1] = origin[1];
        target.origin_dest[2] = origin[2];
        target.origin_dest[3] = dest[0];
        target.origin_dest[4] = dest[1];
        target.origin_dest[5] = dest[2];
        target.origin_dest[6] = '\0';
        block_position &position = this->index_[target];

        int offset = position.offset;
        number_record = position.size;
        for(int i=0;i<number_record*SIZE_RECORD;i+=SIZE_RECORD){
            total_arrdelay += *(int*)(db_mmap+offset+i);
        }

    }else{

        for(int i=0;i<sb_db.st_size;i+=SIZE_RECORD){
            if( memcmp(origin, db_mmap+i+SIZE_ARRDELAY, SIZE_ORIGIN ) == 0 &&
                memcmp(dest, db_mmap+i+SIZE_ARRDELAY+SIZE_ORIGIN, SIZE_DEST) == 0){
                total_arrdelay += *(int*)(db_mmap+i);
                number_record++;
            }
        }

    }

    munmap(db_mmap, sb_db.st_size);
    close(fd_db);

	return (double)total_arrdelay / (double)number_record; //Remember to return your result.
}

void db::cleanup(){
	//Release memory, close files and anything you should do to clean up your db class.
    this->index_.clear();
    this->pre_index_.clear();

    return;
}
