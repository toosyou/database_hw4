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
        vector<int> &position = this->index_[target];

        number_record = position.size();
        for(int i=0;i<number_record;++i){
            total_arrdelay += *(int*)(db_mmap+position[i]);
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

    return;
}
