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
    int first_compare = a.Origin.compare(b.Origin);
    if( first_compare == 0){ // equal
        return a.Dest.compare(b.Dest) < 0 ? true : false;
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
    mkdir(this->address_tmp_dir_.c_str(), 755);

    return;
}

void db::import(string address_csv){

    cerr << "importing " << address_csv << " ..." ;
    cerr.flush();

    fstream in_csv(address_csv.c_str());
    fstream out_db( (this->address_tmp_dir_ + string("/") + this->address_db_).c_str(),
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

    cerr << "\tdone!" <<endl;
    return;
}

void db::createIndex(){
	//Create index.

    //sort records_ with (Origin, Dest)
    sort(this->records_.begin(), this->records_.end(), cmp_record);
    this->indexed_ = true;

    return;
}

double db::query(string origin, string dest){
	//Do the query and return the average ArrDelay of flights from origin to dest.
	//This method will be called multiple times.

    double total_arrdelay = 0.0;
    int number_record = 0;

    record target;
    target.Origin = origin;
    target.Dest = dest;

    //binary search using lower_bound for indexed_ data
    if(this->indexed_){
        vector<record>::iterator it_first = lower_bound(this->records_.begin(), this->records_.end(), target, cmp_record);
        for(vector<record>::iterator it = it_first; it!=this->records_.end() ; ++it){
            record &this_record = (*it);
            if( this_record.Origin != origin || this_record.Dest != dest )
                break;
            total_arrdelay += (double)this_record.ArrDelay;
            number_record++;
            //cout << this_record.Origin << "\t" << this_record.Dest << "\t" << this_record.ArrDelay <<endl;
        }
    }else{
        //linear search for unindexed_ data
        for(int i=0;i<this->records_.size();++i){
            if( this->records_[i].Origin == target.Origin &&
                this->records_[i].Dest == target.Dest){
                total_arrdelay += (double)this->records_[i].ArrDelay;
                number_record++;
            }
        }
    }

    //cout << total_arrdelay << "\t" << number_record << "\t" << total_arrdelay / (double)number_record <<endl;

	return total_arrdelay / (double)number_record; //Remember to return your result.
}

void db::cleanup(){
	//Release memory, close files and anything you should do to clean up your db class.
    this->records_.clear();

    return;
}
