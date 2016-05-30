#include "db.h"

bool cmp_record(record a, record b){ // a <= b
    int first_compare = a.Origin.compare(b.Origin);
    if( first_compare == 0){ // equal
        return a.Dest.compare(b.Dest) < 0 ? true : false;
    }else{
        return first_compare < 0 ? true : false;
    }
}

/*bool operator<(const record &a, const record & b){
    int first_compare = a.Origin.compare(b.Origin);
    if( first_compare == 0){ // equal
        return a.Dest.compare(b.Dest) < 0 ? true : false;
    }else{
        return first_compare < 0 ? true : false;
    }
}*/

void db::init(){
	//Do your db initialization.
    this->indexed = false;
}

void db::setTempFileDir(string dir){
	//All the files that created by your program should be located under this directory.
    this->address_tmp_dir_ = dir;
}

void db::import(string csvDir){
	//Import csv files under this directory.
    DIR *dir = opendir(csvDir.c_str());
    dirent *ent;
    while( (ent = readdir(dir)) != NULL ){
        //cout << ent->d_name <<endl;
        if(ent->d_name[0] != '.' ){ // todo : check .csv
            fstream in_csv(csvDir + string("/") + string(ent->d_name), fstream::in);
            if(in_csv.is_open() == false)
                exit(-1);

            string buffer;
            //first line
            getline(in_csv, buffer);
            //load the data
            while( getline(in_csv, buffer) ){
                record tmp;

                size_t find_pos = -1;
                for(int i=0;i<14;++i)
                    find_pos = buffer.find(',', find_pos+1);

                //15 : ArrDelay
                string str_arrdelay = buffer.substr(find_pos+1, buffer.find(',', find_pos+1)-find_pos-1);
                tmp.ArrDelay = atoi(str_arrdelay.c_str());

                //17 : Origin
                find_pos = buffer.find(',', find_pos+1);
                find_pos = buffer.find(',', find_pos+1);
                tmp.Origin = buffer.substr(find_pos+1, buffer.find(',', find_pos+1)-find_pos-1);

                //18 : Dest
                find_pos = buffer.find(',', find_pos+1);
                tmp.Dest = buffer.substr(find_pos+1, buffer.find(',', find_pos+1)-find_pos-1);

                this->records_.push_back(tmp);
            }
            in_csv.close();
        }
    }
    closedir(dir);

    return;
}

void db::createIndex(){
	//Create index.

    //sort records_ with (Origin, Dest)
    sort(this->records_.begin(), this->records_.end(), cmp_record);
    this->indexed = true;

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

    //binary search using lower_bound for indexed data
    if(this->indexed){
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
        //linear search for unindexed data
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
