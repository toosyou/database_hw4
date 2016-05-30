#include "db.h"

void db::init(){
	//Do your db initialization.
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
                table tmp;

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

                this->tables_.push_back(tmp);
            }
            in_csv.close();
        }
    }
    closedir(dir);

    return;
}

void db::createIndex(){
	//Create index.
}

double db::query(string origin, string dest){
	//Do the query and return the average ArrDelay of flights from origin to dest.
	//This method will be called multiple times.
	return 0; //Remember to return your result.
}

void db::cleanup(){
	//Release memory, close files and anything you should do to clean up your db class.
}
