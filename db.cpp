#include <string>
#include "db.h"
using namespace std;

void db::init(){
	//Do your db initialization.
}

void db::setTempFileDir(string dir){
	//All the files that created by your program should be located under this directory.
}

void db::import(string csvDir){
	//Inport csv files under this directory.
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