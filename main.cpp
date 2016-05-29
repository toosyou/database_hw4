#include <iostream>
#include <time.h>
#include "db.h"

using namespace std;

int main(int argc, char* argv[]){
	//declear db object
	db mydb;

	//init db
	mydb.init();

	//set temp directory
	mydb.setTempFileDir("temp");

	//Import data
	mydb.import("data");

	//Create index on one or two columns.
	clock_t tIndex = clock();
	mydb.createIndex();

	//Do queries
	//These queries are required in your report.
	//We will do different queries in the contest.
	//Start timing
	clock_t tQuery = clock();
	double result1 = mydb.query("IAH", "JFK");
	double result2 = mydb.query("IAH", "LAX");
	double result3 = mydb.query("JFK", "LAX");
	double result4 = mydb.query("JFK", "IAH");
	double result5 = mydb.query("LAX", "IAH");

	//End timing
	clock_t tEnd = clock();
	printf("Time taken for creating index: %.2fs\n", (double)(tEnd - tIndex) / CLOCKS_PER_SEC);
	printf("Time taken for making queries: %.2fs\n", (double)(tEnd - tQuery) / CLOCKS_PER_SEC);

	//Cleanup db object
	mydb.cleanup();
}