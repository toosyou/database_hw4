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

    //query without indexing
    clock_t tQueryWithoutIndex = clock();
    double result1_noindex = mydb.query("IAH", "JFK");
    double result2_noindex = mydb.query("IAH", "LAX");
    double result3_noindex = mydb.query("JFK", "LAX");
    double result4_noindex = mydb.query("JFK", "IAH");
    double result5_noindex = mydb.query("LAX", "IAH");

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
    printf("Time taken for queries without indexing: %.2fs\n", (double)(tIndex - tQueryWithoutIndex) / CLOCKS_PER_SEC);
	printf("Time taken for creating index: %.2fs\n", (double)(tQuery - tIndex) / CLOCKS_PER_SEC);
	printf("Time taken for making queries: %.2fs\n", (double)(tEnd - tQuery) / CLOCKS_PER_SEC);

	//Cleanup db object
	mydb.cleanup();

    //output result
    cout << "IAH\tJFK" <<endl;
    cout << "with no index: \t" << result1_noindex <<endl;
    cout << "with index: \t" << result1 <<endl;

    cout << "IAH\tLAX" <<endl;
    cout << "with no index: \t" << result2_noindex <<endl;
    cout << "with index: \t" << result2 <<endl;

    cout << "JFK\tLAX" <<endl;
    cout << "with no index: \t" << result3_noindex <<endl;
    cout << "with index: \t" << result3 <<endl;

    cout << "JFK\tIAH" <<endl;
    cout << "with no index: \t" << result4_noindex <<endl;
    cout << "with index: \t" << result4 <<endl;

    cout << "LAX\tIAH" <<endl;
    cout << "with no index: \t" << result5_noindex <<endl;
    cout << "with index: \t" << result5 <<endl;

    return 0;
}
