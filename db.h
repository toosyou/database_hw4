#include <string>

class db{
public:
    void init();                                     //Do your db initialization.
    
    void setTempFileDir(std::string);                //All the files that created by your program should be located under this directory.

    void import(std::string);                        //Inport csv files under this directory.

    void createIndex();                              //Create index on one or two columns.

    double query(std::string, std::string);          //Do the query and return the average ArrDelay of flights from origin to dest.

    void cleanup();                                  //Release memory, close files and anything you should do to clean up your db class.
};
