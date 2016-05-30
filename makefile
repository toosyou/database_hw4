all: test
test: main.cpp db.o db.h
	g++-5 -std=c++11 main.cpp db.o -o test

db.o: db.cpp
	g++-5 -std=c++11 -c db.cpp -o db.o

clean:
	rm test
