all: test
test: main.cpp db.o db.h
	g++ -std=c++11 main.cpp db.o -o test

db.o: db.cpp db.h
	g++ -std=c++11 -c db.cpp -o db.o

clean:
	rm -f test
	rm -f db.o
