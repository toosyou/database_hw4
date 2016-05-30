all: test
test: main.cpp db.cpp db.h
	g++ -std=c++11 main.cpp db.cpp -o test

clean:
	rm test
