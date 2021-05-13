all: parser

parser: 
	g++ -o parser LogParser.cpp --std=c++14 -lcurl -lsqlite3

clear:
	rm parser
