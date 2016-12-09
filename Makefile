all: hello-cpp-world hello-c-world WordCount.cpp

%: %.cc
	g++ -std=c++11 $< -o $@

%: %.c
	gcc $< -o $@

