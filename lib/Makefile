KYOTOCABINETLIB=

DBLink.so:DBLink.cpp
	g++ -fPIC DBLink.cpp -shared -Wl,-Bstatic $(KYOTOCABINETLIB) -lkyotocabinet -Wl,-Bdynamic -o DBLink.so
