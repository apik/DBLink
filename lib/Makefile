# set path to the dir with "kyotocabinet.pc" PKG_CONFIG_PATH=
KYOTOCABINET_CFLAGS=$(shell pkg-config --cflags kyotocabinet)
KYOTOCABINET_LIBS=$(shell pkg-config --libs kyotocabinet)

DBLink.so:DBLink.cpp
	g++ -fPIC $(KYOTOCABINET_CFLAGS) DBLink.cpp -shared -Wl,-Bstatic $(KYOTOCABINET_LIBS) -Wl,-Bdynamic -o DBLink.so
