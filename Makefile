CC ?= cc
CFLAGS ?= -std=c11 -Wall -Wextra -pedantic

.PHONY: all clean

all: webserver client

webserver: webserver.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -pthread $< $(LDFLAGS) $(LDLIBS) -o $@

client: client.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -pthread $< $(LDFLAGS) $(LDLIBS) -o $@

clean:
	$(RM) webserver client
