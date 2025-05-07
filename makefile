CC = gcc
CFLAGS = -Wall -Wextra -lpthread -lrt

all: mcached

mcached: mcached.c uthash.h mcached.h
	$(CC) $(CFLAGS) -o mcached mcached.c

clean:
	rm -f mcached
