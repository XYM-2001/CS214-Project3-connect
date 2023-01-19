CC=gcc
CFLAGS=-Wall -Werror -fsanitize=address

all:
	$(CC) $(CFLAGS) connect.c -o connect -lpthread -lm

clean:
	rm -rf ww
