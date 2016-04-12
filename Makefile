PROG=riemann-mysql
SRCS=log.c riemann-mysql.c
OBJS=$(SRCS:.c=.o)
CC=cc
RM=rm -f
LDADD=-lmysqlclient -lriemann-client
CFLAGS=-Wall -Werror
INSTALL=install

all: $(PROG)

install: all
	$(INSTALL) -D riemann-mysql $(DESTDIR)/usr/bin/riemann-mysql
	$(INSTALL) -D riemann-mysql.conf $(DESTDIR)/etc/riemann-mysql.conf

$(PROG): $(OBJS)
	$(CC) $(CFLAGS) -o $(PROG) $(OBJS) $(LDADD)

clean:
	$(RM) $(PROG) *.deb *.o *core *~
