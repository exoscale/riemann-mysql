PROG=riemann-mysql
SRCS=riemann.pb-c.c log.c riemann-mysql.c
OBJS=$(SRCS:.c=.o)
CC=cc
RM=rm -f
LDADD=-lprotobuf-c -lmysqlclient
CFLAGS=-Wall -Werror
PROTOC=protoc-c
FPMTYPE=deb
VERSION?=0.5.0
INSTALL=install

all: $(PROG)

version:
	@echo $(VERSION)

package: 
	mkdir -p /tmp/fpm-pkg/usr/bin
	mkdir -p /tmp/fpm-pkg/etc/init
	make install DESTDIR=/tmp/fpm-pkg
	fpm -s dir -t $(FPMTYPE) -C /tmp/fpm-pkg -n $(PROG) -v $(VERSION) .
	rm -r /tmp/fpm-pkg

install: all
	$(INSTALL) riemann-mysql $(DESTDIR)/usr/bin
	$(INSTALL) riemann-mysql.conf $(DESTDIR)/etc/riemann-mysql.conf.example
	$(INSTALL) riemann-mysql.upstart $(DESTDIR)/etc/init/riemann-mysql.conf

$(PROG): $(OBJS)
	$(CC) $(CFLAGS) -o $(PROG) $(OBJS) $(LDADD)

riemann.pb-c.c: riemann.proto
	$(PROTOC) --c_out=. $<

clean:
	$(RM) $(PROG) *.deb *.o *core riemann.pb-c.c riemann.pb-c.h *~
