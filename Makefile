PROG=riemann-mysql
RM=rm -f
INSTALL=install

all: $(PROG)

install: all
	$(INSTALL) -D riemann-mysql $(DESTDIR)/usr/bin/riemann-mysql
	$(INSTALL) -D riemann-mysql.conf $(DESTDIR)/etc/riemann-mysql.conf

$(PROG):
	go build -mod vendor -o $(PROG)

clean:
	$(RM) $(PROG) *.deb
