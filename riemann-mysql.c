/*
 * Copyright (c) 2012 Exoscale
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF MIND, USE, DATA OR PROFITS, WHETHER
 * IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING
 * OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <sys/types.h>
#include <sys/socket.h>

#include <arpa/inet.h>
#include <err.h>
#include <getopt.h>
#include <mysql/mysql.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>

#include "riemann.pb-c.h"

#define PROGNAME		"riemann-mysql"
#define DEFAULT_CONFIG		"/etc/riemann-mysql.conf"
#define DEFAULT_INTERVAL        30
#define DEFAULT_DELAY		2.0
#define DEFAULT_HOSTNAME        "<unknown>"
#define DEFAULT_RIEMANN_HOST	"localhost"
#define DEFAULT_RIEMANN_PORT	5555
#define DEFAULT_MYSQL_HOST	"localhost"
#define DEFAULT_MYSQL_DB	""
#define DEFAULT_MYSQL_PORT	3306
#define MYSQL_QUERY		"show slave status"
#define MYSQL_SLAVE_IO		10
#define MYSQL_SLAVE_SQL		11
#define MYSQL_SECONDS_BEHIND	32
#define MAXLEN		        1024
#define MAXTAGS			32

/*
 * riemann-mysql is a simple mysql replication
 * check agent which submits values to a riemann
 * instance.
 *
 * Connections are persistent and retried at
 * each interval should they be lost.
 *
 * See https://github.com/exoscale/riemann-mysql
 */

enum status {
	S_DOWN = 0,
	S_UP
};

enum state {
	S_OK = 0,
	S_WARNING = 1,
	S_CRITICAL = 2,
	S_UNKNOWN = 3
};

struct mysql_handler {
	enum status	 status;
	MYSQL		*db;
	char		 host[MAXLEN];
	int		 port;
	char		 user[MAXLEN];
	char		 password[MAXLEN];
	char		 dbname[MAXLEN];
};

struct riemann_msg {
	u_char	*buf;
	int	 len;
};

struct riemann_host {
	enum status		status;
	char			host[MAXLEN];
	int			port;
	int			s;
	struct sockaddr_in	sin;
};

struct mysql_stats {
	enum state	 state;
	float		 ttl;
	int		 has_metric;
	double		 metric;
	char		*tags[MAXTAGS];
	int		 tagcount;
	char		*hostname;	
	char		 description[MAXLEN];
};

/* from log.c */
void		 log_init(const char *, int);
void		 log_warn(const char *, ...);
void		 log_warnx(const char *, ...);
void		 log_info(const char *, ...);
void		 log_debug(const char *, ...);
void		 fatal(const char *);
void		 fatalx(const char *);

void		 usage(const char *);

int		 mysql_get_handle(struct mysql_handler *);
MYSQL_RES	*mysql_run_query(struct mysql_handler *, const char *);
int		 mysql_gather(struct mysql_handler *, struct mysql_stats *);
int		 riemann_pack(struct riemann_msg *, struct mysql_stats *);
int		 riemann_send(struct riemann_host *, struct riemann_msg *);

/*
 * Attempt to get a working mysql connection, when
 * a previous succesful connection exists, ping it
 * to make sure it is still useable.
 */
int
mysql_get_handle(struct mysql_handler *hdl)
{
	int	res;

	if (hdl->status == S_UP) {
		if ((res = mysql_ping(hdl->db)) != 0) {
			hdl->status = S_DOWN;
			mysql_close(hdl->db);
			hdl->db = NULL;
			return mysql_get_handle(hdl);
		}
		return 0;
	}

	if ((hdl->db = mysql_init(NULL)) == NULL) {
		log_warnx("cannot allocate mysql connection");
		return -1;
	}
	
	if (mysql_real_connect(hdl->db, hdl->host, hdl->user,
			       hdl->password, 
			       (strlen(hdl->dbname) > 0 ? hdl->dbname : NULL)
			       , hdl->port, NULL, 0) == NULL) {
		log_warnx("cannot connect to mysql://%s@%s:%d/%s [%s]: %s",
			  hdl->user, hdl->host, hdl->port,
			  hdl->dbname, hdl->password, mysql_error(hdl->db));
		mysql_close(hdl->db);
		hdl->db = NULL;
		return -1;
	}

	hdl->status = S_UP;
	return 0;
}

/*
 * Execute "show slave status" and act on the resulting fields
 */
int
mysql_gather(struct mysql_handler *hdl, struct mysql_stats *stats)
{
	int		 desclen, io, sql;
	
	MYSQL_RES	*res;
	MYSQL_ROW	 row;

	bzero(stats->description, sizeof(stats->description));
	desclen = sizeof(stats->description) - 1;

	if (mysql_real_query(hdl->db, MYSQL_QUERY, strlen(MYSQL_QUERY))) {
		log_warn("could not execute query: %s", mysql_error(hdl->db));
		stats->state = S_UNKNOWN;
		strncpy(stats->description, mysql_error(hdl->db), desclen);
		return 0;
	}

	res = mysql_store_result(hdl->db);
	if (res == NULL) {
		log_warn("could store result: %s", mysql_error(hdl->db));
		stats->state = S_UNKNOWN;
		strncpy(stats->description, mysql_error(hdl->db), desclen);
		return 0;
	}

	row = mysql_fetch_row(res);
	if (row == NULL) {
		log_warn("could store result: %s", mysql_error(hdl->db));
		stats->state = S_UNKNOWN;
		strncpy(stats->description, mysql_error(hdl->db), desclen);
		mysql_free_result(res);
		return 0;
	}

	if (mysql_num_fields(res) < 33) {
		log_warnx("not enough fields given");
		stats->state = S_UNKNOWN;
		strncpy(stats->description, "fields missing", desclen);
		mysql_free_result(res);
	}

	io = (row[MYSQL_SLAVE_IO] != NULL &&
	      (strcasecmp(row[MYSQL_SLAVE_IO], "yes") == 0));
	sql = (row[MYSQL_SLAVE_SQL] != NULL &&
	       (strcasecmp(row[MYSQL_SLAVE_SQL], "yes") == 0));

	if (row[MYSQL_SECONDS_BEHIND] != NULL) {
		stats->has_metric = 1;
		stats->metric = atoll(row[MYSQL_SECONDS_BEHIND]);
	} else {
		stats->has_metric = 0;
	}
	
	snprintf(stats->description, desclen, "slave io: %s, slave sql: %s",
		 (io ? "running" : "stopped"), (sql ? "running" : "stopped"));
	if (io && sql) {
		stats->state = S_OK;
	} else {
		if (!sql)
			stats->state = S_WARNING;
		if (!io)
			stats->state = S_CRITICAL;
	}
	log_debug("gathered: state = %d, description = %s, metric = %f",
		  stats->state, stats->description, stats->metric);
	mysql_free_result(res);
	return 0;
}

/*
 * Prepare a serialized buffer from an Event structure
 */
int
riemann_pack(struct riemann_msg *m, struct mysql_stats *stats)
{
	Msg			 msg = MSG__INIT;
	Event			 ev = EVENT__INIT;
	Event			*evtab[1];
	char			*states[4] = { "ok", "warning",
					       "critical", "unknown" };

	ev.has_time = 1;
	ev.time = time(NULL);
	ev.state = states[stats->state];
	ev.service = "mysql";
	ev.host = stats->hostname;
	ev.description = stats->description;
	ev.has_ttl = 1;
	ev.ttl = stats->ttl;
	ev.n_tags = stats->tagcount;
	ev.tags = stats->tags;
	ev.has_metric_f = stats->has_metric;
	ev.metric_f = stats->metric;

	evtab[0] = &ev;
	msg.n_events = 1;
	msg.events = evtab;

	m->len = msg__get_packed_size(&msg);
	m->buf = calloc(1, m->len);
	msg__pack(&msg, m->buf);

	return 0;
}

/*
 * Attempt to get a socket connected to a riemann server
 */
int
riemann_connect(struct riemann_host *rh)
{
	int		 e;
	struct addrinfo	*res, *ai, hints;

	if (rh->status == S_UP)
		return 0;

	bzero(&hints, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_flags = 0;
	hints.ai_protocol = 0;

	if ((e = getaddrinfo(rh->host, NULL, &hints, &res)) != 0) {
		log_warnx("cannot lookup: %s: %s", rh->host, gai_strerror(e));
		return -1;
	}

	for (ai = res; ai != NULL; ai = ai->ai_next) {
		if ((rh->s = socket(ai->ai_family, ai->ai_socktype,
				    ai->ai_protocol)) == -1)
			continue;
		/* ugly cast to force port to our value */
		((struct sockaddr_in *)ai->ai_addr)->sin_port = htons(rh->port);
		if (connect(rh->s, ai->ai_addr, ai->ai_addrlen) != -1)
			break;

		close(rh->s);
	}

	freeaddrinfo(res);
	if (ai == NULL)
		return -1;
	return 0;
}

/*
 * Atomically send a riemann event
 */
int
riemann_send(struct riemann_host *rh, struct riemann_msg *msg)
{
	int	nlen;

	if (riemann_connect(rh) == -1) {
		log_warnx("could not connect to riemann %s:%s",
			  rh->host, rh->port);
		free(msg->buf);
		return -1;
	}

	nlen = htonl(msg->len);

	/* TODO: loop through write */
	if (write(rh->s, &nlen, sizeof(nlen)) != sizeof(nlen)) {
		log_warnx("short write for len");
		close(rh->s);
		rh->status = S_DOWN;
		goto out;
	}

	/* TODO: loop through write */
	if (write(rh->s, msg->buf, msg->len)  != msg->len) {
		log_warnx("short write for buf");
		close(rh->s);
		rh->status = S_DOWN;
	}

 out:
	free(msg->buf);
	return 0;
}

/* Bail */
void
usage(const char *progname)
{
	fprintf(stderr, "usage: %s [-d] [-f config]\n",
		progname);
	_exit(1);
}

int
main(int argc, char *argv[])
{
	int			 i, c;
	int			 debug = 0;
	int			 interval = 30;
	int			 tagcount = 0;
	double			 delay = DEFAULT_DELAY;
	int			 waitfor;
	const char		*progname = argv[0];
	char			 config[MAXLEN];
	char			 confline[MAXLEN];
	char			 hostname[MAXLEN];
	char			*tags[MAXTAGS];
	char			*key, *val;
	time_t		         start_ts, end_ts;
	size_t			 len, read;
	struct mysql_handler	 hdl;
	struct mysql_stats	 stats;
	struct riemann_host	 rh;
	struct riemann_msg	 msg;
	FILE			*fd;

	bzero(config, sizeof(config));
	strncpy(config, DEFAULT_CONFIG, strlen(DEFAULT_CONFIG) + 1);

	/* parse command line opts */
	while ((c = getopt(argc, argv, "df:")) != -1) {
		switch (c) {
		case 'd':
			debug += 1;
			break;
		case 'f':
			strncpy(config, optarg, sizeof(config) - 1);
			config[sizeof(config) - 1] = '\0';
			break;
		default:
			usage(progname);
		}
	}

	argc -= optind;
	argv += optind;

	/* fill in default values */
	bzero(&rh, sizeof(rh));
	strncpy(rh.host, DEFAULT_RIEMANN_HOST,
		strlen(DEFAULT_RIEMANN_HOST) + 1);
	rh.port = DEFAULT_RIEMANN_PORT;
	
	bzero(&hdl, sizeof(hdl));
	strncpy(hdl.host, DEFAULT_MYSQL_HOST, strlen(DEFAULT_MYSQL_HOST) + 1);
	strncpy(hdl.dbname, DEFAULT_MYSQL_DB, strlen(DEFAULT_MYSQL_DB) + 1);
	hdl.port = DEFAULT_MYSQL_PORT;

	if (gethostname(hostname, MAXLEN - 1) == -1) {
		strncpy(hostname, DEFAULT_HOSTNAME,
			strlen(DEFAULT_HOSTNAME) + 1);
	}

	log_init(progname, debug + 1);
	/*
	 * ghetto parser for a simple key = value config file format
	 */
	if ((fd = fopen(config, "r")) == NULL)
		err(1, "cannot open configuration");


	len = 0;
	while ((read = getline(&key, &len, fd)) != -1) {
		if (len >= MAXLEN)
			err(1, "config line too wide");
		strncpy(confline, key, len);

		if ((confline[0] == '#') || (confline[0] == '\n'))
			continue;
		key = confline + strspn(confline, " \t");
		val = confline + strcspn(confline, "=") + 1;
		key[strcspn(key, " \t=")] = '\0';
		val += strspn(val, " \t");
		val[strcspn(val, " \t\n")] = '\0';

		log_debug("configuration parsed key = %s, val = %s", key, val);

		/*
		 * not doing any boundary check here since we
		 * already asserted that the line length was
		 * not wider than MAXLEN
		 */
		if (strcasecmp(key, "mysql_host") == 0) {
			strncpy(hdl.host, val, strlen(val) + 1);
		} else if (strcasecmp(key, "mysql_port") == 0) {
			hdl.port = atoi(val);
		} else if (strcasecmp(key, "mysql_user") == 0) {
			strncpy(hdl.user, val, strlen(val) + 1);
		} else if (strcasecmp(key, "mysql_password") == 0) {
			strncpy(hdl.password, val, strlen(val) + 1);
		} else if (strcasecmp(key, "mysql_database") == 0) {
			strncpy(hdl.dbname, val, strlen(val) + 1);
		} else if (strcasecmp(key, "riemann_host") == 0) {
			strncpy(rh.host, val, strlen(val) + 1);
		} else if (strcasecmp(key, "riemann_port") == 0) {
			rh.port = atoi(val);
		} else if (strcasecmp(key, "interval") == 0) {
			interval = atoi(val);
		} else if (strcasecmp(key, "delay") == 0) {
			interval = atof(val);
		} else if (strcasecmp(key, "hostname") == 0) {
			strncpy(hostname, val, strlen(val) + 1);
		} else if (strcasecmp(key, "tags") == 0) {
			for (i = 0; i < tagcount; i++) {
				free(tags[i]);
				tags[i] = NULL;
			}
			tagcount = 0;
			do {
				if (tagcount >= MAXTAGS)
					errx(1, "too many tags");
				key = val;
				val = val + strcspn(key, " \t\n") + 1;
				key[strcspn(key, " \t\n")] = '\0';
				tags[tagcount++] = strdup(key);
				log_debug("adding tag: %s, tagcount: %d", key, tagcount);
			} while (*val);
		} else {
			errx(1, "invalid configuration directive: %s", key);
		}
	}
	
	/* sanity checks */
	if (interval <= 0)
		usage(progname);

	log_init(PROGNAME, debug);
	log_info("starting " PROGNAME " loop, using hostname: %s", hostname);

	/* main loop */
	while (1) {
		start_ts = time(NULL);
		bzero(&stats, sizeof(stats));
		bzero(&msg, sizeof(msg));
		log_debug("getting mysql handle");
		if (mysql_get_handle(&hdl) == -1) {
			log_warnx("could not get mysql handle");
			sleep(interval);
			continue;
		}
		log_debug("gathering statistics");
		mysql_gather(&hdl, &stats);

		/*
		 * Set these fields systematically
		 */
		stats.ttl = interval + delay;
		stats.hostname = hostname;
		stats.tagcount = tagcount;
		for (i = 0; i < tagcount; i++)
			stats.tags[i] = tags[i];

		log_debug("packing riemann message");
		riemann_pack(&msg, &stats);
		log_debug("sending riemann message");
		riemann_send(&rh, &msg);
		end_ts = time(NULL);

		waitfor = interval - (end_ts - start_ts);
		log_debug("got wait interval: %d", waitfor);
		if (waitfor > 0)
			sleep(waitfor);
	}

	return 0;
}
