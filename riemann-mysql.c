/*
 * riemann mysql, see https://github.com/exoscale/riemann-mysql
 *
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
#include <limits.h>
#include <riemann/riemann-client.h>

#define PROGNAME                "riemann-mysql"
#define DEFAULT_CONFIG          "/etc/riemann-mysql.conf"
#define DEFAULT_INTERVAL        30
#define DEFAULT_DELAY           2.0
#define DEFAULT_HOSTNAME        "<unknown>"
#define DEFAULT_RIEMANN_HOST    "localhost"
#define DEFAULT_RIEMANN_PORT    5555
#define DEFAULT_MYSQL_HOST      "localhost"
#define DEFAULT_MYSQL_DB        ""
#define DEFAULT_MYSQL_PORT      3306
#define MYSQL_QUERY             "show slave status"
#define MYSQL_SLAVE_IO          10
#define MYSQL_SLAVE_SQL         11
#define MYSQL_SECONDS_BEHIND    32
#define MYSQL_MIN_FIELDS        33
#define MAXLEN                  1024
#define MAXTAGS                 32

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
    S_DOWN      = 0,
    S_UP
};

enum state {
    S_OK        = 0,
    S_WARNING   = 1,
    S_CRITICAL  = 2,
    S_UNKNOWN   = 3
};

struct mysql_handler {
    enum status  status;
    MYSQL       *db;
    char         host[HOST_NAME_MAX];
    int          port;
    char         user[MAXLEN];
    char         password[MAXLEN];
    char         dbname[MAXLEN];
};

char             hostname[HOST_NAME_MAX];
char            *tags[MAXTAGS];
int              tagcount;

/* from log.c */
void             log_init(const char *, int);
void             log_warn(const char *, ...);
void             log_warnx(const char *, ...);
void             log_info(const char *, ...);
void             log_debug(const char *, ...);
void             fatal(const char *);
void             fatalx(const char *);

void             usage(const char *);
int              mysql_get_handle(struct mysql_handler *);
MYSQL_RES       *mysql_run_query(struct mysql_handler *, const char *);
riemann_event_t *mysql_gather(struct mysql_handler *);

/*
 * Attempt to get a working mysql connection, when
 * a previous succesful connection exists, ping it
 * to make sure it is still useable.
 */
int
mysql_get_handle(struct mysql_handler *hdl)
{
    int res;

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
                           (strlen(hdl->dbname) > 0 ? hdl->dbname : NULL),
                           hdl->port, NULL, 0) == NULL) {
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
riemann_event_t *
mysql_gather(struct mysql_handler *hdl)
{
    int              desclen;
    int              io;
    int              sql;
    enum state       state;
    int              has_metric;
    double           metric;
    char             description[MAXLEN];
    char            *states[4] = { "ok", "warning", "critical", "unknown" };
    MYSQL_RES       *res;
    MYSQL_ROW        row;
    riemann_event_t *ev;

    bzero(description, sizeof(description));
    desclen = sizeof(description) - 1;

    if (mysql_real_query(hdl->db, MYSQL_QUERY, strlen(MYSQL_QUERY))) {
        log_warn("could not execute query: %s", mysql_error(hdl->db));
        state = S_UNKNOWN;
        strncpy(description, mysql_error(hdl->db), desclen);
        goto out;
    }

    res = mysql_store_result(hdl->db);
    if (res == NULL) {
        log_warn("could not tore result: %s", mysql_error(hdl->db));
        state = S_CRITICAL;
        strncpy(description, mysql_error(hdl->db), desclen);
        goto out;
    }

    row = mysql_fetch_row(res);
    if (row == NULL) {
        log_warn("could not fetch row: %s", mysql_error(hdl->db));
        state = S_UNKNOWN;
        strncpy(description, mysql_error(hdl->db), desclen);
        mysql_free_result(res);
        goto out;
    }

    if (mysql_num_fields(res) < MYSQL_MIN_FIELDS) {
        log_warnx("not enough fields given");
        state = S_UNKNOWN;
        strncpy(description, "fields missing", desclen);
        mysql_free_result(res);
    }

    io = (row[MYSQL_SLAVE_IO] != NULL &&
          (strcasecmp(row[MYSQL_SLAVE_IO], "yes") == 0));
    sql = (row[MYSQL_SLAVE_SQL] != NULL &&
           (strcasecmp(row[MYSQL_SLAVE_SQL], "yes") == 0));

    if (row[MYSQL_SECONDS_BEHIND] != NULL) {
        has_metric = 1;
        metric = atoll(row[MYSQL_SECONDS_BEHIND]);
    } else {
        has_metric = 0;
    }

    snprintf(description, desclen, "slave io: %s, slave sql: %s",
             (io ? "running" : "stopped"), (sql ? "running" : "stopped"));
    if (io && sql) {
        state = S_OK;
    } else {
        if (!sql)
            state = S_WARNING;
        if (!io)
            state = S_CRITICAL;
    }
    log_debug("gathered: state = %d, description = %s, metric = %f",
              state, description, metric);
    mysql_free_result(res);

 out:
    ev = riemann_event_create(RIEMANN_EVENT_FIELD_HOST,
                              hostname,
                              RIEMANN_EVENT_FIELD_SERVICE,
                              "mysql/replication",
                              RIEMANN_EVENT_FIELD_TIME,
                              (int64_t)time(NULL),
                              RIEMANN_EVENT_FIELD_STATE,
                              states[state],
                              RIEMANN_EVENT_FIELD_DESCRIPTION,
                              description,
                              RIEMANN_EVENT_FIELD_NONE);
    if (ev == NULL)
        err(1, "cannot build riemann event");
    if (has_metric) {
        riemann_event_set_one(ev, METRIC_D, metric);
    }
    return ev;
}

/* Bail */
void
usage(const char *progname)
{
    fprintf(stderr, "usage: %s [-d] [-f config]\n", progname);
    _exit(1);
}

int
main(int argc, char *argv[])
{
    int                      i;
    int                      e;
    int                      c;
    int                      debug = 0;
    int                      interval = 30;
    double                   delay = DEFAULT_DELAY;
    int                      waitfor;
    const char              *progname = argv[0];
    char                     config[MAXLEN];
    char                     confline[MAXLEN];
    char                    *key;
    char                    *val;
    time_t                   start_ts;
    time_t                   end_ts;
    size_t                   len;
    size_t                   read;
    struct mysql_handler     hdl;
    char                     riemann_host[HOST_NAME_MAX];
    char                     riemann_cert[PATH_MAX];
    char                     riemann_cert_key[PATH_MAX];
    char                     riemann_ca_cert[PATH_MAX];
    int                      riemann_proto;
    int                      riemann_port;
    riemann_client_t        *client;
    riemann_message_t       *msg;
    riemann_event_t         *ev;
    FILE                    *fd;

    tagcount = 0;
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
    bzero(riemann_host, sizeof(riemann_host));
    bzero(riemann_cert, sizeof(riemann_cert));
    bzero(riemann_cert_key, sizeof(riemann_cert_key));
    bzero(riemann_ca_cert, sizeof(riemann_ca_cert));
    strncpy(riemann_host, DEFAULT_RIEMANN_HOST,
            strlen(DEFAULT_RIEMANN_HOST) + 1);
    riemann_port = DEFAULT_RIEMANN_PORT;
    riemann_proto = RIEMANN_CLIENT_TCP;

    bzero(&hdl, sizeof(hdl));
    strncpy(hdl.host, DEFAULT_MYSQL_HOST, strlen(DEFAULT_MYSQL_HOST) + 1);
    strncpy(hdl.dbname, DEFAULT_MYSQL_DB, strlen(DEFAULT_MYSQL_DB) + 1);
    hdl.port = DEFAULT_MYSQL_PORT;

    if (gethostname(hostname, HOST_NAME_MAX - 1) == -1) {
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
            strncpy(riemann_host, val, strlen(val) + 1);
        } else if (strcasecmp(key, "riemann_port") == 0) {
            riemann_port = atoi(val);
        } else if (strcasecmp(key, "riemann_cert") == 0) {
            strncpy(riemann_cert, val, strlen(val) + 1);
        } else if (strcasecmp(key, "riemann_cert_key") == 0) {
            strncpy(riemann_cert_key, val, strlen(val) + 1);
        } else if (strcasecmp(key, "riemann_ca_cert") == 0) {
            strncpy(riemann_ca_cert, val, strlen(val) + 1);
        } else if (strcasecmp(key, "riemann_proto") == 0) {
            if (strcasecmp(val, "tcp") == 0) {
                riemann_proto = RIEMANN_CLIENT_TCP;
            } else if (strcasecmp(val, "udp") == 0) {
                riemann_proto = RIEMANN_CLIENT_UDP;
            } else if (strcasecmp(val, "tls") == 0) {
                riemann_proto = RIEMANN_CLIENT_TLS;
            } else {
                errx(1, "invalid riemann protocol: %s", val);
            }
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
                tags[tagcount] = strdup(key);
                if (tags[tagcount] == NULL)
                    err(1, "cannot allocate tag");
                tagcount++;
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

    if ((client = riemann_client_new()) == NULL) {
        err(1, "cannot create riemann client");
    }

    /* main loop */
    while (1) {
        start_ts = time(NULL);
        if ((msg = riemann_message_new()) == NULL)
            err(1, "cannot allocate riemann_message");
        log_debug("getting mysql handle");
        if (mysql_get_handle(&hdl) == -1) {
            log_warnx("could not get mysql handle");
            sleep(interval);
            continue;
        }
        log_debug("gathering statistics");
        ev = mysql_gather(&hdl);
        riemann_event_set_one(ev, TTL, (double)(interval + delay));

        for (i = 0; i < tagcount; i++)
            riemann_event_tag_add(ev, tags[i]);

        log_debug("sending riemann message");
        e = riemann_client_connect(client,
                                   riemann_proto,
                                   riemann_host,
                                   riemann_port,
                                   RIEMANN_CLIENT_OPTION_TLS_CA_FILE,
                                   riemann_ca_cert,
                                   RIEMANN_CLIENT_OPTION_TLS_CERT_FILE,
                                   riemann_cert,
                                   RIEMANN_CLIENT_OPTION_TLS_KEY_FILE,
                                   riemann_cert_key,
                                   RIEMANN_CLIENT_OPTION_TLS_HANDSHAKE_TIMEOUT,
                                   10000,
                                   RIEMANN_CLIENT_OPTION_NONE);
        if (e != 0) {
            log_warn("could not connect to riemann host: %s\n",
                     strerror(-e));
        } else {
            riemann_message_append_events(msg, ev, NULL);
            riemann_client_send_message_oneshot(client, msg);
        }

        end_ts = time(NULL);
        waitfor = interval - (end_ts - start_ts);
        log_debug("got wait interval: %d", waitfor);
        if (waitfor > 0)
            sleep(waitfor);
    }

    return 0;
}
