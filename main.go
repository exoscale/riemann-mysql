package main

import (
	"flag"
	"fmt"
	"log/syslog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/amir/raidman"
	mysql "github.com/siddontang/go-mysql/client"
	"gopkg.in/inconshreveable/log15.v2"
)

var (
	mysqlHost     string   = "localhost"        // TODO
	mysqlPort     string   = "3306"             // TODO
	mysqlUser     string   = "root"             // TODO
	mysqlPassword string   = "root"             // TODO
	mysqlDatabase string   = "mysql"            // TODO
	riemannHost   string   = "localhost"        // TODO
	riemannPort   string   = "5555"             // TODO
	riemannTTL    float32  = 60                 // TODO
	riemannTags   []string = []string{"a", "b"} // TODO

	interval   time.Duration = time.Second * 10 // TODO
	delay      float64       = 2.0              // TODO
	log        log15.Logger
	configFile string
	debug      bool
)

func init() {
	var (
		h   log15.Handler
		err error
	)

	flag.BoolVar(&debug, "d", false, "run in debug mode")
	flag.StringVar(&configFile, "f", "", "path to configuration file")
	flag.Parse()

	log = log15.New()
	if debug {
		h = log15.LvlFilterHandler(log15.LvlDebug, log15.StderrHandler)
	} else {
		if h, err = log15.SyslogHandler(syslog.LOG_WARNING, "riemann-mysql", log15.LogfmtFormat()); err != nil {
			fmt.Fprintf(os.Stderr, "error: unable to initialize syslog logging: %s", err)
			os.Exit(1)
		}
		h = log15.LvlFilterHandler(log15.LvlError, h)
	}
	log.SetHandler(h)
}

func main() {
	var (
		riemann *raidman.Client
		db      *mysql.Conn
		err     error
	)

	// Handle termination signals
	sig := make(chan os.Signal, 1)
	stop := make(chan bool, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sig // Block until we receive a notification on the chan from signal handler
		log.Info("terminating")
		stop <- true
	}()

	if riemann, err = raidman.Dial("tcp4", net.JoinHostPort(riemannHost, riemannPort)); err != nil {
		dieOnError(fmt.Sprintf("unable to connect to riemann server: %s", err))
	}

	go func() {
		log.Info("starting")

		for {
			log.Debug("getting database handle")
			if db, err = getDbHandle(db); err != nil {
				log.Warn("unable to get database handle", "error", err)
				time.Sleep(interval)
				continue
			}

			events := make([]*raidman.Event, 0)
			t := time.Now()

			log.Debug("gathering statistics")
			r, err := db.Execute("SHOW ALL SLAVES STATUS")
			if err != nil {
				log.Warn("unable to query replication status", "error", err)
				events = append(events, &raidman.Event{
					Time:        t.Unix(),
					Service:     "mysql/replication",
					State:       "unknown",
					Description: fmt.Sprintf("unable to query replication status: %s", err),
					Tags:        riemannTags,
					Ttl:         float32(interval.Seconds() + delay),
				})
				goto send
			}

			for i := 0; i < r.Resultset.RowNumber(); i++ {
				event := &raidman.Event{
					Time:    t.Unix(),
					Service: fmt.Sprintf("mysql/replication/conn%d", i),
					State:   "ok",
					Tags:    riemannTags,
					Ttl:     float32(interval.Seconds() + delay),
				}

				if connName, _ := r.Resultset.GetStringByName(i, "Connection_name"); connName != "" {
					event.Service = fmt.Sprintf("mysql/replication/%s", connName)
				}

				sqlSlaveRunning, err := r.Resultset.GetStringByName(i, "Slave_SQL_Running")
				if err != nil {
					event.State = "unknown"
					event.Description = fmt.Sprintf("unable to retrieve SQL slave state: %s", err)
					events = append(events, event)
					log.Warn(event.Description)
					continue
				} else if threadState(sqlSlaveRunning) != "running" {
					event.State = "warning"
				}

				ioSlaveRunning, err := r.Resultset.GetStringByName(i, "Slave_IO_Running")
				if err != nil {
					event.State = "unknown"
					event.Description = fmt.Sprintf("unable to retrieve IO thread state: %s", err)
					events = append(events, event)
					log.Warn(event.Description)
					continue
				} else if threadState(ioSlaveRunning) != "running" {
					event.State = "critical"
				}

				secondsBehind, err := r.Resultset.GetIntByName(i, "Seconds_Behind_Master")
				if err != nil {
					event.State = "unknown"
					event.Description = fmt.Sprintf("unable to retrieve replication lag value: %s", err)
					events = append(events, event)
					log.Warn(event.Description)
					continue
				}

				log.Debug("gathered",
					"connection", strings.Split(event.Service, "/")[2],
					"sql_thread", threadState(sqlSlaveRunning),
					"io_thread", threadState(ioSlaveRunning),
					"seconds_behind", secondsBehind)

				event.Description = fmt.Sprintf("slave io: %s, slave sql: %s",
					threadState(ioSlaveRunning),
					threadState(sqlSlaveRunning))
				event.Metric = secondsBehind
				events = append(events, event)
			}

		send:
			log.Debug("sending Riemann events")
			if err := riemann.SendMulti(events); err != nil {
				log.Error("unable to send Riemann events", "error", err)
			}

			waitFor := interval - time.Now().Sub(t)
			log.Debug("wait interval", "duration", waitFor)
			if waitFor > 0 {
				time.Sleep(waitFor)
			}
		}
	}()

	<-stop

	db.Close()
	riemann.Close()
}

func dieOnError(msg string) {
	log.Error(msg)
	os.Exit(1)
}

func getDbHandle(db *mysql.Conn) (*mysql.Conn, error) {
	if db != nil {
		if err := db.Ping(); err != nil {
			return nil, err
		}

		return db, nil
	}

	return mysql.Connect(net.JoinHostPort(mysqlHost, mysqlPort), mysqlUser, mysqlPassword, mysqlDatabase)
}

func threadState(s string) string {
	if strings.EqualFold(s, "yes") {
		return "running"
	}

	return "stopped"
}
