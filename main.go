package main

import (
	"bufio"
	"flag"
	"fmt"
	"log/syslog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/amir/raidman"
	mysql "github.com/siddontang/go-mysql/client"
	"gopkg.in/inconshreveable/log15.v2"
)

var (
	mysqlHost     string = "localhost"
	mysqlPort     string = "3306"
	mysqlUser     string = "root"
	mysqlPassword string = "root"
	mysqlDatabase string = ""
	riemannHost   string = "localhost"
	riemannPort   string = "5555"
	riemannTTL    float32
	riemannTags   []string

	hostname   string
	interval   time.Duration = time.Second * 30
	delay      float64       = 2.0
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

	if configFile != "" {
		log.Debug("loading configuratin file", "path", configFile)
		if err := loadConfig(configFile); err != nil {
			dieOnError(fmt.Sprintf("unable to load configuration: %s", err))
		}
	}

	riemannTTL = float32(interval + time.Duration(delay))
}

func loadConfig(path string) error {
	var (
		k, v string
	)

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}

		items := strings.Split(line, "=")
		if len(items) != 2 {
			return fmt.Errorf("malformated line %q", line)
		}

		k, v = strings.TrimSpace(items[0]), strings.TrimSpace(items[1])
		log.Debug("parsed configuration line",
			"key", k,
			"value", v)

		switch k {
		case "mysql_host":
			mysqlHost = v

		case "mysql_port":
			mysqlPort = v

		case "mysql_user":
			mysqlUser = v

		case "mysql_password":
			mysqlPassword = v

		case "mysql_database":
			mysqlDatabase = v

		case "riemann_host":
			riemannHost = v

		case "riemann_port":
			riemannPort = v

		case "interval":
			i, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return fmt.Errorf("invalid value %q for setting `interval`", v)
			}
			interval = time.Duration(i) * time.Second

		case "delay":
			d, err := strconv.ParseFloat(v, 32)
			if err != nil {
				return fmt.Errorf("invalid value %q for setting `delay`", v)
			}
			delay = d

		case "hostname":
			hostname = v

		case "tags":
			riemannTags = strings.Split(v, " ")

		default:
			log.Warn(fmt.Sprintf("unsupported configuration setting %q", k))
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
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
					Ttl:     float32(interval.Seconds() + delay),
					Tags:    riemannTags,
				}
				if hostname != "" {
					event.Host = hostname
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
