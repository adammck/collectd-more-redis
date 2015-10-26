package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Metric struct {
	Section string
	Prefix  string
	Key     string
	Value   string
}

type Metrics []*Metric

const (
	DEFAULT_INTERVAL = "10.0"
)

var (
	redisHost = flag.String("host", "localhost", "redis hostname")
	redisPort = flag.Int("port", 6379, "redis port")
)

func main() {
	flag.Parse()

	interval, err := getInterval()
	if err != nil {
		fmt.Println("error parsing interval:")
		fmt.Println(err)
		os.Exit(1)
	}

	conn, err := getRedis(*redisHost, *redisPort)
	if err != nil {
		fmt.Println("error connecting to redis:")
		fmt.Println(err)
		os.Exit(1)
	}

	for {
		t := time.Now()

		ms, err := fetchMetrics(conn)
		if err != nil {
			fmt.Println("error fetching metrics:")
			fmt.Println(err)
			os.Exit(1)
		}

		for _, m := range ms {

			var pk string
			if m.Prefix != "" {
				pk = fmt.Sprintf("%s/%s", m.Prefix, m.Key)
			} else {
				pk = m.Key
			}

			f, err := strconv.ParseFloat(m.Value, 64)
			if err != nil {
				continue
			}

			fmt.Printf("PUTVAL redis/%s/%s interval=%f %d:%f\n", m.Section, pk, interval.Seconds(), t.Unix(), f)
		}
		time.Sleep(interval)
	}
}

func fetchMetrics(conn redis.Conn) (Metrics, error) {
	ms := make([]*Metric, 0)
	s := ""

	reply, err := conn.Do("INFO", "ALL")
	if err != nil {
		return ms, err
	}

	blob, err := redis.Bytes(reply, err)
	if err != nil {
		return ms, err
	}

	scanner := bufio.NewScanner(bytes.NewReader(blob))
	for scanner.Scan() {
		line := scanner.Text()

		// Ignore Empty lines
		if len(line) == 0 {
			continue
		}

		// Update the section name?
		if strings.HasPrefix(line, "#") {
			s = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(line, "#")))
			continue
		}

		// Add all metrics found on the line
		mms, _ := parseLine(s, line)
		for _, m := range mms {
			ms = append(ms, m)
		}
	}

	return ms, nil
}

func parseLine(section, line string) (Metrics, error) {
	ms := make([]*Metric, 0)

	// Comment lines aren't an error, but they're not a metric either.
	if strings.HasPrefix(line, "#") {
		return ms, nil
	}

	// All other lines should be in k:v form.
	parts := strings.SplitN(line, ":", 2)
	if len(parts) != 2 {
		return ms, fmt.Errorf("expected 2 parts, got %d", len(parts))
	}

	k := parts[0]
	v := parts[1]

	// The commandstats section is in a special format:
	// cmdstat_XXX: calls=XXX,usec=XXX,usec_per_call=XXX
	if strings.HasPrefix(k, "cmdstat_") || strings.HasPrefix(k, "db") {
		for _, m := range parseKVLine(section, k, v) {
			ms = append(ms, m)
		}
	} else {
		ms = append(ms, &Metric{
			Section: section,
			Key:     k,
			Value:   v,
		})
	}

	return ms, nil
}

func parseKVLine(section, prefix, v string) Metrics {
	ms := make(Metrics, 0)

	parts := strings.Split(v, ",")
	for _, pair := range parts {

		tupl := strings.SplitN(pair, "=", 2)
		if len(tupl) != 2 {
			continue
		}

		ms = append(ms, &Metric{
			Section: section,
			Prefix:  prefix,
			Key:     tupl[0],
			Value:   tupl[1],
		})
	}

	return ms
}

func getRedis(host string, port int) (redis.Conn, error) {
	addr := fmt.Sprintf("%s:%d", host, port)

	r, err := redis.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	s, err := redis.String(r.Do("PING"))
	if err != nil {
		return nil, err
	}

	if s != "PONG" {
		return nil, fmt.Errorf("expected PONG, got %v", s)
	}

	fmt.Printf("# connected to Redis server: %s\n", addr)
	return r, nil
}

func getInterval() (time.Duration, error) {
	s := os.Getenv("COLLECTD_INTERVAL")
	if s == "" {
		s = DEFAULT_INTERVAL
	}

	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}

	// Convert seconds float to nanoseconds int.
	return time.Duration(int(f * 1000000000)), nil
}
