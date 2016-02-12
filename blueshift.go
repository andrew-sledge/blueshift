package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/fzzy/radix/extra/cluster"
	"github.com/fzzy/radix/redis"
	"github.com/gorilla/mux"
	"log"
	"menteslibres.net/gosexy/yaml"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"
)

type IngestMessage struct {
	Timestamp string
	Subject   string
	Detail    string
	Magnitude int
	Severity  int
	Extra     string
}

type Message struct {
	Timestamp string
	Group     string
	Source    string
	Subject   string
	Detail    string
	Magnitude int
	Severity  int
	Extra     string
}

func nzl(s string) bool {
	if utf8.RuneCountInString(s) <= 0 {
		return false
	}
	// Not Zero Length
	return true
}

func isn(n int) bool {
	return reflect.ValueOf(n).Kind() == reflect.Int
}

func PushNode(settings *yaml.Yaml, message Message) {
	debug := settings.Get("debug").(bool)
	redis_connection := settings.Get("redis_connection").(string)
	redis_list := settings.Get("redis_list").(string)
	redis_db := settings.Get("redis_db").(int)

	t := time.Now()
	ts := t.Format("Mon Jan 2 15:04:05 -0700 MST 2006")
	rc, err := redis.Dial("tcp", redis_connection)
	if err != nil {
		fmt.Printf("[%s] ERROR Error received: %s\n", ts, err)
	}
	if debug {
		fmt.Printf("[%s] INFO Connection made to Redis server %s\n", ts, redis_connection)
	}
	r := rc.Cmd("SELECT", redis_db)
	if r.Err != nil {
		fmt.Printf("[%s] ERROR Error received: %s\n", ts, r.Err)
	} else {
		if debug {
			fmt.Printf("[%s] INFO Redis database selected: %d\n", ts, redis_db)
		}
	}
	j, errj := json.Marshal(message)
	if errj != nil {
		fmt.Printf("[%s] ERROR Error received: %s\n", ts, errj)
	}
	r = rc.Cmd("LPUSH", redis_list, j)
	rc.Close()
}

func PushCluster(settings *yaml.Yaml, message Message) {
	debug := settings.Get("debug").(bool)
	redis_connection := settings.Get("redis_connection").(string)
	redis_list := settings.Get("redis_list").(string)
	redis_db := settings.Get("redis_db").(int)

	t := time.Now()
	ts := t.Format("Mon Jan 2 15:04:05 -0700 MST 2006")
	rc, err := cluster.NewCluster(redis_connection)
	if err != nil {
		fmt.Printf("[%s] ERROR Error received: %s\n", ts, err)
	}
	if debug {
		fmt.Printf("[%s] INFO Connection made to Redis server %s\n", ts, redis_connection)
	}
	r := rc.Cmd("SELECT", 0)
	if r.Err != nil {
		fmt.Printf("[%s] ERROR Error received: %s\n", ts, r.Err)
	} else {
		if debug {
			fmt.Printf("[%s] INFO Redis database selected: %d\n", ts, redis_db)
		}
	}
	j, errj := json.Marshal(message)
	if errj != nil {
		fmt.Printf("[%s] ERROR Error received: %s\n", ts, errj)
	}
	r = rc.Cmd("LPUSH", redis_list, j)
	rc.Close()
}

func IngestHandler(w http.ResponseWriter, r *http.Request) {

	var yml_config_file string
	y_c, e := os.LookupEnv("BLUESHIFT_CONFIG")
	if e == false {
		yml_config_file = "config.yml"
	} else {
		yml_config_file = y_c
	}

	settings, err := yaml.Open(yml_config_file)
	if err != nil {
		fmt.Println("TODO: fix errors")
	}

	vars := mux.Vars(r)
	group := vars["group"]
	source := vars["source"]

	t := time.Now()
	ts := t.Format("Mon Jan 2 15:04:05 -0700 MST 2006")
	debug := settings.Get("debug").(bool)

	if r.Method == "GET" {
		http.Error(w, "Method not allowed", 405)
	} else if r.Method == "POST" {
		if debug {
			fmt.Printf("[%s] INFO Message received: %+v\n", ts, r.Body)
		}
		decoder := json.NewDecoder(r.Body)
		var t IngestMessage
		err := decoder.Decode(&t)
		if err != nil {
			fmt.Printf("[%s] ERROR Error received decoding JSON: %s\n", ts, err)
		}
		if nzl(t.Timestamp) && nzl(t.Subject) && nzl(t.Detail) && isn(t.Severity) && isn(t.Magnitude) {
			// Proceed
			var message Message
			message.Timestamp = t.Timestamp
			message.Group = group
			message.Source = source
			message.Subject = t.Subject
			message.Detail = t.Detail
			message.Magnitude = t.Magnitude
			message.Severity = t.Severity
			message.Extra = t.Extra

			redis_is_cluster := settings.Get("redis_is_cluster").(bool)
			if redis_is_cluster {
				if debug {
					fmt.Printf("[%s] INFO Starting up in cluster mode\n", ts)
				}
				PushCluster(settings, message)
			} else {
				if debug {
					fmt.Printf("[%s] INFO Starting up in single node mode\n", ts)
				}
				PushNode(settings, message)
			}
		} else {
			// Return 5XX?
			http.Error(w, "Input validation failed.", 500)
		}
	} else {
		// Unknown
		fmt.Printf("[%s] ERROR Unknown HTTP method.\n", ts)
	}
}

func RedisStatusHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Hello")
	fmt.Println("hello world")
}

func M(h http.HandlerFunc, middleware ...func(http.HandlerFunc) http.HandlerFunc) http.HandlerFunc {
	for _, m := range middleware {
		h = m(h)
	}
	return h
}

func AuthHandler(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)

		s := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
		if len(s) != 2 {
			http.Error(w, "Not authorized", 401)
			return
		}

		b, err := base64.StdEncoding.DecodeString(s[1])
		if err != nil {
			http.Error(w, err.Error(), 401)
			return
		}

		pair := strings.SplitN(string(b), ":", 2)
		if len(pair) != 2 {
			http.Error(w, "Not authorized", 401)
			return
		}

		// TODO: read htaccess file
		if pair[0] != "username" || pair[1] != "password" {
			http.Error(w, "Not authorized", 401)
			return
		}

		h.ServeHTTP(w, r)
	}
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/b/{group}/{source}", M(IngestHandler, AuthHandler))
	r.HandleFunc("/redis-status", RedisStatusHandler)
	log.Fatal(http.ListenAndServe(":8080", r))

}
