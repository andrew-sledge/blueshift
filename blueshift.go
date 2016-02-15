package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/abbot/go-http-auth"
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

func IngestHandler(w http.ResponseWriter, r *auth.AuthenticatedRequest) {

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

	url_pieces := strings.Split(r.URL.String(), "/")

	group := url_pieces[2]
	source := url_pieces[3]

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

func Authenticator(user, realm string) string {
	if file, err := os.Open(".htdigest"); err == nil {
		defer file.Close()
		scanner := bufio.NewScanner(file)
		var line string
		user_realm := fmt.Sprintf("%s:%s", user, realm)
		for scanner.Scan() {
			line = scanner.Text()
			if strings.Contains(line, user_realm) {
				r := strings.Split(line, ":")
				if r[0] == user && r[1] == realm {
					return r[2]
				}
			}
		}

		if err = scanner.Err(); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal(err)
	}
	return ""
}

func main() {
	r := mux.NewRouter()
	authenticator := auth.NewDigestAuthenticator("doppler", Authenticator)
	r.HandleFunc("/b/{group}/{source}", authenticator.Wrap(IngestHandler))
	r.HandleFunc("/redis-status", RedisStatusHandler)
	log.Fatal(http.ListenAndServe(":8080", r))

}
