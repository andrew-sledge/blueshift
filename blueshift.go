package main

import (
	"fmt"
	//"github.com/fzzy/radix/extra/cluster"
	//"github.com/fzzy/radix/redis"
	"encoding/base64"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"strings"
)

func IngestHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method == "GET" {
		vars := mux.Vars(r)
		group := vars["group"]
		fmt.Fprintln(w, "Group:", group)
		source := vars["source"]
		fmt.Fprintln(w, "source:", source)
		fmt.Fprintln(w, "Method:", r.Method)
	} else if r.Method == "POST" {
		fmt.Fprintln(w, "Method:", r.Method)
	} else {
		// Unknown
		fmt.Println("Unknown method")
	}
}

func RedisStatusHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("hello world")
}

func HandlerWrapper(h http.HandlerFunc, middleware ...func(http.HandlerFunc) http.HandlerFunc) http.HandlerFunc {
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

		if pair[0] != "username" || pair[1] != "password" {
			http.Error(w, "Not authorized", 401)
			return
		}

		h.ServeHTTP(w, r)
	}
}

func main() {

	r := mux.NewRouter()
	r.HandleFunc("/b/{group}/{source}", HandlerWrapper(IngestHandler, AuthHandler))
	r.HandleFunc("/redis-status", RedisStatusHandler)
	log.Fatal(http.ListenAndServe(":8080", r))

}
