package main

import (
	"errors"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

var store = make(map[string]string)
var ErrorNoSuchKey = errors.New("no such key")

func main() {
	r := mux.NewRouter()

	r.HandleFunc("/", pingHandler)
	r.HandleFunc("/v1/key/{key}", keyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/key/{key}", keyValueGetHandler).Methods("GET")
	r.HandleFunc("/v1/key/{key}", keyValueDeleteHandler).Methods("DELETE")

	log.Fatal(http.ListenAndServe(":8080", r))
}

func pingHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("pong"))
}

func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"] // retrieve "key" from the request

	value, err := io.ReadAll(r.Body) // the request body has our value
	defer r.Body.Close()

	if err != nil { // if we have an error, report it
		http.Error(w,
			err.Error(),
			http.StatusInternalServerError)
		return
	}

	err = Put(key, string(value)) // store the value as a string
	if err != nil {               // if we have an error, report it
		http.Error(w,
			err.Error(),
			http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := Get(key)
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(value))
}

func keyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	if err := Delete(key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func Put(key string, value string) error {
	store[key] = value

	return nil
}

func Get(key string) (string, error) {
	val, ok := store[key]

	if !ok {
		return "", ErrorNoSuchKey
	}

	return val, nil
}

func Delete(key string) error {
	delete(store, key)

	return nil
}
