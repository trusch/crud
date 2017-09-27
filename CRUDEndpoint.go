package crud

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"github.com/trusch/streamstore"
)

// Endpoint is an http.Handler which serves CRUD requests
type Endpoint struct {
	router *mux.Router
	store  streamstore.Storage
	prefix string
}

// ServeHTTP is the function needed to implement http.Handler
func (endpoint *Endpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	endpoint.router.ServeHTTP(w, r)
	return
}

// NewEndpoint constructs a new handler instances
func NewEndpoint(prefix string, store streamstore.Storage) http.Handler {
	endpoint := &Endpoint{mux.NewRouter(), store, prefix}
	endpoint.router.Path("/").Methods("POST").HandlerFunc(endpoint.handlePost)
	endpoint.router.Path("/").Methods("GET").HandlerFunc(endpoint.handleList)
	endpoint.router.Path("/{id}").Methods("GET").HandlerFunc(endpoint.handleGet)
	endpoint.router.Path("/{id}").Methods("PUT").HandlerFunc(endpoint.handlePut)
	endpoint.router.Path("/{id}").Methods("PATCH").HandlerFunc(endpoint.handlePatch)
	endpoint.router.Path("/{id}").Methods("DELETE").HandlerFunc(endpoint.handleDel)
	return endpoint
}

func (endpoint *Endpoint) handlePost(w http.ResponseWriter, r *http.Request) {
	log.Debugf("POST request to %v", r.URL)
	if r.Body == nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("no body supplied"))
		return
	}
	id := uuid.NewV4()
	objectID := fmt.Sprintf("%v::%v", endpoint.prefix, id.String())
	writer, err := endpoint.store.GetWriter(objectID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	_, err = io.Copy(writer, r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	err = writer.Close()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(id.String()))
}

func (endpoint *Endpoint) handleGet(w http.ResponseWriter, r *http.Request) {
	log.Debugf("GET request to %v", r.URL)
	vars := mux.Vars(r)
	id := vars["id"]
	objectID := fmt.Sprintf("%v::%v", endpoint.prefix, id)
	if !endpoint.store.Has(objectID) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("object not found"))
		return
	}
	reader, err := endpoint.store.GetReader(objectID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	log.Debug("got reader")
	_, err = io.Copy(w, reader)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	reader.Close()
}

func (endpoint *Endpoint) handleList(w http.ResponseWriter, r *http.Request) {
	log.Debugf("GET request to list", r.URL)
	keys, err := endpoint.store.List(endpoint.prefix)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	for id := range keys {
		keys[id] = strings.Split(keys[id], "::")[1]
	}
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	encoder.Encode(keys)
}

func (endpoint *Endpoint) handlePut(w http.ResponseWriter, r *http.Request) {
	log.Debugf("PUT request to %v", r.URL)
	if r.Body == nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("no body supplied"))
		return
	}
	vars := mux.Vars(r)
	id := vars["id"]
	objectID := fmt.Sprintf("%v::%v", endpoint.prefix, id)
	writer, err := endpoint.store.GetWriter(objectID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	_, err = io.Copy(writer, r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	err = writer.Close()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write([]byte(id))
}
func (endpoint *Endpoint) handleDel(w http.ResponseWriter, r *http.Request) {
	log.Debugf("DELETE request to %v", r.URL)
	vars := mux.Vars(r)
	id := vars["id"]
	objectID := fmt.Sprintf("%v::%v", endpoint.prefix, id)
	if !endpoint.store.Has(objectID) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("object not found"))
		return
	}
	err := endpoint.store.Delete(objectID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
}

func (endpoint *Endpoint) handlePatch(w http.ResponseWriter, r *http.Request) {
	log.Debugf("PATCH request to %v", r.URL)
	vars := mux.Vars(r)
	id := vars["id"]
	objectID := fmt.Sprintf("%v::%v", endpoint.prefix, id)
	if !endpoint.store.Has(objectID) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("object not found"))
		return
	}

	// get old object
	reader, err := endpoint.store.GetReader(objectID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	oldObject := make(map[string]interface{})
	decoder := json.NewDecoder(reader)
	if err = decoder.Decode(&oldObject); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	// get patch object
	decoder = json.NewDecoder(r.Body)
	patchObject := make(map[string]interface{})
	if err = decoder.Decode(&patchObject); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	// merge objects
	for key, val := range patchObject {
		oldObject[key] = val
	}

	// save object
	writer, err := endpoint.store.GetWriter(objectID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	defer writer.Close()
	encoder := json.NewEncoder(io.MultiWriter(writer, w))
	encoder.Encode(oldObject)
}
