package crud_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"

	. "github.com/trusch/crud"
	"github.com/trusch/streamstore"
	"github.com/trusch/streamstore/uriparser"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CRUDEndpoint", func() {
	var (
		recorder *httptest.ResponseRecorder
		handler  http.Handler
		store    streamstore.Storage
		err      error
	)

	BeforeEach(func() {
		recorder = httptest.NewRecorder()
		store, err = uriparser.NewFromURI("file:///tmp/test", nil)
		Expect(err).NotTo(HaveOccurred())
		handler = NewEndpoint("test", store)
	})

	AfterEach(func() {
		os.RemoveAll("/tmp/test")
	})

	It("should be possible to post and retrieve data", func() {
		content := "foobar"
		code, responseData := post(handler, "/", content)
		Expect(code).To(Equal(http.StatusCreated))
		Expect(responseData).NotTo(BeEmpty())
		code, responseData = get(handler, "/"+responseData)
		Expect(code).To(Equal(http.StatusOK))
		Expect(responseData).To(Equal(content))
	})

	It("should be possible to put and retrieve data", func() {
		content := "foobar"
		code, responseData := put(handler, "/key", content)
		Expect(code).To(Equal(http.StatusOK))
		Expect(responseData).To(Equal("key"))
		code, responseData = get(handler, "/key")
		Expect(code).To(Equal(http.StatusOK))
		Expect(responseData).To(Equal(content))
	})

	It("should be possible to list data", func() {
		content := "foobar"
		put(handler, "/key1", content)
		put(handler, "/key2", content)
		put(handler, "/key3", content)
		put(handler, "/key4", content)
		code, responseData := get(handler, "/")
		Expect(code).To(Equal(http.StatusOK))
		list := []string{}
		err := json.Unmarshal([]byte(responseData), &list)
		Expect(err).NotTo(HaveOccurred())
		Expect(len(list)).To(Equal(4))
		Expect(list).To(ConsistOf("key1", "key2", "key3", "key4"))
	})

	It("should be possible to delete data", func() {
		content := "foobar"
		put(handler, "/key1", content)
		code, _ := del(handler, "/key1")
		Expect(code).To(Equal(http.StatusOK))
		code, _ = get(handler, "/key1")
		Expect(code).To(Equal(http.StatusNotFound))
	})

	It("should be possible to patch json data fields", func() {
		content := `{"a":1,"b":2}`
		put(handler, "/key1", content)
		patchContent := `{"c":3,"a":4}`
		code, _ := patch(handler, "/key1", patchContent)
		Expect(code).To(Equal(http.StatusOK))
		code, resp := get(handler, "/key1")
		Expect(code).To(Equal(http.StatusOK))
		obj := make(map[string]int)
		Expect(json.Unmarshal([]byte(resp), &obj)).To(Succeed())
		Expect(len(obj)).To(Equal(3))
		Expect(obj).To(HaveKeyWithValue("a", 4))
		Expect(obj).To(HaveKeyWithValue("b", 2))
		Expect(obj).To(HaveKeyWithValue("c", 3))
	})

	It("should return 404 for something wrong", func() {
		req, _ := http.NewRequest("GET", "/wrong", nil)
		handler.ServeHTTP(recorder, req)
		Expect(recorder.Code).To(Equal(http.StatusNotFound))
	})
})
