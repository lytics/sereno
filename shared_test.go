package sereno_test

import (
	"fmt"
	"log"
	"path"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	etcdc "github.com/coreos/etcd/client"
	"github.com/lytics/sereno/embeddedetcd"
)

func KeyClientFromCluster(cluster *embeddedetcd.EtcdCluster) etcdc.KeysAPI {
	peers := cluster.HTTPMembers()[0].ClientURLs

	cfg := etcdc.Config{
		Endpoints: peers,
		Transport: etcdc.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}

	c, err := etcdc.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	return etcdc.NewKeysAPI(c)
}

func AssertT(t *testing.T, eval bool, format string, v ...interface{}) {
	if eval {
		return
	}

	t.Logf("callstack : %v-->%v-->%v", Who(3), Who(2), Who(1))
	if len(v) == 0 {
		t.Fatalf(format)
	} else {
		t.Fatalf(format, v...)
	}
}

func Who(skip int) string {
	pc, _, ln, ok := runtime.Caller(skip + 1)
	if !ok {
		return "unknown"
	}
	funcPc := runtime.FuncForPC(pc)
	if funcPc == nil {
		return "unnamed"
	}

	pathname := funcPc.Name()
	name := path.Base(pathname)
	t := strings.Split(name, ".")
	if len(t) > 1 {
		name = t[len(t)-1]
	}
	return fmt.Sprintf("%v:%v", name, ln)
}

func NewTestCaseTimeout(t *testing.T, timeout, delay time.Duration) *TestcaseTimeout {
	done := make(chan bool)
	callstack := fmt.Sprintf("%v-->%v-->%v", Who(3), Who(2), Who(1))
	go func() {
		time.Sleep(delay)
		select {
		case <-time.After(timeout):
			fmt.Printf("testcase timed out: callstack : %v\n", callstack)
			panic("timeout")
		case <-done:
		}
	}()

	return &TestcaseTimeout{
		done: done,
		once: &sync.Once{},
	}

}

type TestcaseTimeout struct {
	done chan bool
	once *sync.Once
}

func (t *TestcaseTimeout) End() {
	t.once.Do(func() {
		close(t.done)
	})
}
