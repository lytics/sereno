package sereno_test

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lytics/sereno"
)

func TestIds(t *testing.T) {
	runtime.GOMAXPROCS(8)

	idchan := make(chan uint64, 100000)
	startbarrier := &sync.WaitGroup{}
	startbarrier.Add(1)

	const wrkers = 7
	const ids = 10000
	alldone := &sync.WaitGroup{}
	alldone.Add(wrkers)
	for x := 0; x < wrkers; x++ {
		go func() {
			startbarrier.Wait()
			for i := 0; i < ids; i++ {
				id, err := sereno.NextId()
				AssertT(t, err == nil, "err should be nil, got:%v", err)
				idchan <- id
			}
			alldone.Done()
		}()
	}

	testtimeout := NewTestCaseTimeout(t, 10*time.Second, time.Microsecond)
	defer testtimeout.End()
	startbarrier.Done()

	go func() {
		idmap := make(map[uint64]interface{})
		st := time.Now()
		defer func() {
			secs := time.Now().Sub(st).Seconds()
			rate := float64(len(idmap)) / secs
			t.Logf("we generated %v ids without a collision. rate: %0.0f ids/s", len(idmap), rate)
		}()

		for {
			id := <-idchan
			if _, ok := idmap[id]; ok {
				t.Fatalf("duplicate id found: id:%v", id)
			} else {
				idmap[id] = nil
			}

			if len(idmap) == ids*wrkers {
				return
			}
		}
	}()

	alldone.Wait()
}
