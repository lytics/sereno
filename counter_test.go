package sereno_test

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lytics/sereno"
	"github.com/lytics/sereno/embeddedetcd"
	"golang.org/x/net/context"
)

const wipe_data_onterm = true

func TestCounterInc(t *testing.T) {
	runtime.GOMAXPROCS(8)
	cluster := embeddedetcd.TestClusterOf1()
	cluster.Launch()
	defer cluster.Terminate(wipe_data_onterm)

	startbarrier := &sync.WaitGroup{}
	startbarrier.Add(1)

	const wrkers = 3
	const wrkersInc = 100
	alldone := &sync.WaitGroup{}
	alldone.Add(wrkers)
	for x := 0; x < wrkers; x++ {
		go func() {
			startbarrier.Wait()
			kapi := KeyClientFromCluster(cluster)
			ctx := context.Background()
			cntr, err := sereno.NewCounter(ctx, "counter001", kapi)
			AssertT(t, err == nil, "err should be nil, got:%v", err)

			for i := 0; i < wrkersInc; i++ {
				err := cntr.Inc(1)
				AssertT(t, err == nil, "err should be nil, got:%v", err)
			}
			alldone.Done()
		}()
	}

	kapi := KeyClientFromCluster(cluster)
	ctx := context.Background()
	mycntr, err := sereno.NewCounter(ctx, "counter001", kapi)
	AssertT(t, err == nil, "err should be nil, got:%v", err)

	testtimeout := NewTestCaseTimeout(t, 10*time.Second, time.Microsecond)
	defer testtimeout.End()
	startbarrier.Done()

	alldone.Wait()

	cnt, err := mycntr.Val()
	AssertT(t, err == nil, "err should be nil, got:%v", err)
	AssertT(t, cnt == wrkers*wrkersInc, "expected %v counts, got:%v", wrkers*wrkersInc, cnt)
}

func TestCounterDec(t *testing.T) {
	runtime.GOMAXPROCS(8)
	cluster := embeddedetcd.TestClusterOf1()
	cluster.Launch()
	defer cluster.Terminate(wipe_data_onterm)

	const wrkers = 3
	const wrkersInc = 100

	startbarrier := &sync.WaitGroup{}
	startbarrier.Add(1)

	alldone := &sync.WaitGroup{}
	alldone.Add(wrkers)

	kapi := KeyClientFromCluster(cluster)
	ctx := context.Background()
	mycntr, err := sereno.NewCounter(ctx, "counter002", kapi)
	AssertT(t, err == nil, "err should be nil, got:%v", err)
	err = mycntr.Set(wrkers * wrkersInc)
	AssertT(t, err == nil, "err should be nil, got:%v", err)

	for x := 0; x < wrkers; x++ {
		go func() {
			startbarrier.Wait()
			kapi := KeyClientFromCluster(cluster)
			ctx := context.Background()
			cntr, err := sereno.NewCounter(ctx, "counter002", kapi)
			AssertT(t, err == nil, "err should be nil, got:%v", err)

			for i := 0; i < wrkersInc; i++ {
				err := cntr.Dec(1)
				AssertT(t, err == nil, "err should be nil, got:%v", err)
			}
			alldone.Done()
		}()
	}

	testtimeout := NewTestCaseTimeout(t, 10*time.Second, time.Microsecond)
	defer testtimeout.End()
	startbarrier.Done()

	alldone.Wait()

	cnt, err := mycntr.Val()
	AssertT(t, err == nil, "err should be nil, got:%v", err)
	AssertT(t, cnt == 0, "expected %v counts, got:%v", 0, cnt)
}

func TestCounterWatch(t *testing.T) {
	runtime.GOMAXPROCS(8)
	cluster := embeddedetcd.TestClusterOf1()
	cluster.Launch()
	defer cluster.Terminate(wipe_data_onterm)

	const wrkers = 2
	const wrkersInc = 500

	done := make(chan bool, wrkers+10)
	startbarrier := &sync.WaitGroup{}
	startbarrier.Add(1)

	kapi := KeyClientFromCluster(cluster)
	ctx := context.Background()
	mycntr, err := sereno.NewCounter(ctx, "counter002", kapi)
	AssertT(t, err == nil, "err should be nil, got:%v", err)
	err = mycntr.Set(wrkers * wrkersInc)
	AssertT(t, err == nil, "err should be nil, got:%v", err)

	for x := 0; x < wrkers; x++ {
		go func(z int) {
			startbarrier.Wait()
			kapi := KeyClientFromCluster(cluster)
			ctx := context.Background()
			cntr, err := sereno.NewCounter(ctx, "counter002", kapi)
			AssertT(t, err == nil, "err should be nil, got:%v", err)

			for i := 0; i < wrkersInc; i++ {
				err := cntr.Dec(1)
				AssertT(t, err == nil, "err should be nil, got:%v", err)
			}
			done <- true
		}(x)
	}

	startbarrier.Done()
	func() {
		out, err := mycntr.Watch()
		AssertT(t, err == nil, "err should be nil, got:%v", err)

		expval := wrkers*wrkersInc - 1
		cnt := 0
		zero_hit := false
		for {
			select {
			case <-time.After(60 * time.Second):
				t.Fatalf("testcase timed out")
			case <-done:
				cnt++
				if zero_hit {
					AssertT(t, cnt == wrkers, "expected %v counts, got:%v", wrkers, cnt)
				}
			case i, ok := <-out:
				if !ok {
					t.Fatalf("closed before expecteds. cnt=", cnt)
				}
				if zero_hit {
					AssertT(t, false, "we weren't expecting any more watch values... cnt:%v", cnt)
				}

				AssertT(t, i.Cnt == expval, "expected %v value, got:%v", expval, i.Cnt)
				expval--
				if i.Cnt == 0 {
					zero_hit = true
					return
				}
			}
		}
	}()

	cnt, err := mycntr.Val()
	AssertT(t, err == nil, "err should be nil, got:%v", err)
	AssertT(t, cnt == 0, "expected %v counts, got:%v", 0, cnt)
}
