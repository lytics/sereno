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
