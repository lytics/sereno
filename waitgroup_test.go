package sereno_test

import (
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lytics/sereno"
	"github.com/lytics/sereno/embeddedetcd"
	"golang.org/x/net/context"
)

func TestWorkGroupTest(t *testing.T) {
	runtime.GOMAXPROCS(8)
	const wrkers = 100

	cluster := embeddedetcd.TestClusterOf1()
	cluster.Launch()
	defer cluster.Terminate(wipe_data_onterm)

	startbarrier := &sync.WaitGroup{}
	startbarrier.Add(1)
	activeWork := int64(wrkers)

	kapi := KeyClientFromCluster(cluster)
	ctx := context.Background()
	dwg, err := sereno.NewWaitGroup(ctx, "wg001", kapi)
	AssertT(t, err == nil, "err should be nil, got:%v", err)

	err = dwg.Add(wrkers)
	AssertT(t, err == nil, "err should be nil, got:%v", err)

	for x := 0; x < wrkers; x++ {
		go func() {
			defer func() {
				atomic.AddInt64(&activeWork, -1)

				kapi := KeyClientFromCluster(cluster)
				ctx := context.Background()
				dwg, err := sereno.NewWaitGroup(ctx, "wg001", kapi)
				AssertT(t, err == nil, "err should be nil, got:%v", err)
				dwg.Done()
			}()
			startbarrier.Wait()

			r := rand.Int31n(int32(4096))
			d := time.Duration(r) * time.Millisecond
			time.Sleep(d)
		}()
	}

	done := make(chan bool)
	go func() {
		time.Sleep(150 * time.Millisecond)
		startbarrier.Done()

		select {
		case <-time.After(10 * time.Second):
			c, err := sereno.WgCount(dwg)
			AssertT(t, err == nil, "etestcase timed out: err should be nil, got:%v", err)
			t.Fatalf("testcase timed out: waiting on: %v", c)
		case <-done:
			t.Log("nothing to see here, all done...")
		}
	}()

	err = dwg.Wait()
	AssertT(t, err == nil, "err should be nil, got:%v", err)

	close(done)
	AssertT(t, 0 == atomic.LoadInt64(&activeWork), "active work count wasn't zero, got:%v", atomic.LoadInt64(&activeWork))

	time.Sleep(150 * time.Millisecond)
}
