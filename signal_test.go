package sereno_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lytics/sereno"
	"github.com/lytics/sereno/embeddedetcd"
	"golang.org/x/net/context"
)

func TestSignal(t *testing.T) {
	cluster := embeddedetcd.TestClusterOf1()
	cluster.Launch()
	defer func() {
		t.Log("terminating etcd cluster")
		cluster.Terminate(wipe_data_onterm)
	}()

	testtimeout := NewTestCaseTimeout(t, 10*time.Second, time.Microsecond)
	defer testtimeout.End()

	const msgcount = 9

	ready1barrier := &sync.WaitGroup{}
	ready1barrier.Add(1)

	done := &sync.WaitGroup{}
	done.Add(1)

	kapi := KeyClientFromCluster(cluster)
	ctx := context.Background()
	pub, err := sereno.NewPubSubTopic(ctx, "topic42", kapi)
	AssertT(t, err == nil, "err should be nil, got:%v", err)

	go func() {
		defer done.Done()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		sub, err := sereno.NewPubSubTopic(ctx, "topic42", kapi)
		AssertT(t, err == nil, "err should be nil, got:%v", err)
		subchan, err := sub.Subscribe()
		AssertT(t, err == nil, "err should be nil, got:%v", err)

		ready1barrier.Done()

		cnt := 0
		st := time.Now()
		defer func() {
			secs := time.Now().Sub(st).Seconds()
			rate := float64(cnt) / secs
			t.Logf("Background Subscriber: %v msgs @ rate: %0.0f msg/s", cnt, rate)
		}()
		for msgout := range subchan {
			if msgout.Err != nil {
				err := msgout.Err
				if err == context.Canceled {
					return
				} else if err == context.DeadlineExceeded {
					return
				}
				t.Fatalf("error: %v", msgout.Err)
			}
			//t.Logf("msg: %v", string(msgout.Msg))
			cnt++
			if cnt == msgcount {
				return
			}
		}
	}()

	ready1barrier.Wait()
	for i := 0; i < msgcount; i++ {
		m := fmt.Sprintf("msgid:%d", i)
		err := pub.Publish([]byte(m))
		AssertT(t, err == nil, "err should be nil, got:%v", err)
	}

	done.Wait()
	t.Log("testing done...")
}
