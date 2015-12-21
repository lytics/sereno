package sereno_test

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lytics/sereno"
	"github.com/lytics/sereno/embeddedetcd"
	"golang.org/x/net/context"
)

func TestSignal(t *testing.T) {
	runtime.GOMAXPROCS(8)
	cluster := embeddedetcd.TestClusterOf1()
	cluster.Launch()
	defer func() {
		t.Log("terminating etcd cluster")
		cluster.Terminate(wipe_data_onterm)
	}()

	testtimeout := NewTestCaseTimeout(t, 60*time.Second, time.Microsecond)
	defer testtimeout.End()

	const msgcount = 1500

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
		defer sub.UnSubscribe()

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

			cnt++
			if cnt == msgcount-1 {
				break
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

func TestSignalWithEctdLoad(t *testing.T) {
	//turn off 
	t.SkipNow()
	
	runtime.GOMAXPROCS(8)

	sereno.UseDebugdlogging = true
	sereno.Usedtracedlogging = true

	cluster := embeddedetcd.TestClusterOf1()
	cluster.Launch()
	defer func() {
		t.Log("terminating etcd cluster")
		cluster.Terminate(wipe_data_onterm)
	}()

	testtimeout := NewTestCaseTimeout(t, 260*time.Second, time.Microsecond)
	defer testtimeout.End()

	const msgcount = 500
	const workers = 5

	ready1barrier := &sync.WaitGroup{}
	ready1barrier.Add(1)

	done := &sync.WaitGroup{}
	done.Add(1)

	kapi := KeyClientFromCluster(cluster)
	ctx := context.Background()
	pub, err := sereno.NewPubSubTopic(ctx, "topic42", kapi)
	AssertT(t, err == nil, "err should be nil, got:%v", err)

	tcnt := int64(0)
	go func() {
		defer done.Done()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		sub, err := sereno.NewPubSubTopic(ctx, "topic42", kapi)
		AssertT(t, err == nil, "err should be nil, got:%v", err)
		subchan, err := sub.Subscribe()
		AssertT(t, err == nil, "err should be nil, got:%v", err)

		ready1barrier.Done()
		ecnt := int64(0)
		cnt := 0
		st := time.Now()
		defer func() {
			secs := time.Now().Sub(st).Seconds()
			rate := float64(cnt) / secs
			t.Logf("Background Subscriber: %v msgs @ rate: %0.0f msg/s", cnt, rate)
		}()

		seen := map[string]bool{}

		for {
			select {
			case <-time.After(25 * time.Second):
				sub.UnSubscribe()
				return
			case msgout, open := <-subchan:
				if !open {
					t.Fatalf("sub chan closed....")
				}
				if msgout.Err != nil {
					err := msgout.Err
					if err == context.Canceled {
						return
					} else if err == context.DeadlineExceeded {
						return
					}
					t.Fatalf("error: %v", msgout.Err)
				}

				if cnt == 10 {
					fmt.Println("sleeping")
					time.Sleep(5 * time.Second)
				}

				if string(msgout.Msg) == "exit" {
					fmt.Println("exit signaled : cnt:", cnt, " ecnt:", ecnt)
					ecnt++
				} else {
					cnt++
					atomic.AddInt64(&tcnt, 1)
					if _, ok := seen[string(msgout.Msg)]; ok {
						t.Logf("the message:%v is a duplicate", string(msgout.Msg))
					}
					seen[string(msgout.Msg)] = true
				}

				if ecnt == workers {
					//return
				}
			}
		}
	}()

	ready1barrier.Wait()

	for i := 0; i < workers; i++ {
		go func() {
			for i := 0; i < msgcount; i++ {
				m := fmt.Sprintf("msgid:%d", i)
				err := pub.Publish([]byte(m))
				AssertT(t, err == nil, "err should be nil, got:%v", err)
			}

			time.Sleep(200 * time.Millisecond)
			err := pub.Publish([]byte("exit"))
			AssertT(t, err == nil, "err should be nil, got:%v", err)
		}()
	}
	done.Wait()
	AssertT(t, msgcount*workers == atomic.LoadInt64(&tcnt), "mis count with results %v != %v", msgcount*4, atomic.LoadInt64(&tcnt))
	t.Log("testing done...")
}
