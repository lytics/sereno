package sereno

import (
	"fmt"
	"time"

	etcdc "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

func NewWaitGroup(ctx context.Context, wgid string, kapi etcdc.KeysAPI) (*DistributedWaitGroup, error) {
	wgcntr := "/sereno/d/waitgroup-counter/" + wgid
	return NewDistributedWaitGroup(ctx, DefaultTTL, wgcntr, kapi)
}

func NewDistributedWaitGroup(ctx context.Context, ttl time.Duration, wgkeyid string, kapi etcdc.KeysAPI) (*DistributedWaitGroup, error) {
	wgcntr := wgkeyid + "/counter"

	c, err := NewDistributedCounter(ctx, wgcntr, ttl, kapi)
	if err != nil {
		return nil, err
	}

	dlog("new waitgroup for %v, ttl:%v", wgcntr, ttl)

	//TODO add distributed locks to prevent anyone calling Add(N) after the first actor has called
	//     Wait().  No guards for that right now.
	return &DistributedWaitGroup{
		ctx:         ctx,
		wgcntr:      wgcntr,
		kapi:        kapi,
		waitcounter: c,
	}, nil
}

type DistributedWaitGroup struct {
	waitcounter *DistributedCounter
	wgcntr      string
	kapi        etcdc.KeysAPI
	ctx         context.Context
}

//Add adds an expected number of works to wait on.
func (wg *DistributedWaitGroup) Add(delta int) error {
	dlog("add for %v, delta:%v", wg.wgcntr, delta)

	return wg.waitcounter.Inc(delta)
}

//Done signals the parent that this workers has finished.
func (wg *DistributedWaitGroup) Done() error {
	dlog("done call for %v", wg.wgcntr)

	return wg.waitcounter.Dec(1)
}

// Wait will block until all workers have called Done().
//
// WARNING: There aren't currently any guards in place to prevent
// other actors from calling Add() after the parent calls Wait().
// This differs from the behavior of the sync.WaitGroup,
// which doesn't allow you to add workers after calling Wait().
// So be careful...
func (wg *DistributedWaitGroup) Wait() error {
	dlog("wait call for %v", wg.wgcntr)

	waittime := 100     //100 milliseconds
	minwait := 100      //100 milliseconds
	maxwait := 1000 * 4 //4 seconds

	try := 0
	lastval := 0
	for {

		time.Sleep(time.Duration(waittime) * time.Millisecond)

		//slow, but more accurate giving all the issues with watches...
		cnt, err := wg.waitcounter.Val()
		if err != nil {
			try++
			if try == 100 {
				return fmt.Errorf("get key watch error:%v", err)
			}
			backoff(try + 1000)
			continue
		}
		try = 0

		//pply adaptive polling logic
		if cnt != lastval {
			//speed up fast using powers of 2
			waittime = max(minwait, waittime/2)
		} else {
			//slow down slow using addition 
			waittime = min(maxwait, waittime+80)
		}

		lastval = cnt

		if cnt == 0 {
			return nil
		}
		if cnt < 0 {
			return fmt.Errorf("Calling Done() has lead to the wait count going negative.  Did you forget to call Add()?")
		}
	}
}

// WgCount is a helper function to extract the number of workers this waitgroup is currently waiting on.
// useful for tests that aren't exiting.
func WgCount(wg *DistributedWaitGroup) (int, error) {
	return wg.waitcounter.Val()
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func min(x, y int) int {
	if x > y {
		return y
	}
	return x
}
