package sereno

import (
	"fmt"
	"time"

	etcdclient "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

func NewWaitGroup(ctx context.Context, wgid string, kapi etcdclient.KeysAPI) (*DistributedWaitGroup, error) {
	wgcntr := "/sereno/d/waitgroup-counter/" + wgid
	return NewDistributedWaitGroup(ctx, DefaultTTL, wgcntr, kapi)
}

func NewDistributedWaitGroup(ctx context.Context, ttl time.Duration, wgkeyid string, kapi etcdclient.KeysAPI) (*DistributedWaitGroup, error) {
	wgcntr := wgkeyid + "/counter"

	c, err := NewDistributedCounter(ctx, wgcntr, ttl, kapi)
	if err != nil {
		return nil, err
	}
	//TODO add distributed locks to prevent anyone calling Add(N) after the first actor has called
	//     Wait().  No guards for that right now.
	return &DistributedWaitGroup{
		waitcounter: c,
	}, nil
}

type DistributedWaitGroup struct {
	waitcounter *DistributedCounter
}

//Add adds an expected number of works to wait on.
func (wg *DistributedWaitGroup) Add(delta int) error {
	return wg.waitcounter.Inc(delta)
}

//Done signals the parent that this workers has finished.
func (wg *DistributedWaitGroup) Done() error {
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
	out, err := wg.waitcounter.Watch()
	if err != nil {
		return err
	}

	for {
		i, ok := <-out
		if !ok {
			return fmt.Errorf("underlying watch channel closed.  unexpected ???")
		}
		if i.Cnt == 0 {
			return nil
		}
		if i.Cnt < 0 {
			return fmt.Errorf("Calling Done() has lead to the wait count going negative.  Did you forget to call Add()?")
		}
	}
}

// WgCount is a helper function to extract the number of workers this waitgroup is currently waiting on.
// useful for tests that aren't exiting.
func WgCount(wg *DistributedWaitGroup) (int, error) {
	return wg.waitcounter.Val()
}
