package sereno

import (
	"sync"
	"time"

	etcdc "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

func NewNodeKeepAlive(ctx context.Context, nodeid string, nodettl time.Duration, kapi etcdc.KeysAPI) (*NodeKeepAlive, error) {
	done := make(chan bool)
	go func() {
		refreshtime := TTLRefreshDur(nodettl)
		for {
			select {
			case <-time.After(refreshtime):
				const tries = 1024
				evaluator := func(res *etcdc.Response, setOpts *etcdc.SetOptions) (val string, err error) {
					setOpts.TTL = nodettl // update the ttl
					return res.Node.Value, nil
				}
				CompareAndSwapUntil(ctx, tries, nodeid, kapi, evaluator)
			case <-done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return &NodeKeepAlive{
		done: done,
		once: &sync.Once{},
	}, nil

}

type NodeKeepAlive struct {
	done chan bool
	once *sync.Once
}

func (k *NodeKeepAlive) Stop() {
	k.once.Do(func() {
		close(k.done)
	})
}

func TTLRefreshDur(nodettl time.Duration) time.Duration {
	refreshtime := time.Hour
	switch {
	case nodettl >= time.Hour:
		refreshtime = time.Hour - 5*time.Minute
	case nodettl >= 10*time.Minute:
		refreshtime = 9 * time.Minute
	case nodettl >= 5*time.Minute:
		refreshtime = 4 * time.Minute
	case nodettl >= time.Minute:
		refreshtime = 40 * time.Second
	case nodettl >= 30*time.Second:
		refreshtime = 15 * time.Second
	case nodettl >= 10*time.Second:
		refreshtime = 5 * time.Second
	case nodettl >= 5*time.Second:
		refreshtime = 2 * time.Second
	case nodettl >= time.Second:
		refreshtime = 500 * time.Millisecond
	default:
		//really??? this is just crazy fast and I hope we never see this case...
		refreshtime = 250 * time.Millisecond
	}
	return refreshtime
}
