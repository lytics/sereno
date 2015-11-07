package sereno

import (
	"fmt"
	"strconv"
	"time"

	etcdc "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

func NewCounter(ctx context.Context, cid string, kapi etcdc.KeysAPI) (*DistributedCounter, error) {
	keyid := "sereno/d/counter/" + cid
	return NewDistributedCounter(ctx, keyid, DefaultTTL, kapi)
}

func NewDistributedCounter(ctx context.Context, keyid string, ttl time.Duration, kapi etcdc.KeysAPI) (*DistributedCounter, error) {
	_, err := kapi.Get(ctx, keyid, nil)
	if IsKeyNotFound(err) {
		_, err := kapi.Create(ctx, keyid, "0")
		if err != nil && !IsCompareAndSwapFailure(err) {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	return &DistributedCounter{ctx: ctx, keyid: keyid, kapi: kapi, ttl: ttl, stop: make(chan bool)}, nil
}

type DistributedCounter struct {
	keyid string
	kapi  etcdc.KeysAPI
	ttl   time.Duration
	ctx   context.Context

	stop chan bool
}

func (c *DistributedCounter) Set(val int) error {
	opts := &etcdc.SetOptions{TTL: c.ttl}
	_, err := c.kapi.Set(c.ctx, c.keyid, strconv.Itoa(val), opts)
	return err
}

func (c *DistributedCounter) Inc(n int) error {
	const tries = 256
	evaluator := func(res *etcdc.Response, setOpts *etcdc.SetOptions) (val string, err error) {
		valstr := res.Node.Value
		i, err := strconv.Atoi(valstr)
		if err != nil {
			return "", err
		}
		n := i + n

		setOpts.TTL = c.ttl // update the ttl also

		return strconv.Itoa(n), nil
	}

	return CompareAndSwapUntil(context.Background(), tries, c.keyid, c.kapi, evaluator)
}

func (c *DistributedCounter) Dec(n int) error {
	const tries = 256
	evaluator := func(res *etcdc.Response, setOpts *etcdc.SetOptions) (val string, err error) {
		valstr := res.Node.Value
		i, err := strconv.Atoi(valstr)
		if err != nil {
			return "", err
		}
		n := i - n

		setOpts.TTL = c.ttl // update the ttl also

		return strconv.Itoa(n), nil
	}

	return CompareAndSwapUntil(context.Background(), tries, c.keyid, c.kapi, evaluator)
}

func (c *DistributedCounter) Val() (int, error) {
	k := c.kapi

	resp, err := k.Get(c.ctx, c.keyid, &etcdc.GetOptions{Quorum: true})
	if err != nil {
		return 0, err
	}
	valstr := resp.Node.Value

	return strconv.Atoi(valstr)
}

type CounterUpdate struct {
	Cnt int
	Err error
}

func (c *DistributedCounter) UnWatch() {
	close(c.stop)
}

func (c *DistributedCounter) Watch() (<-chan *CounterUpdate, error) {
	out := make(chan *CounterUpdate)
	k := c.kapi
	c.stop = make(chan bool)

	go func() {
		defer close(out)

		resp, err := k.Get(c.ctx, c.keyid, &etcdc.GetOptions{Quorum: true})
		if err != nil {
			out <- &CounterUpdate{Err: fmt.Errorf("counter: get key watch error:", err)}
			return
		}

		errcnt := 0
		w := k.Watcher(c.keyid, &etcdc.WatcherOptions{AfterIndex: resp.Index, Recursive: false})
		for {
			select {
			case <-c.stop:
				return
			default:
			}
			ctx, can := context.WithTimeout(context.Background(), 2*time.Second)
			res, err := w.Next(ctx)
			can()

			if err != nil {
				if err == context.Canceled {
					out <- &CounterUpdate{Err: err}
					return
				} else if err == context.DeadlineExceeded {
					continue
				} else if cerr, ok := err.(*etcdc.ClusterError); ok {
					errcnt++
					if errcnt == 128 {
						out <- &CounterUpdate{Err: cerr}
						return
					}
					backoff(errcnt)
					continue
				} else {
					out <- &CounterUpdate{Err: err}
					return
				}
			}
			cstr := res.Node.Value
			c, err := strconv.Atoi(cstr)
			if err != nil {
				out <- &CounterUpdate{Err: fmt.Errorf("counter: value Atoi error:", err)}
				return
			}

			out <- &CounterUpdate{Cnt: c}
		}
	}()

	return out, nil
}
