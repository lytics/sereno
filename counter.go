package sereno

import (
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
		if err != nil && !IsCompareAndSwapFailure(err) && !IsNodeExists(err) {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	keepalive, err := NewNodeKeepAlive(keyid, ttl, kapi)
	if err != nil {
		return nil, err
	}

	dlog("new counter:%v ttl:%v", keyid, ttl)

	return &DistributedCounter{
		ctx:       ctx,
		keyid:     keyid,
		kapi:      kapi,
		ttl:       ttl,
		stop:      make(chan bool),
		keepalive: keepalive,
	}, nil
}

type DistributedCounter struct {
	keyid     string
	kapi      etcdc.KeysAPI
	ttl       time.Duration
	ctx       context.Context
	keepalive *NodeKeepAlive

	stop chan bool
}

func (c *DistributedCounter) Set(val int) error {
	dlog("set:%v val:%v", c.keyid, val)

	opts := &etcdc.SetOptions{TTL: c.ttl}
	_, err := c.kapi.Set(c.ctx, c.keyid, strconv.Itoa(val), opts)
	return err
}

func (c *DistributedCounter) Inc(n int) error {
	const tries = 256

	dlog("inc:%v val:%v", c.keyid, n)

	evaluator := func(res *etcdc.Response, setOpts *etcdc.SetOptions) (val string, err error) {
		valstr := res.Node.Value
		i, err := strconv.Atoi(valstr)
		if err != nil {
			return "", err
		}
		n := i + n

		setOpts.TTL = c.ttl // update the ttl also
		setOpts.PrevValue = res.Node.Value

		return strconv.Itoa(n), nil
	}

	return compareAndSwapUntil(context.Background(), tries, c.keyid, c.kapi, evaluator)
}

func (c *DistributedCounter) Dec(n int) error {
	const tries = 256

	dlog("dec:%v val:%v", c.keyid, n)

	evaluator := func(res *etcdc.Response, setOpts *etcdc.SetOptions) (val string, err error) {
		valstr := res.Node.Value
		i, err := strconv.Atoi(valstr)
		if err != nil {
			return "", err
		}
		n := i - n

		setOpts.TTL = c.ttl // update the ttl also
		setOpts.PrevValue = res.Node.Value

		return strconv.Itoa(n), nil
	}

	return compareAndSwapUntil(context.Background(), tries, c.keyid, c.kapi, evaluator)
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
