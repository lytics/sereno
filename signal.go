package sereno

import (
	"fmt"
	"strconv"
	"time"

	etcdc "github.com/coreos/etcd/client"
	etcdstore "github.com/coreos/etcd/store"
	"golang.org/x/net/context"
)

type TopicMsg struct {
	Msg []byte
	Err error
}

//NewPubSubTopic uses Etcd as a pub/sub message broker.   Not really the best application for Etcd,
// as the overhead of raft consensus is expensive.  So don't expect to use this to send thousands
// of messages a second.  I've only used it as a low volume message bus, in projects were I wanted to
// limit the number of technologies involved.
func NewPubSubTopic(ctx context.Context, cid string, kapi etcdc.KeysAPI) (*EtcdPubSubTopic, error) {
	keyid := "sereno/d/topic/" + cid
	return NewPubSubTopicByKey(ctx, keyid, DefaultTTL, kapi)
}

func NewPubSubTopicByKey(ctx context.Context, keyid string, ttl time.Duration, kapi etcdc.KeysAPI) (*EtcdPubSubTopic, error) {
	_, err := kapi.Get(ctx, keyid, nil)
	if IsKeyNotFound(err) {
		opt := &etcdc.SetOptions{PrevExist: etcdc.PrevNoExist, TTL: ttl, Dir: true}
		_, err = kapi.Set(ctx, keyid, "", opt)
		if err != nil && !IsCompareAndSwapFailure(err) && !IsNodeExists(err) {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	keepalive, err := NewNodeKeepAlive(ctx, keyid, ttl, kapi)
	if err != nil {
		return nil, err
	}

	Log("signal: new signal(pub/sub) for %v, ttl:%v", keyid, ttl)

	return &EtcdPubSubTopic{
		ctx:       ctx,
		keyid:     keyid,
		kapi:      kapi,
		ttl:       ttl,
		keepalive: keepalive,
		stop:      make(chan bool),
	}, nil
}

type EtcdPubSubTopic struct {
	keyid     string
	kapi      etcdc.KeysAPI
	ctx       context.Context
	ttl       time.Duration
	keepalive *NodeKeepAlive

	stop chan bool
}

func (t *EtcdPubSubTopic) Publish(msg []byte) error {
	mid, err := NextId()
	msgkey := t.keyid + "/" + strconv.FormatUint(mid, 10)

	msgbody := string(msg)

	opt := &etcdc.SetOptions{PrevExist: etcdc.PrevNoExist, Dir: false}
	_, err = t.kapi.Set(t.ctx, msgkey, msgbody, opt)

	return err
}

func (t *EtcdPubSubTopic) UnSubscribe() {
	close(t.stop)
}

func (t *EtcdPubSubTopic) Subscribe() (<-chan *TopicMsg, error) {
	out := make(chan *TopicMsg, 3)
	k := t.kapi
	t.stop = make(chan bool)

	Log("new subscriber for %v", t.keyid)

	go func() {
		defer close(out)

		resp, err := k.Get(t.ctx, t.keyid, &etcdc.GetOptions{Quorum: true})
		if err != nil {
			Log("%v get before watch err %v", t.keyid, err)
			out <- &TopicMsg{Err: fmt.Errorf("pubsubtopic: get key watch error:", err)}
			return
		}

		//TODO -- to drain or not to drain the backlog (current msgs) in the queue?
		//        for now we'll only send new msgs.

		try := 0
		w := k.Watcher(t.keyid, &etcdc.WatcherOptions{AfterIndex: resp.Index, Recursive: true})
		for {

			select {
			case <-t.stop:
				Log("%v watch aborted.", t.keyid)
				return
			default:
			}
			ctx, can := context.WithTimeout(context.Background(), 10*time.Second)
			res, err := w.Next(ctx)
			can()

			if err != nil {
				if err == context.Canceled {
					Trace("%v watch context canceled err %v.  surfacing.", t.keyid, err)
					out <- &TopicMsg{Err: err}
					return
				} else if err == context.DeadlineExceeded {
					continue
				} else if cerr, ok := err.(*etcdc.ClusterError); ok {
					if len(cerr.Errors) > 0 && cerr.Errors[0] == context.DeadlineExceeded {
						continue
					}
					Trace("%v watch cluster err %v.  retrying.", t.keyid, cerr.Detail())
					continue
				} else if err.Error() == etcdc.ErrClusterUnavailable.Error() {
					Trace("%v watch ErrClusterUnavailable err %v.  retrying.", t.keyid, err)
					try++
					backoff(try + 1000)
					continue
				} else {
					Log("%v watch err %v : surfacing.", t.keyid, err)
					out <- &TopicMsg{Err: err}
					return
				}
			} else if res == nil {
				continue
			}

			try = 0

			if res.Action == etcdstore.Create {
				m := []byte(res.Node.Value)
				if len(m) < 512 {
					Trace("%v new msg:[%v]", t.keyid, string(m))
				} else {
					Trace("%v new msg:...", t.keyid)
				}
				out <- &TopicMsg{Msg: m}
			}
		}
	}()
	return out, nil
}
