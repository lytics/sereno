
[![Build Status](https://travis-ci.org/lytics/sereno.svg?branch=master)](https://travis-ci.org/lytics/sereno)
[![GoDoc](https://godoc.org/github.com/lytics/sereno?status.svg)](https://godoc.org/github.com/lytics/sereno)

## Why Sereno?

sereno means night watchman in Spanish and since this project is inspired by the curator library for zookeeper, it seemed like a good choice. 

## Inspiration:

Inspired by the recipes in the curator library.  http://curator.apache.org/curator-recipes/index.html


## Sereno's Recipes:

#### Leader Election :

```go
	kapi := client.NewKeysAPI(c) // the etcd client form: https://github.com/coreos/etcd/tree/master/client
	leadership, err := sereno.NewLeaderElection(context.Background(), "leader-topology1", kapi)
	if err != nil {
		log.Fatalf("error:", err)
	}
	leadership.OnElectedLeader(func(t sereno.Topology){
		//Do leader stuff
	})
	leadership.OnElectedFollower(func(t sereno.Topology){
		//Do follower stuff
	})
	//[Optional]
	leadership.OnPeerJoin(func(t sereno.Topology){
		//Do follower stuff
	})
	//[Optional]
	leadership.OnPeerLeave(func(t sereno.Topology){
		//Do follower stuff
	})
```

#### Distributed Counters :

**On server 1**
```go
	kapi := client.NewKeysAPI(c) // the etcd client form: https://github.com/coreos/etcd/tree/master/client
	cntr, err := sereno.NewCounter(context.Background(), "counter001", kapi)
	if err != nil {
		log.Fatalf("error:", err)
	}
	err := cntr.Inc(1)
```

**On server 2**
```go
	kapi := client.NewKeysAPI(c) // the etcd client form: https://github.com/coreos/etcd/tree/master/client
	cntr, err := sereno.NewCounter(context.Background(), "counter001", kapi)
	if err != nil {
		log.Fatalf("error:", err)
	}
	err := cntr.Inc(1)
```

#### Distributed WaitGroup :

A distributed version of golang's WaitGroup.  

Example:

**Parent**

**i.e. waiting for workers to finish.**
```go
	kapi := client.NewKeysAPI(c) // the etcd client form: https://github.com/coreos/etcd/tree/master/client
	dwg, err := sereno.NewWaitGroup(context.Background(), "workgroup0001", kapi)
	if err != nil {
		log.Fatalf("error:", err)
	}
	dwg.Add(5)
	dwg.Wait()
```

**Child**

**i.e. the ones doing the work that the "parent".**
```go
	kapi := client.NewKeysAPI(c) // the etcd client form: https://github.com/coreos/etcd/tree/master/client
	dwg, err := sereno.NewWaitGroup(context.Background(), "workgroup0001", kapi)
	if err != nil {
		log.Fatalf("error:", err)
	}
	// Do some Work.....
	dwg.Done()
```

#### Topic Based PubSub :

This is a topic based pub/sub message bus using etcd.  This solution isn't going to be good for high volume message (see [Kafka8+sarama](https://github.com/Shopify/sarama), [gnatsd](https://github.com/nats-io/gnatsd),etc if you need high throughput message loads).  From my testing this does fine upto about 200 msgs/second.  

So with that caveat why use it? Convenience!   If your already uses this library and you don't' need message throughput it nice to be able to just use it without having to setup one more service and bring in more dependencies.  Im using it to signal my works to begin tasks, and using a Distrusted WaitGroup to signal when they've all finished. 

####### Example:

**Publisher:**

```go
	kapi := client.NewKeysAPI(c) // the etcd client form: https://github.com/coreos/etcd/tree/master/client
	pub, err := sereno.NewPubSubTopic(context.Background(), "topic42", kapi)
	if err != nil {
		log.Fatalf("error:", err)
	}
	err := pub.Publish([]byte("newwork:taskid:123456789"))
	if err != nil {
		log.Fatalf("error:", err)
	}

```

**Subscriber:**

```go
    sub, err := sereno.NewPubSubTopic(context.Background(), "topic42", kapi)
	if err != nil {
		log.Fatalf("error:", err)
	}
	subchan, err := sub.Subscribe()
	if err != nil {
		log.Fatalf("error:", err)
	}
	for msgout := range subchan {
		if msgout.Err != nil {
			err := msgout.Err
			if err == context.Canceled {
				return
			} else if err == context.DeadlineExceeded {
				return
			}
			log.Fatalf("error: %v", msgout.Err)
		}
		log.Println("new message: %v", string(msgout.Msg))
	}
```


#### Node keep alive :

This struct is useful to announcing that this node is still alive.  A common use of this pattern is to refresh an etcd node's ttl every so often (i.e. 30 seconds), so that a collection of actors can be detect when other actors enter or leave the topology.    

This will eventually be a building block for a Leader Election Recipe. 

```go
func main(){
	kapi := client.NewKeysAPI(c) // the etcd client form: https://github.com/coreos/etcd/tree/master/client
	keepalive, err := sereno.NewNodeKeepAlive(context.Background(), "service/api/node0001", 30*time.Second, kapi)
	if err != nil {
		log.Fatalf("error:", err)
	}
	defer keepalive.Stop() //[Optional] just explicitly stops the keepalive, not really needed if your going to exit though...   
	//... 
}
```

#### Convenience wrapper for Time Sortable UUIDs (via SonyFlake).  

```go
	msgid, err := sereno.NextId()
	if err != nil {
		log.Fatalf("error:", err)
	}
	//use that msgid, its a uint64 that is sortable by creation time.  see [SonyFlake](https://github.com/sony/sonyflake)
```
