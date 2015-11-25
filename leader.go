package sereno

import (
	etcdc "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

func NewLeaderElection(ctx context.Context, electionname string, kapi etcdc.KeysAPI) (l *Leadership, err error) {
	Log("leader:")
	return nil, nil
}

type Topology struct {
}

type Leadership struct {
	onleader   func(t Topology)
	onfollower func(t Topology)
	onnewpeer  func(t Topology)
	onlostpeer func(t Topology)
}
