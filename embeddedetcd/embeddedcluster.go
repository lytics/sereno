//Copied from https://github.com/coreos/etcd/blob/df7074911e1745a607348f8559470ed195e2ae15/integration/cluster_test.go#L732


package embeddedetcd


import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time" 

	"github.com/coreos/etcd/client"
	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/etcdserver/etcdhttp"
	"github.com/coreos/etcd/pkg/testutil"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/rafthttp"
    "golang.org/x/net/context"
)

const (
	tickDuration   = 10 * time.Millisecond
	clusterName    = "etcd"
	requestTimeout = 20 * time.Second
)

var (
	electionTicks = 10

	// integration test uses well-known ports to listen for each running member,
	// which ensures restarted member could listen on specific port again.
	nextListenPort int64 = 20000
)

func init() {
	// open microsecond-level time log for integration test debugging
	log.SetFlags(log.Ltime | log.Lmicroseconds | log.Lshortfile)
}

func TestClusterOf1()*EtcdCluster { 
	c := NewCluster( 1, false) 
	return c
}
func TestClusterOf3()*EtcdCluster { 
	c := NewCluster( 3, false) 
	return c 
}

type EtcdCluster struct {
	Members []*member
}

// NewCluster returns an unlaunched cluster of the given size which has been
// set to use static bootstrap.
func NewCluster( size int, usePeerTLS bool) *EtcdCluster {
	c := &EtcdCluster{}
	ms := make([]*member, size)
	for i := 0; i < size; i++ {
		ms[i] = mustNewMember( c.Name(i), usePeerTLS)
	}
	c.Members = ms
	if err := fillClusterForMembers(c.Members); err != nil {
		log.Fatal(err)
	}

	return c
}

func (c *EtcdCluster) Launch() {
	errc := make(chan error)
	for _, m := range c.Members {
		// Members are launched in separate goroutines because if they boot
		// using discovery url, they have to wait for others to register to continue.
		go func(m *member) {
			errc <- m.Launch()
		}(m)
	}
	for range c.Members {
		if err := <-errc; err != nil {
			log.Fatalf("error setting up member: %v", err)
		}
	}
	// wait cluster to be stable to receive future client requests
	c.waitMembersMatch( c.HTTPMembers())
	c.waitVersion()
}

func (c *EtcdCluster) URL(i int) string {
	return c.Members[i].ClientURLs[0].String()
}

func (c *EtcdCluster) URLs() []string {
	urls := make([]string, 0)
	for _, m := range c.Members {
		for _, u := range m.ClientURLs {
			urls = append(urls, u.String())
		}
	}
	return urls
}

func (c *EtcdCluster) HTTPMembers() []client.Member {
	ms := make([]client.Member, len(c.Members))
	for i, m := range c.Members {
		scheme := "http"
		if !m.PeerTLSInfo.Empty() {
			scheme = "https"
		}
		ms[i].Name = m.Name
		for _, ln := range m.PeerListeners {
			ms[i].PeerURLs = append(ms[i].PeerURLs, scheme+"://"+ln.Addr().String())
		}
		for _, ln := range m.ClientListeners {
			ms[i].ClientURLs = append(ms[i].ClientURLs, "http://"+ln.Addr().String())
		}
	}
	return ms
}

func (c *EtcdCluster) AddMember() {
	c.addMember( false)
}

func (c *EtcdCluster) AddTLSMember() {
	c.addMember( true)
}

func (c *EtcdCluster) RemoveMember( id uint64) {
	// send remove request to the cluster
	cc := mustNewHTTPClient( c.URLs())
	ma := client.NewMembersAPI(cc)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	if err := ma.Remove(ctx, types.ID(id).String()); err != nil {
		log.Fatalf("unexpected remove error %v", err)
	}
	cancel()
	newMembers := make([]*member, 0)
	for _, m := range c.Members {
		if uint64(m.s.ID()) != id {
			newMembers = append(newMembers, m)
		} else {
			select {
			case <-m.s.StopNotify():
				m.Terminate(false)
			// 1s stop delay + election timeout + 1s disk and network delay + connection write timeout
			// TODO: remove connection write timeout by selecting on http response closeNotifier
			// blocking on https://github.com/golang/go/issues/9524
			case <-time.After(time.Second + time.Duration(electionTicks)*tickDuration + time.Second + rafthttp.ConnWriteTimeout):
				log.Fatalf("failed to remove member %s in time", m.s.ID())
			}
		}
	}
	c.Members = newMembers
	c.waitMembersMatch( c.HTTPMembers())
}

func (c *EtcdCluster) Terminate(wipe_data bool) {
	for _, m := range c.Members {
		m.Terminate(wipe_data)
	}
}

func (c *EtcdCluster) addMember(usePeerTLS bool) {
	m := mustNewMember( c.Name(rand.Int()), usePeerTLS)
	scheme := "http"
	if usePeerTLS {
		scheme = "https"
	}

	// send add request to the cluster
	cc := mustNewHTTPClient( []string{c.URL(0)})
	ma := client.NewMembersAPI(cc)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	peerURL := scheme + "://" + m.PeerListeners[0].Addr().String()
	if _, err := ma.Add(ctx, peerURL); err != nil {
		log.Fatalf("add member on %s error: %v", c.URL(0), err)
	}
	cancel()

	// wait for the add node entry applied in the cluster
	members := append(c.HTTPMembers(), client.Member{PeerURLs: []string{peerURL}, ClientURLs: []string{}})
	c.waitMembersMatch( members)

	m.InitialPeerURLsMap = types.URLsMap{}
	for _, mm := range c.Members {
		m.InitialPeerURLsMap[mm.Name] = mm.PeerURLs
	}
	m.InitialPeerURLsMap[m.Name] = m.PeerURLs
	m.NewCluster = false
	if err := m.Launch(); err != nil {
		log.Fatal(err)
	}
	c.Members = append(c.Members, m)
	// wait cluster to be stable to receive future client requests
	c.waitMembersMatch( c.HTTPMembers())
}

func fillClusterForMembers(ms []*member) error {
	addrs := make([]string, 0)
	for _, m := range ms {
		scheme := "http"
		if !m.PeerTLSInfo.Empty() {
			scheme = "https"
		}
		for _, l := range m.PeerListeners {
			addrs = append(addrs, fmt.Sprintf("%s=%s://%s", m.Name, scheme, l.Addr().String()))
		}
	}
	clusterStr := strings.Join(addrs, ",")
	var err error
	for _, m := range ms {
		m.InitialPeerURLsMap, err = types.NewURLsMap(clusterStr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *EtcdCluster) waitMembersMatch( membs []client.Member) {
	for _, u := range c.URLs() {
		cc := mustNewHTTPClient( []string{u})
		ma := client.NewMembersAPI(cc)
		for {
			ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
			ms, err := ma.List(ctx)
			cancel()
			if err == nil && isMembersEqual(ms, membs) {
				break
			}
			time.Sleep(tickDuration)
		}
	}
	return
}

func (c *EtcdCluster) waitLeader( membs []*member) {
	possibleLead := make(map[uint64]bool)
	var lead uint64
	for _, m := range membs {
		possibleLead[uint64(m.s.ID())] = true
	}

	for lead == 0 || !possibleLead[lead] {
		lead = 0
		for _, m := range membs {
			if lead != 0 && lead != m.s.Lead() {
				lead = 0
				break
			}
			lead = m.s.Lead()
		}
		time.Sleep(10 * tickDuration)
	}
}

func (c *EtcdCluster) waitVersion() {
	for _, m := range c.Members {
		for {
			if m.s.ClusterVersion() != nil {
				break
			}
			time.Sleep(tickDuration)
		}
	}
}

func (c *EtcdCluster) Name(i int) string {
	return fmt.Sprint("node", i)
}

// isMembersEqual checks whether two members equal except ID field.
// The given wmembs should always set ID field to empty string.
func isMembersEqual(membs []client.Member, wmembs []client.Member) bool {
	sort.Sort(SortableMemberSliceByPeerURLs(membs))
	sort.Sort(SortableMemberSliceByPeerURLs(wmembs))
	for i := range membs {
		membs[i].ID = ""
	}
	return reflect.DeepEqual(membs, wmembs)
}

func newLocalListener() net.Listener {
	port := atomic.AddInt64(&nextListenPort, 1)
	l, err := net.Listen("tcp", "127.0.0.1:"+strconv.FormatInt(port, 10))
	if err != nil {
		log.Fatal(err)
	}
	return l
}

func newListenerWithAddr( addr string) net.Listener {
	var err error
	var l net.Listener
	// TODO: we want to reuse a previous closed port immediately.
	// a better way is to set SO_REUSExx instead of doing retry.
	for i := 0; i < 5; i++ {
		l, err = net.Listen("tcp", addr)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		log.Fatal(err)
	}
	return l
}

type member struct {
	etcdserver.ServerConfig
	PeerListeners, ClientListeners []net.Listener
	// inited PeerTLSInfo implies to enable peer TLS
	PeerTLSInfo transport.TLSInfo

	raftHandler *testutil.PauseableHandler
	s           *etcdserver.EtcdServer
	hss         []*httptest.Server
}

// mustNewMember return an inited member with the given name. If usePeerTLS is
// true, it will set PeerTLSInfo and use https scheme to communicate between
// peers.
func mustNewMember(name string, usePeerTLS bool) *member {
	var (
		testTLSInfo = transport.TLSInfo{
			KeyFile:        "./fixtures/server.key.insecure",
			CertFile:       "./fixtures/server.crt",
			TrustedCAFile:  "./fixtures/ca.crt",
			ClientCertAuth: true,
		}
		err error
	)
	m := &member{}

	peerScheme := "http"
	if usePeerTLS {
		peerScheme = "https"
	}

	pln := newLocalListener()
	m.PeerListeners = []net.Listener{pln}
	m.PeerURLs, err = types.NewURLs([]string{peerScheme + "://" + pln.Addr().String()})
	if err != nil {
		log.Fatal(err)
	}
	if usePeerTLS {
		m.PeerTLSInfo = testTLSInfo
	}

	cln := newLocalListener()
	m.ClientListeners = []net.Listener{cln}
	m.ClientURLs, err = types.NewURLs([]string{"http://" + cln.Addr().String()})
	if err != nil {
		//TODO new Logger t.Fatal(err)
	}

	m.Name = name

	m.DataDir, err = ioutil.TempDir(os.TempDir(), "etcd")
	if err != nil {
		log.Fatal(err)
	}
	clusterStr := fmt.Sprintf("%s=%s://%s", name, peerScheme, pln.Addr().String())
	m.InitialPeerURLsMap, err = types.NewURLsMap(clusterStr)
	if err != nil {
		log.Fatal(err)
	}
	m.InitialClusterToken = clusterName
	m.NewCluster = true
	m.ServerConfig.PeerTLSInfo = m.PeerTLSInfo
	m.ElectionTicks = electionTicks
	m.TickMs = uint(tickDuration / time.Millisecond)
	return m
}

// Clone returns a member with the same server configuration. The returned
// member will not set PeerListeners and ClientListeners.
func (m *member) Clone() *member {
	mm := &member{}
	mm.ServerConfig = m.ServerConfig

	var err error
	clientURLStrs := m.ClientURLs.StringSlice()
	mm.ClientURLs, err = types.NewURLs(clientURLStrs)
	if err != nil {
		// this should never fail
		panic(err)
	}
	peerURLStrs := m.PeerURLs.StringSlice()
	mm.PeerURLs, err = types.NewURLs(peerURLStrs)
	if err != nil {
		// this should never fail
		panic(err)
	}
	clusterStr := m.InitialPeerURLsMap.String()
	mm.InitialPeerURLsMap, err = types.NewURLsMap(clusterStr)
	if err != nil {
		// this should never fail
		panic(err)
	}
	mm.InitialClusterToken = m.InitialClusterToken
	mm.ElectionTicks = m.ElectionTicks
	mm.PeerTLSInfo = m.PeerTLSInfo
	return mm
}

// Launch starts a member based on ServerConfig, PeerListeners
// and ClientListeners.
func (m *member) Launch() error {
	var err error
	if m.s, err = etcdserver.NewServer(&m.ServerConfig); err != nil {
		return fmt.Errorf("failed to initialize the etcd server: %v", err)
	}
	m.s.SyncTicker = time.Tick(500 * time.Millisecond)
	m.s.Start()

	m.raftHandler = &testutil.PauseableHandler{Next: etcdhttp.NewPeerHandler(m.s.Cluster(), m.s.RaftHandler())}

	for _, ln := range m.PeerListeners {
		hs := &httptest.Server{
			Listener: ln,
			Config:   &http.Server{Handler: m.raftHandler},
		}
		if m.PeerTLSInfo.Empty() {
			hs.Start()
		} else {
			hs.TLS, err = m.PeerTLSInfo.ServerConfig()
			if err != nil {
				return err
			}
			hs.StartTLS()
		}
		m.hss = append(m.hss, hs)
	}
	for _, ln := range m.ClientListeners {
		hs := &httptest.Server{
			Listener: ln,
			Config:   &http.Server{Handler: etcdhttp.NewClientHandler(m.s, m.ServerConfig.ReqTimeout())},
		}
		hs.Start()
		m.hss = append(m.hss, hs)
	}
	return nil
}

func (m *member) WaitOK() {
	cc := mustNewHTTPClient( []string{m.URL()})
	kapi := client.NewKeysAPI(cc)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		_, err := kapi.Get(ctx, "/", nil)
		if err != nil {
			time.Sleep(tickDuration)
			continue
		}
		cancel()
		break
	}
	for m.s.Leader() == 0 {
		time.Sleep(tickDuration)
	}
}

func (m *member) URL() string { return m.ClientURLs[0].String() }

func (m *member) Pause() {
	m.raftHandler.Pause()
	m.s.PauseSending()
}

func (m *member) Resume() {
	m.raftHandler.Resume()
	m.s.ResumeSending()
}

// Stop stops the member, but the data dir of the member is preserved.
func (m *member) Stop() {
	m.s.Stop()
	for _, hs := range m.hss {
		hs.CloseClientConnections()
		hs.Close()
	}
	m.hss = nil
}

// Start starts the member using the preserved data dir.
func (m *member) Restart() error {
	newPeerListeners := make([]net.Listener, 0)
	for _, ln := range m.PeerListeners {
		newPeerListeners = append(newPeerListeners, newListenerWithAddr(ln.Addr().String()))
	}
	m.PeerListeners = newPeerListeners
	newClientListeners := make([]net.Listener, 0)
	for _, ln := range m.ClientListeners {
		newClientListeners = append(newClientListeners, newListenerWithAddr( ln.Addr().String()))
	}
	m.ClientListeners = newClientListeners
	return m.Launch()
}

// Terminate stops the member and removes the data dir.
func (m *member) Terminate(wipe_data bool) {
	m.s.Stop()
	for _, hs := range m.hss {
		hs.CloseClientConnections()
		hs.Close()
	}
	if err := os.RemoveAll(m.ServerConfig.DataDir); err != nil {
		log.Fatal(err)
	}
}

func mustNewHTTPClient( eps []string) client.Client {
	cfg := client.Config{Transport: mustNewTransport( transport.TLSInfo{}), Endpoints: eps}
	c, err := client.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	return c
}

func mustNewTransport( tlsInfo transport.TLSInfo) *http.Transport {
	// tick in integration test is short, so 1s dial timeout could play well.
	tr, err := transport.NewTimeoutTransport(tlsInfo, time.Second, rafthttp.ConnReadTimeout, rafthttp.ConnWriteTimeout)
	if err != nil {
		log.Fatal(err)
	}
	return tr
}

type SortableMemberSliceByPeerURLs []client.Member

func (p SortableMemberSliceByPeerURLs) Len() int { return len(p) }
func (p SortableMemberSliceByPeerURLs) Less(i, j int) bool {
	return p[i].PeerURLs[0] < p[j].PeerURLs[0]
}
func (p SortableMemberSliceByPeerURLs) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
