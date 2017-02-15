package embeddedetcd

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/pkg/capnslog"
)

// Cleanup by stopping embedded server and remvoing temp files.
type Cleanup func() error

// StartAndConnect to the embedded server. Return the connected client,
// the configuration of the embedded server, and a cleanup function
// for shutdown.
func StartAndConnect(t *testing.T) (*clientv3.Client, *embed.Config, Cleanup) {
	embedCfg, cleanup := Start(t)

	endpoints := []string{}
	for _, u := range embedCfg.LCUrls {
		endpoints = append(endpoints, u.String())
	}

	cfg := clientv3.Config{
		Endpoints: endpoints,
	}

	etcd, err := clientv3.New(cfg)
	if err != nil {
		t.Fatalf("embeddedetcd: failed creating new etcd client: %v", err)
	}

	return etcd, embedCfg, cleanup
}

// Start the embedded server and return the configuration used to start
// it along with a cleanup function for shutdown.
func Start(t *testing.T) (*embed.Config, Cleanup) {
	cfg := embed.NewConfig()

	// Create temp directory for data.
	dir, err := ioutil.TempDir("", "etcd.testserver.")
	if err != nil {
		t.Fatalf("embeddedetcd: failed creating temp dir: %v", err)
	}
	cfg.Dir = dir

	// Find usable URLs for the configuration.
	err = initConfigURLs(cfg)
	if err != nil {
		t.Fatalf("embeddedetcd: failed to initialize embedded etcd configuration: %v", err)
	}

	// Dumb magic that has to be called after updating the URLs.
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	cfg.Debug = true

	f, err := os.Create(cfg.Dir + "/" + "etcd.log")
	if err != nil {
		t.Fatalf("embeddedetcd: failed creating log file: %v", err)
	}
	capnslog.SetFormatter(capnslog.NewPrettyFormatter(f, cfg.Debug))

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("embeddedetcd: failed call to start etcd embed: %v", err)
	}

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(60 * time.Second):
		e.Server.Stop()
		t.Fatal("embeddedetcd: failed to get ready notify")
	}

	select {
	case err := <-e.Err():
		t.Fatalf("embeddedetcd: failed post startup: %v", err)
	default:
	}

	cleanup := func() error {
		e.Close()
		select {
		case err := <-e.Err():
			if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("failed stopping server: %v", err)
			}
		default:
		}
		return os.RemoveAll(cfg.Dir)
	}

	return cfg, cleanup
}

// initConfigURLs sets the following fields in the config:
//
//      LPUrls
//      LCUrls
//      APUrls
//      ACUrls
//
func initConfigURLs(cfg *embed.Config) error {
	l1, _ := net.Listen("tcp", ":0")
	defer l1.Close()
	p1 := l1.Addr().(*net.TCPAddr).Port

	l2, _ := net.Listen("tcp", ":0")
	defer l2.Close()
	p2 := l2.Addr().(*net.TCPAddr).Port

	lh1 := fmt.Sprintf("http://localhost:%d", p1)
	lh2 := fmt.Sprintf("http://localhost:%d", p2)
	localUrl1, err := url.Parse(lh1)
	if err != nil {
		return err
	}
	localUrl2, err := url.Parse(lh2)
	if err != nil {
		return err
	}

	cfg.LPUrls = []url.URL{*localUrl1}
	cfg.LCUrls = []url.URL{*localUrl2}

	ip, err := getLocalIP()
	if err != nil {
		return err
	}
	ah1 := fmt.Sprintf("http://%s:%d", ip, p1)
	ah2 := fmt.Sprintf("http://%s:%d", ip, p2)
	adUrl1, _ := url.Parse(ah1)
	adUrl2, _ := url.Parse(ah2)

	cfg.APUrls = []url.URL{*adUrl1}
	cfg.ACUrls = []url.URL{*adUrl2}

	return nil
}

// getLocalIP returns the non loopback local IP of the host, or
// an error if no such network IP exists.
func getLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", fmt.Errorf("failed to find network address")
}
