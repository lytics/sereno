package sereno

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	etcdc "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

var CASErrorOutOfRetries error = fmt.Errorf("error trying to do a compare and swap of a value.  out of retries.")
var DefaultTTL time.Duration = 24 * time.Hour

func dice() *rand.Rand {
	return rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
}

func compareAndSwapUntil(ctx context.Context, tries int, keyid string, kapi etcdc.KeysAPI,
	evaluator func(res *etcdc.Response, setOpts *etcdc.SetOptions) (val string, err error),
) error {
	//uncomment for debugging..
	id := int64(0)
	if Usedtracedlogging {
		id = dice().Int63()
	}
	for i := 0; i < tries; i++ {
		resp, err := kapi.Get(ctx, keyid, &etcdc.GetOptions{Quorum: true})
		if err != nil {
			dlog("%v kapi get error %v", keyid, err)
			return err
		}

		opt := &etcdc.SetOptions{}
		nv, err := evaluator(resp, opt)
		if err != nil {
			dlog("%v eval error %v", keyid, err)
			return err
		}

		dtrace("before: %v \tnewval:%v try:%v idx:%v key:%v", id, nv, i, resp.Index, keyid)
		_, err = kapi.Set(ctx, keyid, nv, opt)
		if err == nil {
			dlog("%v update successful %v", keyid, err)
			return nil
		} else if !IsCompareAndSwapFailure(err) {
			dlog("unexpected error %v", err)
			return err
		}

		dtrace("after : %v \tnewval:%v try:%v key:%v error: %v", id, nv, i, keyid, err)

		backoff(i)
	}

	return CASErrorOutOfRetries
}

func IsNodeExists(err error) bool {
	if err != nil {
		if errEtcd, ok := err.(etcdc.Error); ok {
			if errEtcd.Code == etcdc.ErrorCodeNodeExist {
				return true
			}
		}
	}
	return false
}

func IsKeyNotFound(err error) bool {
	if err != nil {
		if errEtcd, ok := err.(etcdc.Error); ok {
			if errEtcd.Code == etcdc.ErrorCodeKeyNotFound {
				return true
			}
		}
	}
	return false
}

func IsCompareAndSwapFailure(err error) bool {
	if err != nil {
		if errEtcd, ok := err.(etcdc.Error); ok {
			if errEtcd.Code == etcdc.ErrorCodeTestFailed {
				return true
			}
		}
	}
	return false
}

//backoff sleeps a random amount so we can.
//http://play.golang.org/p/l9aUHgiR8J
func backoff(try int) {
	nf := math.Pow(4, float64(try))
	nf = math.Max(1000, nf)
	nf = math.Min(nf, 2000000)
	r := dice().Int31n(int32(nf))
	d := time.Duration(r) * time.Microsecond
	time.Sleep(d)
}

//
// LOGGING
//
var UseDebugdlogging = false
var Usedtracedlogging = false

var std = log.New(os.StdErr, "           ", log.Ltime|log.Lmicroseconds|log.Lshortfile)

func dlog(format string, v ...interface{}) {
	if UseDebugdlogging {
		std.Output(2, fmt.Sprintf(format, v...))
	}
}

func dtrace(format string, v ...interface{}) {
	if Usedtracedlogging {
		std.Output(2, fmt.Sprintf(format, v...))
	}
}
