package sereno

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	etcdc "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

var CASErrorOutOfRetries error = fmt.Errorf("error trying to do a compare and swap of a value.  out of retries.")

var DefaultTTL time.Duration = 24 * time.Hour

var dice *rand.Rand = rand.New(rand.NewSource(int64(time.Now().Nanosecond())))

func CompareAndSwapUntil(ctx context.Context, tries int, keyid string, kapi etcdc.KeysAPI,
	evaluator func(res *etcdc.Response, setOpts *etcdc.SetOptions) (val string, err error),
) error {
	//uncomment for debugging..
	//id := dice.Int63()

	for i := 0; i < tries; i++ {
		resp, err := kapi.Get(ctx, keyid, &etcdc.GetOptions{Quorum: true})
		if err != nil {
			return err
		}

		opt := &etcdc.SetOptions{PrevIndex: resp.Index}
		nv, err := evaluator(resp, opt)
		if err != nil {
			return err
		}

		//uncomment for debugging..
		//fmt.Println("before:", id, "\tnewval:", nv, " try:", i)
		_, err = kapi.Set(ctx, keyid, nv, opt)
		if err == nil {
			return nil
		} else if !IsCompareAndSwapFailure(err) {
			return err
		}

		//uncomment for debugging..
		//fmt.Println("after :", id, "\tnewval:", nv, " try:", i, " error:", err)

		backoff(i)
	}

	return CASErrorOutOfRetries
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
	r := dice.Int31n(int32(nf))
	d := time.Duration(r) * time.Microsecond
	time.Sleep(d)
}
