package sereno_test

import (
	"runtime"
	"testing"
	"time"
)

func TestLeader(t *testing.T) {
	runtime.GOMAXPROCS(8)

	testtimeout := NewTestCaseTimeout(t, 10*time.Second, time.Microsecond)
	defer testtimeout.End()
}
