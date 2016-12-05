package datadog

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/assert"
)

var _ = fmt.Println

const addr = "127.0.0.1:9999"

// testWaitTimeout determines how long to wait for a result. Configured by
// setting the TEST_TIMEOUT environment variable
var testWaitTimeout = 1 * time.Millisecond

func newServer(t *testing.T, c int) chan []byte {
	ch := make(chan []byte, 64)

	cn, err := net.ListenPacket("udp", addr)
	if cn == nil || err != nil {
		t.Fatalf("unable to create connection; %s", err)
	}

	go func() {
		defer cn.Close()



		for ; c > 0; c-- {
			cn.SetReadDeadline(time.Now().Add(testWaitTimeout << 1))
			buf := make([]byte, 128)
			n, _, err := cn.ReadFrom(buf)
			if err != nil {
				t.Fatalf("unable to read data; %s", err)
				return
			}

			ch <- buf[:n]
		}
	}()

	return ch
}

func TestMain(m *testing.M) {
	FlushLength = 1
	t := os.Getenv("TEST_TIMEOUT")
	if d, err := time.ParseDuration(t); err == nil {
		testWaitTimeout = d
	}

	os.Exit(m.Run())
}

func TestNew_WithDefaultOptions(t *testing.T) {
	r, err := New()
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, "127.0.0.1:8125", r.addr)
	assert.Equal(t, metrics.DefaultRegistry, r.registry)
}

func TestNew_WithAddress(t *testing.T) {
	r, _ := New(WithAddress("127.0.0.2:8125"))
	assert.NotNil(t, r)
	assert.Equal(t, "127.0.0.2:8125", r.addr)
}

func TestReporter_FlushCounter(t *testing.T) {
	ch := newServer(t, 1)

	r := metrics.NewRegistry()
	c := metrics.NewRegisteredCounter("foo", r)
	c.Inc(1)

	dd, _ := New(WithAddress(addr), WithRegistry(r))
	dd.Flush()

	select {
	case d := <-ch:
		assert.Equal(t, "foo:1|c", string(d))

	case <-time.After(testWaitTimeout):
		assert.Fail(t, "timeout")
	}
}

func TestReporter_FlushGauge(t *testing.T) {
	ch := newServer(t, 1)

	r := metrics.NewRegistry()
	c := metrics.NewRegisteredGauge("foo", r)
	c.Update(100)

	dd, _ := New(WithAddress(addr), WithRegistry(r))
	dd.Flush()
	select {
	case d := <-ch:
		assert.Equal(t, "foo:100.000000|g", string(d))

	case <-time.After(testWaitTimeout):
		assert.Fail(t, "timeout")
	}
}

func TestReporter_FlushGaugeFloat64(t *testing.T) {
	ch := newServer(t, 1)

	r := metrics.NewRegistry()
	c := metrics.NewRegisteredGaugeFloat64("foo", r)
	c.Update(55.55)

	dd, _ := New(WithAddress(addr), WithRegistry(r))
	dd.Flush()
	select {
	case d := <-ch:
		assert.Equal(t, "foo:55.550000|g", string(d))

	case <-time.After(testWaitTimeout):
		assert.Fail(t, "timeout")
	}
}

func TestReporter_FlushHistogram(t *testing.T) {
	n := 10
	ch := newServer(t, n)

	r := metrics.NewRegistry()
	c := metrics.NewRegisteredHistogram("foo", r, metrics.NewExpDecaySample(4, 1.0))
	c.Update(11)
	c.Update(1)

	dd, _ := New(WithAddress(addr), WithRegistry(r))
	dd.Flush()

	var res []string
	for i := 0; i<n; i++ {
		select {
		case d := <-ch:
		res = append(res, string(d))

		case <-time.After(testWaitTimeout):
			assert.FailNow(t, "timeout")
		}
	}

	assert.Equal(t, "foo.count:2.000000|g", res[0])
	assert.Equal(t, "foo.max:11.000000|g", res[1])
	assert.Equal(t, "foo.min:1.000000|g", res[2])
}