package datadog

import (
	"strings"
	"time"

	"fmt"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/rcrowley/go-metrics"
)

type configFn func(r *Reporter)

// FlushLength determines the number of metrics to be buffered before submitting
// to Datadog.
var FlushLength = 32

// WithAddress sets the UDP address to report datadog metrics
func WithAddress(v string) configFn {
	return func(r *Reporter) {
		r.addr = v
	}
}

// WithPrefix sets a Datadog namespace for all metrics
func WithPrefix(v string) configFn {
	return func(r *Reporter) {
		if !strings.HasSuffix(v, ".") {
			v += "."
		}

		r.prefix = v
	}
}

// WithRegistry sets the registry from which metrics should be reported
func WithRegistry(v metrics.Registry) configFn {
	return func(r *Reporter) {
		r.registry = v
	}
}

// WithPercentiles sets the percentiles to use for statistical metrics.
// The default percentiles are 75%, 95%, 99% and 99.9%
func WithPercentiles(v []float64) configFn {
	return func(r *Reporter) {
		r.percentiles = v
	}
}

// Reporter represents a Datadog metrics reporter
type Reporter struct {
	addr        string
	prefix      string
	registry    metrics.Registry
	cn          *statsd.Client
	tags        []string
	percentiles []float64
	p           []string
}

// New creates a new Datadog metrics reporter
func New(options ...configFn) (r *Reporter, err error) {
	r = &Reporter{
		addr:        "127.0.0.1:8125",
		registry:    metrics.DefaultRegistry,
		percentiles: []float64{0.75, 0.95, 0.99, 0.999},
	}

	for _, opt := range options {
		opt(r)
	}

	r.p = make([]string, len(r.percentiles))
	for i, p := range r.percentiles {
		r.p[i] = fmt.Sprintf(".pct-%.2f", p*100.0)
	}

	if FlushLength > 1 {
		r.cn, err = statsd.NewBuffered(r.addr, FlushLength)
	} else {
		r.cn, err = statsd.New(r.addr)
	}
	if err != nil {
		return nil, err
	}
	r.cn.Namespace = r.prefix

	return
}

// FlushWithInterval repeatedly submits a snapshot of metrics to Datadog at an
// interval specified by i
func (r *Reporter) FlushWithInterval(i time.Duration) {
	for range time.Tick(i) {
		r.submit()
	}
}

// Flush submits a snapshot of metrics to Datadog
func (r *Reporter) Flush() error {
	return r.submit()
}

func (r *Reporter) submit() error {
	r.registry.Each(func(name string, i interface{}) {
		switch metric := i.(type) {
		case metrics.Counter:
			r.cn.Count(name, metric.Count(), r.tags, 1)

		case metrics.Gauge:
			r.cn.Gauge(name, float64(metric.Value()), r.tags, 1)

		case metrics.GaugeFloat64:
			r.cn.Gauge(name, metric.Value(), r.tags, 1)

		case metrics.Histogram:
			ms := metric.Snapshot()

			r.cn.Gauge(name+".count", float64(ms.Count()), r.tags, 1)
			r.cn.Gauge(name+".max", float64(ms.Max()), r.tags, 1)
			r.cn.Gauge(name+".min", float64(ms.Min()), r.tags, 1)
			r.cn.Gauge(name+".mean", ms.Mean(), r.tags, 1)
			r.cn.Gauge(name+".stddev", ms.StdDev(), r.tags, 1)
			r.cn.Gauge(name+".var", ms.Variance(), r.tags, 1)

			values := ms.Percentiles(r.percentiles)
			for i, p := range r.p {
				r.cn.Gauge(name+p, values[i], r.tags, 1)
			}

		case metrics.Meter:
			ms := metric.Snapshot()

			r.cn.Gauge(name+".count", float64(ms.Count()), r.tags, 1)
			r.cn.Gauge(name+".rate1", ms.Rate1(), r.tags, 1)
			r.cn.Gauge(name+".rate5", ms.Rate5(), r.tags, 1)
			r.cn.Gauge(name+".rate15", ms.Rate15(), r.tags, 1)
			r.cn.Gauge(name+".mean", ms.RateMean(), r.tags, 1)

		case metrics.Timer:
			ms := metric.Snapshot()
			if ms := metric.Snapshot(); ms == nil {

			}

			r.cn.Gauge(name+".count", float64(ms.Count()), r.tags, 1)
			r.cn.Gauge(name+".max", float64(ms.Max()), r.tags, 1)
			r.cn.Gauge(name+".min", float64(ms.Min()), r.tags, 1)
			r.cn.Gauge(name+".mean", ms.Mean(), r.tags, 1)
			r.cn.Gauge(name+".stddev", ms.StdDev(), r.tags, 1)
			r.cn.Gauge(name+".var", ms.Variance(), r.tags, 1)

			values := ms.Percentiles(r.percentiles)
			for i, p := range r.p {
				r.cn.Gauge(name+p, values[i], r.tags, 1)
			}
		}
	})

	return nil
}