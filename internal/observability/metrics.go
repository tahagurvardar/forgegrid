package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"sync/atomic"
	"time"
)

// Scraping only reads an immutable cached snapshot, never PostgreSQL.
type snapshot struct{ metrics []prometheus.Metric }
type durable struct{ value atomic.Pointer[snapshot] }

func (d *durable) Describe(ch chan<- *prometheus.Desc) {} // fixed catalog supplied by our database sampler
func (d *durable) Collect(ch chan<- prometheus.Metric) {
	if v := d.value.Load(); v != nil {
		for _, m := range v.metrics {
			ch <- m
		}
	}
}

type Metrics struct {
	Registry         *prometheus.Registry
	Stale            prometheus.Counter
	Assignment       prometheus.Histogram
	WorkerExecutions *prometheus.CounterVec
	WorkerLeaseLoss  prometheus.Counter
	SnapshotOK       prometheus.Gauge
	SnapshotTime     prometheus.Gauge
	durable          durable
}

func NewMetrics() *Metrics {
	m := &Metrics{Registry: prometheus.NewRegistry(), Stale: prometheus.NewCounter(prometheus.CounterOpts{Name: "forgegrid_stale_results_rejected_total", Help: "Rejected stale result messages; process-local."}), Assignment: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "forgegrid_scheduler_assignment_duration_seconds", Help: "Successful assignment transaction duration, including lock wait.", Buckets: prometheus.DefBuckets}), WorkerExecutions: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "forgegrid_worker_executions_total", Help: "Local executor outcomes; not authoritative completion."}, []string{"result"}), WorkerLeaseLoss: prometheus.NewCounter(prometheus.CounterOpts{Name: "forgegrid_worker_lease_guard_expirations_total", Help: "Local lease guard expirations."}), SnapshotOK: prometheus.NewGauge(prometheus.GaugeOpts{Name: "forgegrid_metrics_snapshot_success", Help: "Last read-only database sampling succeeded."}), SnapshotTime: prometheus.NewGauge(prometheus.GaugeOpts{Name: "forgegrid_metrics_snapshot_timestamp_seconds", Help: "Time of last successful database snapshot."})}
	m.Registry.MustRegister(m.Stale, m.Assignment, m.SnapshotOK, m.SnapshotTime, &m.durable)
	return m
}
func NewWorkerMetrics() *Metrics {
	m := NewMetrics()
	m.Registry = prometheus.NewRegistry()
	m.Registry.MustRegister(m.WorkerExecutions, m.WorkerLeaseLoss)
	return m
}
func (m *Metrics) SetSnapshot(metrics []prometheus.Metric) {
	m.durable.value.Store(&snapshot{metrics})
	m.SnapshotOK.Set(1)
	m.SnapshotTime.Set(float64(time.Now().Unix()))
}
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Timeout: time.Second, MaxRequestsInFlight: 2})
}
func Result(value string) string {
	switch value {
	case "SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED", "LOST":
		return value
	default:
		return "OTHER"
	}
}

var DurationBuckets = []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 300}
