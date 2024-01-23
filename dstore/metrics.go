package dstore

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	totalReqs *prometheus.CounterVec
	errorReqs *prometheus.CounterVec
	rrrStoreReqs *prometheus.CounterVec
	rrrStoreErr *prometheus.CounterVec
	rrrStoreLag *prometheus.GaugeVec
	cmdReqDurationSeconds *prometheus.HistogramVec
	cmdE2EDurationSeconds *prometheus.HistogramVec
	BdbProxyPromRegistry *prometheus.Registry
)

func init() {
	BdbProxyPromRegistry = prometheus.NewRegistry()
	totalReqs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gobeansproxy",
			Name: "total_reqs",
			Help: "total requests counter",
		},

		[]string{"cmd", "store"},
	)
	BdbProxyPromRegistry.MustRegister(totalReqs)

	errorReqs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gobeansproxy",
			Name: "error_reqs",
			Help: "error requests counter",
		},

		[]string{"cmd", "store"},
	)
	BdbProxyPromRegistry.MustRegister(errorReqs)
	
	cmdE2EDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gobeansproxy",
			Name: "cmd_e2e_duration_seconds",
			Help: "cmd e2e duration",
			Buckets: []float64{
				0.001, 0.003, 0.005,
				0.01, 0.03, 0.05, 0.07,
				0.1, 0.3, 0.5, 0.7,
				1, 2, 5,
			},
		},

		[]string{"cmd"},
	)
	BdbProxyPromRegistry.MustRegister(cmdE2EDurationSeconds)

	rrrStoreReqs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gobeansproxy",
			Name: "rrr_store_reqs",
			Help: "read only rr backends req counter",
		},
		[]string{"host"},
	)
	BdbProxyPromRegistry.MustRegister(rrrStoreReqs)
	
	rrrStoreErr = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gobeansproxy",
			Name: "rrr_store_conn_err",
			Help: "store connection error counter",
		},
		[]string{"host", "conn"},
	)
	BdbProxyPromRegistry.MustRegister(rrrStoreErr)

	rrrStoreLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "gobeansproxy",
			Name: "rrr_store_lag_ms",
			Help: "round robin read only sch store lag",
		},
		[]string{"host"},
	)
	BdbProxyPromRegistry.MustRegister(rrrStoreLag)
}
