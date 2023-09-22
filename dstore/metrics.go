package dstore

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	totalReqs *prometheus.CounterVec
	errorReqs *prometheus.CounterVec
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
	
	cmdReqDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gobeansproxy",
			Name: "cmd_req_duration_seconds",
			Help: "cmd req duration",
			Buckets: []float64{0.01, 0.02, 0.03, 0.04, 0.05, 0.07, 0.1, 0.25, 0.5, 1, 2, 5},
		},

		[]string{"cmd", "store"},
	)
	BdbProxyPromRegistry.MustRegister(cmdReqDurationSeconds)

	cmdE2EDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gobeansproxy",
			Name: "cmd_e2e_duration_seconds",
			Help: "cmd e2e duration",
			Buckets: []float64{0.01, 0.02, 0.03, 0.04, 0.05, 0.07, 0.1, 0.25, 0.5, 1, 2, 5},
		},

		[]string{"cmd"},
	)
	BdbProxyPromRegistry.MustRegister(cmdE2EDurationSeconds)
}
