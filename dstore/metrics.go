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
}
