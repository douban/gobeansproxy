package dstore

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	totalReqs *prometheus.CounterVec
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
	
	cmdReqDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gobeansproxy",
			Name: "cmd_req_duration_seconds",
			Help: "cmd req duration",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},

		[]string{"cmd", "store"},
	)
	BdbProxyPromRegistry.MustRegister(cmdReqDurationSeconds)

	cmdE2EDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gobeansproxy",
			Name: "cmd_e2e_duration_seconds",
			Help: "cmd e2e duration",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},

		[]string{"cmd", "br", "bw", "cr", "cw"},
	)
	BdbProxyPromRegistry.MustRegister(cmdE2EDurationSeconds)
}
