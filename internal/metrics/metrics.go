package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// RequestsTotal tracks the number of requests, categorized by algorithm and whether they were allowed.
	RequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rate_limit_requests_total",
			Help: "Total number of rate limit check requests.",
		},
		[]string{"algorithm", "allowed"},
	)

	// DecisionLatency tracks how long the rate limit decision takes.
	DecisionLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "rate_limit_decision_latency_seconds",
			Help:    "Latency of rate limit decision in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"algorithm"},
	)

	// RedisErrorsTotal tracks failures when communicating with Redis.
	RedisErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rate_limit_redis_errors_total",
			Help: "Total number of errors when communicating with Redis.",
		},
		[]string{"algorithm"},
	)
)

// Init registers all metrics with Prometheus.
func Init() {
	prometheus.MustRegister(RequestsTotal)
	prometheus.MustRegister(DecisionLatency)
	prometheus.MustRegister(RedisErrorsTotal)
}
