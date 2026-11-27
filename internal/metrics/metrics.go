// Package metrics 提供 Prometheus 指标定义、采集入口与 /metrics 端点处理器。
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// requestsTotal 网关转发请求计数（维度：路由/方法/状态码）。
var requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gateway_requests_total",
	Help: "网关转发请求总数",
}, []string{"route", "method", "code"})

// requestDuration 网关转发请求耗时直方图（维度：路由）。
var requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "gateway_request_duration_seconds",
	Help:    "网关转发请求耗时（秒）",
	Buckets: prometheus.DefBuckets,
}, []string{"route"})

// Handler 返回 /metrics 端点处理器。
func Handler() http.Handler {
	return promhttp.Handler()
}

// RecordRequest 记录网关请求，将扩展 HTTP 方法合并到 OTHER 标签。
func RecordRequest(routeID, method string, code int, d time.Duration) {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace:
	default:
		method = "OTHER"
	}
	requestsTotal.WithLabelValues(routeID, method, strconv.Itoa(code)).Inc()
	requestDuration.WithLabelValues(routeID).Observe(d.Seconds())
}
