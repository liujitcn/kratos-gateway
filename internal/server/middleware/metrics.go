package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/liujitcn/kratos-gateway/internal/biz/gateway/route"
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

// MetricsHandler 返回 /metrics 端点处理器。
func MetricsHandler() http.Handler {
	return promhttp.Handler()
}

// Metrics 指标采集过滤器：请求结束后读取状态码记录器上报 Prometheus 指标。
type Metrics struct{}

// NewMetrics 创建指标采集过滤器。
func NewMetrics() *Metrics {
	return &Metrics{}
}

// Order 实现 route.Filter。
func (f *Metrics) Order() int { return 300 }

// Do 实现 route.Filter。
func (f *Metrics) Do(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		code := http.StatusOK
		if rec, ok := w.(*statusRecorder); ok {
			code = rec.status
		}
		routeID := ""
		if info := route.ReqInfoFrom(r.Context()); info != nil {
			routeID = info.RouteID
		}
		recordRequest(routeID, r.Method, code, time.Since(start))
	})
}

// recordRequest 记录网关请求，将扩展 HTTP 方法合并到 OTHER 标签。
func recordRequest(routeID, method string, code int, d time.Duration) {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace:
	default:
		method = "OTHER"
	}
	requestsTotal.WithLabelValues(routeID, method, strconv.Itoa(code)).Inc()
	requestDuration.WithLabelValues(routeID).Observe(d.Seconds())
}
