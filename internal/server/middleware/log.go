package middleware

import (
	"bufio"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/liujitcn/kratos-gateway/internal/biz/gateway/route"
)

// AccessLog 访问日志过滤器：记录 method path -> route upstream 状态 耗时（对齐 kit 结构化日志规范）。
type AccessLog struct {
	// logger 日志。
	logger *slog.Logger
}

// NewAccessLog 创建访问日志过滤器。
func NewAccessLog(logger *slog.Logger) *AccessLog {
	return &AccessLog{logger: logger}
}

// Order 实现 route.Filter：先于指标包装（状态码记录器供内层复用）。
func (f *AccessLog) Order() int { return 200 }

// Do 实现 route.Filter：创建请求观测信息并包装状态码记录器。
func (f *AccessLog) Do(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		info := &route.ReqInfo{}
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r.WithContext(route.WithReqInfo(r.Context(), info)))
		f.logger.Info("access",
			"method", r.Method,
			"path", r.URL.Path,
			"route", info.RouteID,
			"upstream", info.Node,
			"status", rec.status,
			"duration", time.Since(start).String(),
		)
	})
}

// statusRecorder 状态码记录响应包装器（透传 Flush/Hijack 以兼容 SSE/WebSocket）。
type statusRecorder struct {
	http.ResponseWriter
	status int
}

// WriteHeader 记录状态码后透传。
func (w *statusRecorder) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Flush 透传 Flush（SSE 流式响应）。
func (w *statusRecorder) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack 透传 Hijack（WebSocket 升级）。
func (w *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("上游 ResponseWriter 不支持 Hijack")
	}
	return h.Hijack()
}
