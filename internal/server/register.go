package server

import (
	"context"
	"net/http"
	"net/url"
	"path"
	"strings"

	kratoshttp "github.com/go-kratos/kratos/v3/transport/http"
)

type proxyURLKey struct{}

// registerHTTP 将网关请求接入共享中间件，并保留原始转发路径和流式响应。
func registerHTTP(srv *kratoshttp.Server, gateway *Server) {
	srv.Route("/").Handle("*", "/{path:(?s:.*)}", func(c kratoshttp.Context) error {
		if originalURL, ok := c.Value(proxyURLKey{}).(*url.URL); ok {
			c.Request().URL = originalURL
		}
		var streamAborted bool
		_, handleErr := c.Middleware(func(ctx context.Context, _ any) (any, error) {
			// 让共享 Recovery 处理业务 panic，流中断仍由 net/http 关闭连接。
			defer func() {
				if reason := recover(); reason != nil {
					if reason != http.ErrAbortHandler {
						panic(reason)
					}
					streamAborted = true
				}
			}()
			// 保留原始 ResponseWriter，供 SSE Flush、WebSocket Hijack 和反向代理状态码使用。
			gateway.ServeHTTP(c.Response(), c.Request().WithContext(ctx))
			return nil, nil
		})(c, nil)
		if streamAborted {
			panic(http.ErrAbortHandler)
		}
		return handleErr
	})
	handler := srv.Handler
	srv.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPath := r.URL.Path
		// 调试路由及其访问限制由 Kit 处理。
		if !strings.HasPrefix(requestPath, "/debug/") &&
			(strings.HasSuffix(requestPath, "/") || requestPath != path.Clean(requestPath)) {
			// 仅替换会触发 Kratos 路由重定向的路径段，保留 Kit 识别流式端点用的前缀。
			var routingPath strings.Builder
			for segment := range strings.SplitSeq(strings.TrimPrefix(requestPath, "/"), "/") {
				if segment == "" || segment == "." || segment == ".." {
					segment = "_"
				}
				routingPath.WriteByte('/')
				routingPath.WriteString(segment)
			}
			r = r.WithContext(context.WithValue(r.Context(), proxyURLKey{}, r.URL))
			routingURL := *r.URL
			routingURL.Path, routingURL.RawPath = routingPath.String(), ""
			r.URL = &routingURL
		}
		handler.ServeHTTP(w, r)
	})
}
