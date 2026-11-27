package server

import (
	"fmt"

	kratoshttp "github.com/go-kratos/kratos/v3/transport/http"
	coreserver "github.com/liujitcn/kratos-core/server"
	configv1 "github.com/liujitcn/kratos-kit/api/gen/go/config/v1"
	"github.com/liujitcn/kratos-kit/bootstrap"
	kitserver "github.com/liujitcn/kratos-kit/server/http"
)

// HTTPServer 将共享 HTTP 传输与网关的连接排空生命周期组合起来。
type HTTPServer struct {
	*kratoshttp.Server
	gateway *Server
}

// NewHTTPServer 使用统一的 HTTP 工厂和 Core 中间件链挂载网关。
func NewHTTPServer(ctx *bootstrap.Context, gateway *Server) (*HTTPServer, error) {
	boot := ctx.GetConfig()
	if boot.Server == nil {
		boot.Server = &configv1.Server{}
	}
	if boot.Server.Http == nil {
		boot.Server.Http = &configv1.Server_Http{}
	}
	if boot.Server.Http.Addr == "" {
		boot.Server.Http.Addr = ":8080"
	}
	middlewares := coreserver.NewHTTPMiddleware(ctx, nil, nil, nil, nil, nil, nil, nil)
	srv, err := kitserver.CreateHttpServer(boot, middlewares...)
	if err != nil {
		return nil, fmt.Errorf("创建 HTTP 服务失败: %w", err)
	}
	gateway.http = srv.Server
	srv.ConnContext = ConnContext
	srv.ReadHeaderTimeout = gateway.cfg.Server.ReadHeaderTimeout.Std()
	srv.IdleTimeout = gateway.cfg.Server.IdleTimeout.Std()
	registerHTTP(srv, gateway)
	return &HTTPServer{Server: srv, gateway: gateway}, nil
}

// MarkCatalogReady 标记服务目录的第一份路由表已发布。
func (s *HTTPServer) MarkCatalogReady() {
	s.gateway.MarkCatalogReady()
}

// Stop 按网关配置的时限排空 HTTP 和升级连接。
func (s *HTTPServer) Stop() error {
	return s.gateway.Stop()
}
