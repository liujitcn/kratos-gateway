package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/liujitcn/kratos-gateway/internal/config"
	"github.com/liujitcn/kratos-gateway/internal/filter"
	"github.com/liujitcn/kratos-gateway/internal/metrics"
	"github.com/liujitcn/kratos-gateway/internal/route"
	"github.com/liujitcn/kratos-gateway/internal/upstream"
)

// Server 网关 HTTP 服务。
type Server struct {
	// http 底层 HTTP 服务。
	http *http.Server
	// cfg 配置。
	cfg *config.Config
	// manager 上游管理器（/gw-stats 数据源）。
	manager *upstream.Manager
	// router 路由器。
	router *route.Router
	// gateway 全局链包装后的网关处理器。
	gateway http.Handler
	// logger 日志。
	logger *slog.Logger
	// catalogReady 表示已发布服务目录的首个路由快照。
	catalogReady atomic.Bool
	// shuttingDown 表示已开始停机，通知接流方摘除实例。
	shuttingDown atomic.Bool
	// upgradeMu 保护升级请求的登记、排空通知和停机状态。
	upgradeMu    sync.Mutex
	upgrades     map[net.Conn]struct{}
	upgradesDone chan struct{}
	draining     bool
}

// New 组装网关服务：恢复→访问日志→指标 全局链包装路由器，并挂载保留端点。
// 每条客户端连接经 ConnContext 创建粘性会话（target.sticky 路由消费）。
func New(cfg *config.Config, router *route.Router, manager *upstream.Manager, logger *slog.Logger) *Server {
	s := &Server{
		cfg:      cfg,
		manager:  manager,
		router:   router,
		logger:   logger,
		upgrades: make(map[net.Conn]struct{}),
	}
	s.gateway = s.buildChain(router)
	s.http = &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           s,
		ConnContext:       ConnContext,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout.Std(),
		IdleTimeout:       cfg.Server.IdleTimeout.Std(),
		// 禁止设置全局 Read/WriteTimeout：SSE/WS 长连接会被掐断（设计规范，第 8 节）
	}
	return s
}

// ConnContext 记录客户端连接供升级请求排空，并创建连接级粘性会话。
func ConnContext(ctx context.Context, conn net.Conn) context.Context {
	ctx = context.WithValue(ctx, clientConnKey{}, conn)
	return route.WithStickySession(ctx, &route.StickySession{})
}

// Serve 在已绑定的监听器上运行 HTTP 服务，阻塞直至出错或停机。
func (s *Server) Serve(ln net.Listener) error {
	s.logger.Info("网关启动", "addr", ln.Addr().String(), "read_header_timeout", s.cfg.Server.ReadHeaderTimeout.Std().String(), "idle_timeout", s.cfg.Server.IdleTimeout.Std().String())
	if err := s.http.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// MarkCatalogReady 记录首个成功的目录快照已发布到路由表。
func (s *Server) MarkCatalogReady() {
	s.catalogReady.Store(true)
}

// BeginDrain 将就绪探针切为不可用，等待接流方摘除实例。
func (s *Server) BeginDrain() {
	s.shuttingDown.Store(true)
}

// Stop 在同一期限内排空 HTTP 请求和升级连接，超时后强制断开剩余连接。
func (s *Server) Stop() error {
	s.BeginDrain()
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Server.ShutdownTimeout.Std())
	defer cancel()
	err := s.http.Shutdown(ctx)
	s.upgradeMu.Lock()
	s.draining = true
	upgradesDone := s.upgradesDone
	s.upgradeMu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) {
		if upgradesDone == nil {
			return err
		}
		select {
		case <-upgradesDone:
			return err
		case <-ctx.Done():
		}
	}
	s.logger.Warn("停机排空超时，强制关闭剩余连接")
	s.upgradeMu.Lock()
	for conn := range s.upgrades {
		if closeErr := conn.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			s.logger.Warn("关闭升级连接失败", "error", closeErr)
		}
	}
	s.upgradeMu.Unlock()
	closeErr := s.http.Close()
	if errors.Is(closeErr, http.ErrServerClosed) {
		closeErr = nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return closeErr
	}
	return errors.Join(err, closeErr)
}

// ServeHTTP 实现 http.Handler，在转发前登记升级请求，保留端点独立响应。
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/gw-stats":
		s.handleStats(w, r)
	case "/metrics":
		s.handleMetrics(w, r)
	case "/readyz":
		s.handleReadiness(w)
	case "/healthz":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	default:
		if r.Header.Get("Upgrade") != "" {
			if conn, ok := r.Context().Value(clientConnKey{}).(net.Conn); ok {
				if !s.trackUpgrade(conn) {
					http.Error(w, "gateway: shutting down", http.StatusServiceUnavailable)
					return
				}
				defer s.finishUpgrade(conn)
			}
		}
		s.gateway.ServeHTTP(w, r)
	}
}

// handleReadiness 判断实例是否可以接流，并检查已发布发现路由的节点快照。
func (s *Server) handleReadiness(w http.ResponseWriter) {
	if s.shuttingDown.Load() || (s.cfg.Catalog.Enabled && !s.catalogReady.Load()) {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	for _, r := range s.router.Routes() {
		if target, ok := r.Target.(*upstream.DiscoveryTarget); ok && !target.HasDiscoveredNodes() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

// trackUpgrade 在连接脱离 net/http 管理前登记升级请求。
func (s *Server) trackUpgrade(conn net.Conn) bool {
	s.upgradeMu.Lock()
	defer s.upgradeMu.Unlock()
	if s.draining {
		return false
	}
	if s.upgradesDone == nil {
		s.upgradesDone = make(chan struct{})
	}
	s.upgrades[conn] = struct{}{}
	return true
}

// finishUpgrade 释放已结束的升级请求，最后一条连接结束时通知停机流程。
func (s *Server) finishUpgrade(conn net.Conn) {
	s.upgradeMu.Lock()
	defer s.upgradeMu.Unlock()
	delete(s.upgrades, conn)
	if len(s.upgrades) == 0 {
		close(s.upgradesDone)
		s.upgradesDone = nil
	}
}

// clientConnKey 标识请求上下文中的客户端连接。
type clientConnKey struct{}

// handleStats 在显式启用时输出路由表与上游节点命中统计。
func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	if !s.statsEnabled() {
		http.Error(w, "gateway: stats disabled", http.StatusNotFound)
		return
	}
	routes := s.router.Routes()
	routeStats := make([]routeStat, 0, len(routes))
	for _, r := range routes {
		path := ""
		for _, p := range r.Predicates {
			if pp, ok := p.(*route.PathPredicate); ok {
				path = pp.Prefix
				break
			}
		}
		routeStats = append(routeStats, routeStat{ID: r.ID, Path: path, Target: targetDesc(r.Target)})
	}
	resp := statsResponse{
		Routes:    routeStats,
		Upstreams: s.manager.Snapshot(),
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("输出网关状态失败", "error", err)
	}
}

// handleMetrics 输出 Prometheus 指标。
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Server.Metrics != nil && !*s.cfg.Server.Metrics {
		http.Error(w, "gateway: metrics disabled", http.StatusNotFound)
		return
	}
	metrics.Handler().ServeHTTP(w, r)
}

// buildChain 组装全局过滤器链（恢复→访问日志→指标→路由器）。
func (s *Server) buildChain(router *route.Router) http.Handler {
	filters := []route.Filter{
		filter.NewRecovery(s.logger),
		filter.NewAccessLog(s.logger),
		filter.NewMetrics(),
	}
	var handler http.Handler = router
	for i := len(filters) - 1; i >= 0; i-- {
		handler = filters[i].Do(handler)
	}
	return handler
}

// statsEnabled 判断是否显式启用 /gw-stats。
func (s *Server) statsEnabled() bool {
	return s.cfg.Server.Stats != nil && *s.cfg.Server.Stats
}

// targetDesc 生成转发目标的展示描述（/gw-stats 用）。
func targetDesc(target route.Target) string {
	switch t := target.(type) {
	case interface{ Key() string }:
		return t.Key()
	default:
		return "static"
	}
}

// statsResponse /gw-stats 响应结构。
type statsResponse struct {
	// Routes 当前路由表。
	Routes []routeStat `json:"routes"`
	// Upstreams 各上游状态。
	Upstreams []upstream.UpstreamStat `json:"upstreams"`
}

// routeStat 路由展示项。
type routeStat struct {
	// ID 路由 ID。
	ID string `json:"id"`
	// Path 路径段前缀。
	Path string `json:"path"`
	// Target 转发目标描述。
	Target string `json:"target"`
}
