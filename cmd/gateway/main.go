// kratos-gateway 南北向 API 网关入口：加载配置、装配路由、启动服务与热更新、优雅停机。
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"os/user"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/go-kratos/kratos/v3/registry"
	consulapi "github.com/hashicorp/consul/api"
	kitreg "github.com/liujitcn/kratos-kit/registry"
	kitconsul "github.com/liujitcn/kratos-kit/registry/consul"

	"github.com/liujitcn/kratos-gateway/internal/builder"
	"github.com/liujitcn/kratos-gateway/internal/catalog"
	"github.com/liujitcn/kratos-gateway/internal/config"
	"github.com/liujitcn/kratos-gateway/internal/route"
	"github.com/liujitcn/kratos-gateway/internal/server"
	"github.com/liujitcn/kratos-gateway/internal/upstream"
)

// main 加载配置并装配路由，绑定监听端口后启动网关、热更新和自注册，收到信号后排空连接。
func main() {
	confPath := flag.String("conf", "configs/config.yaml", "配置文件路径")
	flag.Parse()
	logger := newLogger()

	cfg, file, err := config.Load(*confPath)
	if err != nil {
		logger.Error("加载配置失败", "error", err)
		os.Exit(1)
	}

	var disc registry.Discovery
	disc, err = newDiscovery(&cfg.Registry)
	if err != nil {
		logger.Error("创建服务发现失败", "error", err)
		os.Exit(1)
	}

	mgr := upstream.NewManager(disc, cfg.Registry.Type, logger)
	router := route.NewRouter()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if _, err = rebuildRoutes(cfg, nil, router, mgr, logger); err != nil {
		logger.Error("首次装配路由表失败", "error", err)
		os.Exit(1)
	}
	var listener net.Listener
	listener, err = net.Listen("tcp", cfg.Server.Addr)
	if err != nil {
		logger.Error("监听失败", "addr", cfg.Server.Addr, "error", err)
		mgr.Stop()
		os.Exit(1)
	}

	routingCfg := *cfg
	var workers sync.WaitGroup
	// 装配状态由多个 goroutine（catalog 回调 / 配置热更新 / 兜底重试）并发读写，统一加锁
	var (
		stateMu        sync.Mutex
		catalogRoutes  []config.Route
		rebuildPending bool
	)
	// rebuild 串行装配静态配置与服务目录的最新快照。
	rebuild := func() {
		stateMu.Lock()
		defer stateMu.Unlock()
		if ctx.Err() != nil {
			return
		}
		updated, err := rebuildRoutes(&routingCfg, catalogRoutes, router, mgr, logger)
		rebuildPending = !updated
		if err != nil {
			logger.Error("装配路由表失败，保留旧表，将定时重试", "error", err)
		}
	}

	// 定时重试装配失败或仍在等待首次发现快照的路由。
	workers.Go(func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			stateMu.Lock()
			pending := rebuildPending
			stateMu.Unlock()
			if pending {
				rebuild()
			}
		}
	})

	if cfg.Catalog.Enabled {
		workers.Go(func() {
			runCatalog(ctx, cfg, logger, func(routes []config.Route) {
				stateMu.Lock()
				catalogRoutes = routes
				stateMu.Unlock()
				rebuild()
			})
		})
	}

	workers.Go(func() {
		file.WatchRoutes(ctx, logger, func(routes []config.Route) {
			stateMu.Lock()
			routingCfg.Routes = routes
			stateMu.Unlock()
			rebuild()
		})
	})

	srv := server.New(cfg, router, mgr, logger)
	runErr := make(chan error, 1)
	go func() {
		runErr <- srv.Serve(listener)
	}()
	deregister := registerSelf(cfg, listener.Addr().(*net.TCPAddr).Port, logger)

	var serveErr error
	select {
	case serveErr = <-runErr:
	case <-ctx.Done():
	}

	stop()
	if serveErr != nil {
		logger.Error("网关运行失败", "error", serveErr)
	} else {
		logger.Info("收到停机信号，开始排空连接")
	}
	if deregister != nil {
		deregister()
	}
	if err = srv.Stop(); err != nil {
		logger.Error("优雅停机失败", "error", err)
	}
	workers.Wait()
	mgr.Stop()
	if serveErr != nil {
		os.Exit(1)
	}
	logger.Info("网关已退出")
}

// rebuildRoutes 装配候选路由表，待新上游就绪后应用共享熔断配置并发布路由。
func rebuildRoutes(cfg *config.Config, catalogRoutes []config.Route, router *route.Router, mgr *upstream.Manager, logger *slog.Logger) (bool, error) {
	current := router.Routes()
	currentKeys := builder.ReferencedKeys(current)
	routes, err := builder.Build(cfg, mgr, catalogRoutes, current, logger)
	if err != nil {
		mgr.Keep(currentKeys)
		return false, err
	}
	keys := builder.ReferencedKeys(routes)
	if len(current) > 0 && slices.ContainsFunc(keys, func(key string) bool {
		return !slices.Contains(currentKeys, key) && !mgr.Ready(key)
	}) {
		mgr.Keep(append(currentKeys, keys...))
		logger.Info("等待新上游的首次服务发现快照，继续使用旧路由表")
		return false, nil
	}
	for _, r := range routes {
		if target, ok := r.Target.(*upstream.DiscoveryTarget); ok {
			target.ApplyBreakerConfig()
		}
	}
	router.Update(routes)
	mgr.Keep(keys)
	logger.Info("路由表已更新", "routes", len(routes))
	return true, nil
}

// newLogger 创建结构化 JSON 日志（访问日志格式对齐 kit 日志规范）。
func newLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, nil))
}

// newDiscovery 为 Consul 创建健康订阅客户端，其他注册中心沿用 kit 工厂。
func newDiscovery(cfg *config.Registry) (registry.Discovery, error) {
	if cfg.Type != string(kitreg.Consul) {
		return kitreg.NewDiscovery(cfg.KitRegistry())
	}
	if cfg.Consul == nil {
		return nil, fmt.Errorf("缺少 registry.consul 配置")
	}
	client, err := newConsulClient(cfg.Consul, 0)
	if err != nil {
		return nil, err
	}
	return upstream.NewConsulDiscovery(client), nil
}

// runCatalog 启动服务目录聚合循环，仅查询匹配前缀的服务。
func runCatalog(ctx context.Context, cfg *config.Config, logger *slog.Logger, fn func([]config.Route)) {
	client, err := newConsulClient(cfg.Registry.Consul, 10*time.Second)
	if err != nil {
		logger.Error("创建 Consul 客户端失败，服务自描述路由不可用", "error", err)
		return
	}
	prefixes := cfg.Catalog.ServicePrefixes
	c := catalog.New(catalog.NewConsulLister(client, prefixes), prefixes, cfg.Catalog.PollInterval.Std(), logger)
	c.Run(ctx, fn)
}

// registerSelf 按实际监听端口注册网关并返回停机时使用的注销函数。
func registerSelf(cfg *config.Config, port int, logger *slog.Logger) func() {
	if !cfg.Server.Register {
		return nil
	}
	timeout := 5 * time.Second
	if drain := cfg.Server.ShutdownTimeout.Std(); drain > 0 {
		timeout = min(timeout, drain)
	}
	var reg registry.Registrar
	var err error
	if cfg.Registry.Type == string(kitreg.Consul) && cfg.Registry.Consul != nil {
		var client *consulapi.Client
		client, err = newConsulClient(cfg.Registry.Consul, timeout)
		if err == nil {
			reg = kitconsul.New(client, kitconsul.WithHealthCheck(cfg.Registry.Consul.HealthCheck))
		}
	} else {
		reg, err = kitreg.NewRegistrar(cfg.Registry.KitRegistry())
	}
	if err != nil {
		logger.Error("创建注册器失败，跳过网关自注册", "error", err)
		return nil
	}
	if reg == nil {
		logger.Error("缺少注册中心配置，跳过网关自注册")
		return nil
	}
	host := hostname()
	instance := &registry.ServiceInstance{
		ID:        fmt.Sprintf("kratos-gateway-%s-%d", host, port),
		Name:      kitreg.NormalizeForRegistry("kratos-gateway", cfg.Registry.Type),
		Endpoints: []string{"http://" + net.JoinHostPort(host, strconv.Itoa(port))},
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err = reg.Register(ctx, instance); err != nil {
		logger.Error("网关自注册失败", "error", err)
		return nil
	}
	logger.Info("网关已注册", "name", instance.Name, "endpoint", instance.Endpoints[0])
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := reg.Deregister(ctx, instance); err != nil {
			logger.Warn("网关注销失败", "error", err)
		}
	}
}

// newConsulClient 应用 Consul 配置，按调用场景设置 HTTP 期限。
func newConsulClient(cfg *config.Consul, timeout time.Duration) (*consulapi.Client, error) {
	options := consulapi.DefaultConfig()
	if cfg != nil {
		options.Address = cfg.Address
		options.Scheme = cfg.Scheme
		options.Token = cfg.Token
	}
	client, err := consulapi.NewClient(options)
	if err != nil {
		return nil, err
	}
	// kit 的 Consul 注册与注销不传递请求上下文，须由实际 HTTP 客户端限制请求时间。
	options.HttpClient.Timeout = timeout
	return client, nil
}

// hostname 取主机名（取不到时回退当前用户名）。
func hostname() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	if u, err := user.Current(); err == nil {
		return u.Username
	}
	return "gateway"
}
