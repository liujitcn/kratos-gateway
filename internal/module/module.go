package module

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/liujitcn/kratos-kit/bootstrap"

	"github.com/liujitcn/kratos-gateway/internal/biz/gateway"
	"github.com/liujitcn/kratos-gateway/internal/biz/gateway/route"
	"github.com/liujitcn/kratos-gateway/internal/config"
	"github.com/liujitcn/kratos-gateway/internal/data"
	"github.com/liujitcn/kratos-gateway/internal/data/upstream"
	"github.com/liujitcn/kratos-gateway/internal/server"
)

// Module 持有网关 HTTP 服务及路由更新任务。
type Module struct {
	*server.HTTPServer
	cancel   context.CancelFunc
	workers  sync.WaitGroup
	manager  *upstream.Manager
	stopOnce sync.Once
	stopErr  error
}

// NewModule 初始化静态路由，并启动目录聚合和本地路由更新任务。
func NewModule(ctx *bootstrap.Context, files config.LocalFiles, cfg *config.Config) (*Module, func(), error) {
	boot := ctx.GetConfig()
	reg := boot.GetRegistry()
	disc, err := data.NewDiscovery(reg)
	if err != nil {
		return nil, nil, fmt.Errorf("创建服务发现失败: %w", err)
	}
	if cfg.Catalog.Enabled && reg.GetConsul() == nil {
		return nil, nil, fmt.Errorf("服务目录需要 registry.consul 配置")
	}
	logger := ctx.GetLogger()
	mgr := upstream.NewManager(disc, reg.GetType(), logger)
	router := route.NewRouter()
	if _, err = gateway.RebuildRoutes(cfg, nil, router, mgr, logger); err != nil {
		mgr.Stop()
		return nil, nil, fmt.Errorf("首次装配路由表失败: %w", err)
	}
	var httpServer *server.HTTPServer
	httpServer, err = server.NewHTTPServer(ctx, server.New(cfg, router, mgr, logger))
	if err != nil {
		mgr.Stop()
		return nil, nil, err
	}

	workCtx, cancel := context.WithCancel(ctx.Context())
	m := &Module{HTTPServer: httpServer, cancel: cancel, manager: mgr}
	var (
		mu            sync.Mutex
		catalogRoutes []config.Route
		pending       bool
	)
	// 路由配置与服务目录的更新共用一把锁，发布时始终使用最新的一对快照。
	rebuild := func() {
		if workCtx.Err() != nil {
			return
		}
		updated, rebuildErr := gateway.RebuildRoutes(cfg, catalogRoutes, router, mgr, logger)
		pending = !updated
		if updated && catalogRoutes != nil {
			httpServer.MarkCatalogReady()
		}
		if rebuildErr != nil {
			logger.Error("装配路由表失败，保留旧表，将定时重试", "error", rebuildErr)
		}
	}
	m.workers.Go(func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-workCtx.Done():
				return
			case <-ticker.C:
			}
			mu.Lock()
			if pending {
				rebuild()
			}
			mu.Unlock()
		}
	})
	if cfg.Catalog.Enabled {
		m.workers.Go(func() {
			data.RunCatalog(workCtx, cfg.Catalog, reg.GetConsul(), logger, func(routes []config.Route) {
				mu.Lock()
				defer mu.Unlock()
				catalogRoutes = routes
				rebuild()
			})
		})
	}
	// 远程配置优先于本地文件，避免本地监听覆盖已经加载的远程路由。
	if boot.GetConfig().GetType() == "" {
		m.workers.Go(func() {
			files.WatchRoutes(workCtx, logger, cfg.Routes, func(routes []config.Route) {
				mu.Lock()
				defer mu.Unlock()
				cfg.Routes = routes
				rebuild()
			})
		})
	}
	return m, func() {
		if stopErr := m.Stop(context.Background()); stopErr != nil {
			logger.Error("优雅停机失败", "error", stopErr)
		}
	}, nil
}

// Stop 排空网关连接、停止更新任务并释放上游订阅。
func (m *Module) Stop(_ context.Context) error {
	m.stopOnce.Do(func() {
		m.cancel()
		m.stopErr = m.HTTPServer.Stop()
		m.workers.Wait()
		m.manager.Stop()
	})
	return m.stopErr
}
