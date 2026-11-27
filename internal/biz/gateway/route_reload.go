package gateway

import (
	"log/slog"
	"slices"

	"github.com/liujitcn/kratos-gateway/internal/biz/gateway/route"
	"github.com/liujitcn/kratos-gateway/internal/config"
	"github.com/liujitcn/kratos-gateway/internal/data/upstream"
)

// RebuildRoutes 装配候选路由表，待新上游就绪后应用共享熔断配置并发布路由。
func RebuildRoutes(cfg *config.Config, catalogRoutes []config.Route, router *route.Router, mgr *upstream.Manager, logger *slog.Logger) (bool, error) {
	current := router.Routes()
	currentKeys := ReferencedKeys(current)
	routes, err := Build(cfg, mgr, catalogRoutes, current, logger)
	if err != nil {
		mgr.Keep(currentKeys)
		return false, err
	}
	keys := ReferencedKeys(routes)
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
