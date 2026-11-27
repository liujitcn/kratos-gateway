// Package builder 负责把配置装配为运行时路由表：谓词、过滤器、转发目标与处理器链。
// 静态路由在前、catalog 聚合路由在后（同深度前缀重叠时静态路由优先）。
package builder

import (
	"fmt"
	"log/slog"
	"net/http"
	"slices"

	"github.com/go-kratos/kratos/v3/selector"

	"github.com/liujitcn/kratos-gateway/internal/catalog"
	"github.com/liujitcn/kratos-gateway/internal/config"
	"github.com/liujitcn/kratos-gateway/internal/forward"
	"github.com/liujitcn/kratos-gateway/internal/route"
	"github.com/liujitcn/kratos-gateway/internal/upstream"
)

// Build 装配静态与目录路由，复用当前路由中参数相同的限流桶。
func Build(cfg *config.Config, mgr *upstream.Manager, catalogRoutes []config.Route, current []*route.Route, logger *slog.Logger) ([]*route.Route, error) {
	specs := make([]config.Route, 0, len(cfg.Routes)+len(catalogRoutes))
	specs = append(specs, cfg.Routes...)
	specs = append(specs, catalogRoutes...)
	previousFilters := make(map[string][]route.Filter, len(current))
	for _, r := range current {
		if _, duplicate := previousFilters[r.ID]; duplicate {
			previousFilters[r.ID] = nil
		} else {
			previousFilters[r.ID] = r.Filters
		}
	}
	counts := make(map[string]int, len(specs))
	targets := make([]upstream.UpstreamConfig, len(specs))
	breakers := make(map[string]*upstream.BreakerConfig)
	for i, spec := range specs {
		counts[spec.ID]++
		target := discoveryConfig(spec.Target)
		targets[i] = target
		if target.Breaker == nil || len(spec.Target.URLs) > 0 || target.Service == "" {
			continue
		}
		key := target.Key()
		if prior := breakers[key]; prior != nil && *prior != *target.Breaker {
			return nil, fmt.Errorf("路由 %q 的熔断参数与同一上游池 %q 的其他路由不一致", spec.ID, key)
		}
		breakers[key] = target.Breaker
	}
	routes := make([]*route.Route, 0, len(specs))
	var err error
	for i, spec := range specs {
		var oldFilters []route.Filter
		if counts[spec.ID] == 1 {
			oldFilters = previousFilters[spec.ID]
		}
		target := targets[i]
		target.Breaker = breakers[target.Key()]
		var r *route.Route
		r, err = buildRoute(spec, target, i >= len(cfg.Routes), oldFilters, mgr, logger)
		if err != nil {
			return nil, fmt.Errorf("装配路由 %q: %w", spec.ID, err)
		}
		routes = append(routes, r)
	}
	if err = assignWeightRanges(routes); err != nil {
		return nil, err
	}
	return routes, nil
}

// ReferencedKeys 计算路由表引用的服务发现上游键集合（热更新回收孤立 Watch 用）。
func ReferencedKeys(routes []*route.Route) []string {
	keys := make([]string, 0, len(routes))
	for _, r := range routes {
		if dt, ok := r.Target.(*upstream.DiscoveryTarget); ok {
			keys = append(keys, dt.Key())
		}
	}
	return keys
}

// buildRoute 校验单条路由并装配过滤器、目标和转发处理器链。
func buildRoute(spec config.Route, targetCfg upstream.UpstreamConfig, catalogRoute bool, previousFilters []route.Filter, mgr *upstream.Manager, logger *slog.Logger) (*route.Route, error) {
	if spec.ID == "" {
		return nil, fmt.Errorf("缺少 id")
	}
	maxRetry := 1
	if spec.Target.MaxRetry != nil {
		maxRetry = *spec.Target.MaxRetry
		if maxRetry < 0 {
			return nil, fmt.Errorf("max_retry 不能小于 0")
		}
	}
	predicates, err := route.BuildPredicates(spec.Predicates)
	if err != nil {
		return nil, err
	}
	if len(predicates) == 0 {
		return nil, fmt.Errorf("至少需要一项谓词")
	}
	var filters []route.Filter
	filters, err = route.BuildFilters(spec.Filters, previousFilters)
	if err != nil {
		return nil, err
	}
	var matches func(selector.Node) bool
	if catalogRoute {
		matches = catalog.RouteNodeMatcher(spec.Predicates[0].Path)
	}
	var target route.Target
	target, err = buildTarget(spec.Target.URLs, targetCfg, mgr, matches)
	if err != nil {
		return nil, err
	}
	var handler http.Handler = forward.NewForwardHandler(target, maxRetry, spec.Target.Sticky, route.UpgradeResponseHeaders(filters), logger)
	for _, filter := range slices.Backward(filters) {
		handler = filter.Do(handler)
	}
	return &route.Route{
		ID:         spec.ID,
		Predicates: predicates,
		Filters:    filters,
		Target:     target,
		Handler:    handler,
	}, nil
}

// buildTarget 构造转发目标，静态地址优先，否则经管理器复用服务发现上游。
func buildTarget(urls []string, cfg upstream.UpstreamConfig, mgr *upstream.Manager, matches func(selector.Node) bool) (route.Target, error) {
	if len(urls) > 0 {
		return upstream.NewStaticTarget(urls, cfg.LB)
	}
	if cfg.Service == "" {
		return nil, fmt.Errorf("target 需要配置 service 或 urls")
	}
	u, err := mgr.Get(cfg)
	if err != nil {
		return nil, err
	}
	return upstream.NewDiscoveryTarget(u, cfg, matches), nil
}

// discoveryConfig 统一手写及目录路由的协议、算法缺省值和熔断参数格式。
func discoveryConfig(t config.Target) upstream.UpstreamConfig {
	cfg := upstream.UpstreamConfig{Service: t.Service, Scheme: t.Scheme, LB: t.LB, Version: t.Version}
	if cfg.Scheme == "" {
		cfg.Scheme = "http"
	}
	if cfg.LB == "" {
		cfg.LB = "round_robin"
	}
	if t.Breaker != nil {
		cfg.Breaker = &upstream.BreakerConfig{
			FailureThreshold: t.Breaker.FailureThreshold,
			OpenDuration:     t.Breaker.OpenDuration.Std(),
		}
	}
	return cfg
}

// assignWeightRanges 校验灰度组的哈希键及权重总和，按声明顺序分配槽位区间。
func assignWeightRanges(routes []*route.Route) error {
	groups := map[string][]*route.WeightPredicate{}
	for _, r := range routes {
		for _, p := range r.Predicates {
			if wp, ok := p.(*route.WeightPredicate); ok {
				groups[wp.Group] = append(groups[wp.Group], wp)
			}
		}
	}
	for group, preds := range groups {
		key := http.CanonicalHeaderKey(preds[0].Key)
		total := 0
		for _, p := range preds {
			if http.CanonicalHeaderKey(p.Key) != key {
				return fmt.Errorf("灰度组 %q 的哈希键必须一致，实际包括 %q 和 %q", group, preds[0].Key, p.Key)
			}
			total += p.End // 构造时 End 即权重值
		}
		if total != 100 {
			return fmt.Errorf("灰度组 %q 权重之和为 %d（须为 100）", group, total)
		}
		start := 0
		for _, p := range preds {
			weight := p.End
			p.Start = start
			p.End = start + weight
			start += weight
		}
	}
	return nil
}
