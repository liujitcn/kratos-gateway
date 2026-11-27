package catalog

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	consulapi "github.com/hashicorp/consul/api"

	"github.com/go-kratos/kratos/v3/registry"
	"github.com/go-kratos/kratos/v3/selector"

	"github.com/liujitcn/kratos-gateway/internal/config"
	"github.com/liujitcn/kratos-gateway/internal/data/upstream"
)

// RoutesMetaKey 实例元数据中声明路由前缀列表的键（逗号分隔多个前缀）。
const RoutesMetaKey = "gateway_routes"

// LBMetaKey 实例元数据中声明负载均衡算法的键（缺省 round_robin）。
const LBMetaKey = "gateway_lb"

// Lister 服务目录列表器：返回健康实例按服务名分组（Consul 实现见 NewConsulLister）。
type Lister func(ctx context.Context) (map[string][]*registry.ServiceInstance, error)

// Catalog 服务目录聚合器：轮询目录，聚合 gateway_routes 元数据生成路由配置，变化时回调。
type Catalog struct {
	// lister 目录列表器。
	lister Lister
	// prefixes 只聚合服务名命中这些前缀的服务（空为全部）。
	prefixes []string
	// interval 轮询间隔。
	interval time.Duration
	// logger 日志。
	logger *slog.Logger
	// mu 保护首次查询状态和上次路由指纹。
	mu sync.Mutex
	// polled 表示已成功查询过目录，首次空目录也需要发布。
	polled bool
	// last 上次路由配置指纹（变更检测）。
	last string
}

// New 创建目录聚合器。
func New(lister Lister, prefixes []string, interval time.Duration, logger *slog.Logger) *Catalog {
	return &Catalog{
		lister:   lister,
		prefixes: prefixes,
		interval: interval,
		logger:   logger,
	}
}

// Run 启动轮询循环（ctx 结束退出）；首次成功查询及之后的路由变更均通过 fn 发布。
func (c *Catalog) Run(ctx context.Context, fn func(routes []config.Route)) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		routes, changed, err := c.poll(ctx)
		if err != nil {
			c.logger.Warn("轮询服务目录失败", "error", err)
			continue
		}
		if changed {
			fn(routes)
		}
	}
}

// poll 拉取目录并生成路由配置；首次成功查询或路由配置变化时标记 changed。
func (c *Catalog) poll(ctx context.Context) (routes []config.Route, changed bool, err error) {
	services, err := c.lister(ctx)
	if err != nil {
		return nil, false, err
	}
	names := make([]string, 0, len(services))
	for name := range services {
		names = append(names, name)
	}
	sort.Strings(names)
	routes = make([]config.Route, 0)
	for _, name := range names {
		if !matchServicePrefix(name, c.prefixes) {
			continue
		}
		routes = append(routes, routesOf(name, services[name], c.logger)...)
	}
	sort.Slice(routes, func(i, j int) bool { return routes[i].ID < routes[j].ID })
	fingerprint := fingerprintOf(routes)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.polled && fingerprint == c.last {
		return routes, false, nil
	}
	c.polled = true
	c.last = fingerprint
	return routes, true, nil
}

// RouteNodeMatcher 选择算法元数据有效且声明指定路由前缀的健康节点。
func RouteNodeMatcher(prefix string) func(selector.Node) bool {
	prefix = strings.TrimSuffix(prefix, "/")
	return func(node selector.Node) bool {
		metadata := node.Metadata()
		if !validRouteLB(metadata[LBMetaKey]) {
			return false
		}
		for declared := range strings.SplitSeq(metadata[RoutesMetaKey], ",") {
			if declared != "" && strings.TrimSuffix(declared, "/") == prefix {
				return true
			}
		}
		return false
	}
}

// NewConsulLister 创建可取消的 Consul 目录列表器，仅查询匹配前缀的服务的健康实例。
func NewConsulLister(client *consulapi.Client, prefixes []string) Lister {
	return func(ctx context.Context) (map[string][]*registry.ServiceInstance, error) {
		opts := (&consulapi.QueryOptions{}).WithContext(ctx)
		names, _, err := client.Catalog().Services(opts)
		if err != nil {
			return nil, fmt.Errorf("列出 Consul 服务: %w", err)
		}
		result := make(map[string][]*registry.ServiceInstance, len(names))
		for name := range names {
			if !matchServicePrefix(name, prefixes) {
				continue
			}
			var entries []*consulapi.ServiceEntry
			entries, _, err = client.Health().Service(name, "", true, opts)
			if err != nil {
				return nil, fmt.Errorf("查询 %s 健康实例: %w", name, err)
			}
			ins := make([]*registry.ServiceInstance, 0, len(entries))
			for _, entry := range entries {
				// 聚合仅消费元数据，转发节点由上游订阅维护。
				ins = append(ins, &registry.ServiceInstance{
					ID:       entry.Service.ID,
					Name:     entry.Service.Service,
					Metadata: entry.Service.Meta,
				})
			}
			result[name] = ins
		}
		return result, nil
	}
}

// routesOf 聚合算法有效的实例，合并其路径前缀并采用首个非空算法声明。
func routesOf(name string, ins []*registry.ServiceInstance, logger *slog.Logger) []config.Route {
	prefixes := map[string]struct{}{}
	lb := ""
	for _, in := range ins {
		instanceLB := in.Metadata[LBMetaKey]
		if !validRouteLB(instanceLB) {
			logger.Warn("跳过负载均衡算法无效的目录实例", "service", name, "instance", in.ID, "lb", instanceLB)
			continue
		}
		for p := range strings.SplitSeq(in.Metadata[RoutesMetaKey], ",") {
			if p != "" {
				prefixes[p] = struct{}{}
			}
		}
		if lb == "" {
			lb = instanceLB
		}
	}
	if len(prefixes) == 0 {
		return nil
	}
	if lb == "" {
		lb = "round_robin"
	}
	sorted := make([]string, 0, len(prefixes))
	for p := range prefixes {
		sorted = append(sorted, p)
	}
	sort.Strings(sorted)
	routes := make([]config.Route, 0, len(sorted))
	for _, p := range sorted {
		routes = append(routes, config.Route{
			ID:         "catalog-" + name + "-" + p,
			Predicates: []config.Predicate{{Path: p}},
			Target:     config.Target{Service: name, Scheme: "http", LB: lb},
		})
	}
	return routes
}

// validRouteLB 校验目录实例的算法声明，空值采用目录默认算法。
func validRouteLB(lb string) bool {
	return lb == "" || upstream.SupportsLB(lb)
}

// matchServicePrefix 判断服务名是否命中聚合前缀（前缀列表为空时聚合全部）。
func matchServicePrefix(name string, prefixes []string) bool {
	if len(prefixes) == 0 {
		return true
	}
	for _, p := range prefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// fingerprintOf 生成路由配置指纹（变更检测）。
func fingerprintOf(routes []config.Route) string {
	parts := make([]string, 0, len(routes))
	for _, r := range routes {
		parts = append(parts, r.ID+"|"+r.Target.Service+"|"+r.Target.Scheme+"|"+r.Target.LB)
	}
	return strings.Join(parts, ";")
}
