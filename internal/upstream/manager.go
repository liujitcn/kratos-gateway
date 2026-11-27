package upstream

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/go-kratos/kratos/v3/registry"
	"github.com/go-kratos/kratos/v3/selector"
	"github.com/go-kratos/kratos/v3/selector/wrr"

	kitreg "github.com/liujitcn/kratos-kit/registry"
)

// selectorFactories 保存支持的负载均衡算法及其构造函数。
var selectorFactories = map[string]func() selector.Selector{
	"round_robin": func() selector.Selector { return &RoundRobin{} },
	wrr.Name:      wrr.NewBuilder().Build,
}

// Manager 按服务、协议、算法和版本缓存 Upstream，相同配置共享同一 Watch 循环。
type Manager struct {
	// disc 注册中心发现客户端。
	disc registry.Discovery
	// regType 注册中心类型（NormalizeForRegistry 规范化参数，须与注册方一致）。
	regType string
	// logger 日志。
	logger *slog.Logger
	// mu 保护 upstreams 并发创建与回收。
	mu sync.Mutex
	// upstreams 已创建上游（键：service/scheme/lb[/version]）。
	upstreams map[string]*Upstream
}

// UpstreamConfig 创建上游所需的参数。
type UpstreamConfig struct {
	// Service 服务名（规范化前）。
	Service string
	// Scheme 上游协议。
	Scheme string
	// LB 负载均衡算法。
	LB string
	// Version 版本过滤（空为全版本；同服务不同版本视为不同上游）。
	Version string
	// Breaker 上游池的共享熔断参数，nil 采用默认值。
	Breaker *BreakerConfig
}

// NewManager 创建上游管理器。
func NewManager(disc registry.Discovery, regType string, logger *slog.Logger) *Manager {
	return &Manager{
		disc:      disc,
		regType:   regType,
		logger:    logger,
		upstreams: make(map[string]*Upstream),
	}
}

// Get 返回（或创建并启动）指定服务的上游实例池。
func (m *Manager) Get(cfg UpstreamConfig) (*Upstream, error) {
	if m.disc == nil {
		return nil, fmt.Errorf("服务 %s 需要配置注册中心", cfg.Service)
	}
	key := cfg.Key()
	m.mu.Lock()
	defer m.mu.Unlock()
	if u, ok := m.upstreams[key]; ok {
		return u, nil
	}
	name := kitreg.NormalizeForRegistry(cfg.Service, m.regType)
	watcher, err := m.disc.Watch(context.Background(), name)
	if err != nil {
		return nil, fmt.Errorf("创建 %s 服务 watcher: %w", name, err)
	}
	var u *Upstream
	u, err = newUpstream(name, cfg, watcher, m.logger)
	if err != nil {
		if stopErr := watcher.Stop(); stopErr != nil {
			m.logger.Warn("回滚关闭 watcher 失败", "service", name, "error", stopErr)
		}
		return nil, err
	}
	m.upstreams[key] = u
	return u, nil
}

// Ready 判断上游是否已发布首个成功的服务发现快照，成功的空快照也算就绪。
func (m *Manager) Ready(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	u := m.upstreams[key]
	return u != nil && u.nodes.Load() != nil
}

// Keep 仅保留指定键集合的上游：路由表热更新后回收不再被引用的 Watch 循环。
// 在途请求仍可使用已回收上游的最后一次节点表（短暂陈旧，可接受）。
func (m *Manager) Keep(keys []string) {
	keep := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		keep[key] = struct{}{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, u := range m.upstreams {
		if _, ok := keep[key]; ok {
			continue
		}
		delete(m.upstreams, key)
		u.stop()
	}
}

// Stop 停止全部上游（进程退出路径）。
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, u := range m.upstreams {
		delete(m.upstreams, key)
		u.stop()
	}
}

// Snapshot 全部上游状态快照（/gw-stats 数据源）。
func (m *Manager) Snapshot() []UpstreamStat {
	m.mu.Lock()
	ups := make([]*Upstream, 0, len(m.upstreams))
	for _, u := range m.upstreams {
		ups = append(ups, u)
	}
	m.mu.Unlock()
	stats := make([]UpstreamStat, 0, len(ups))
	for _, u := range ups {
		stats = append(stats, u.Snapshot())
	}
	return stats
}

// SupportsLB 判断算法能否用于上游负载均衡。
func SupportsLB(lb string) bool {
	_, ok := selectorFactories[lb]
	return ok
}

// newSelector 按算法名构造负载均衡选择器：round_robin 自建，wrr 复用 kratos 实现。
func newSelector(lb string) (selector.Selector, error) {
	if factory, ok := selectorFactories[lb]; ok {
		return factory(), nil
	}
	return nil, fmt.Errorf("不支持的负载均衡算法 %q（可选 round_robin / wrr）", lb)
}

// Key 返回以服务、协议、算法和版本组成的上游池键。
func (cfg UpstreamConfig) Key() string {
	key := cfg.Service + "/" + cfg.Scheme + "/" + cfg.LB
	if cfg.Version != "" {
		key += "/" + cfg.Version
	}
	return key
}

// breakerConfig 返回上游池的生效熔断参数。
func (cfg UpstreamConfig) breakerConfig() BreakerConfig {
	if cfg.Breaker != nil {
		return *cfg.Breaker
	}
	return breakerDefaults
}
