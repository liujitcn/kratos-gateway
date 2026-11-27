package upstream

import (
	"context"
	"log/slog"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kratos/kratos/v3/registry"
	"github.com/go-kratos/kratos/v3/selector"

	"github.com/liujitcn/kratos-gateway/internal/route"
)

// BreakerConfig 节点被动熔断参数。
type BreakerConfig struct {
	// FailureThreshold 连续失败熔断阈值。
	FailureThreshold int
	// OpenDuration 熔断打开时长（冷却期）。
	OpenDuration time.Duration
}

// breakerDefaults 默认熔断参数：连续失败 5 次熔断 10s。
var breakerDefaults = BreakerConfig{FailureThreshold: 5, OpenDuration: 10 * time.Second}

// Upstream 单服务上游实例池：Watch 同步循环维护节点表，Select 叠加粘性复用、版本过滤、熔断与重试排除规则挑节点。
type Upstream struct {
	// name 规范化后的服务名（与注册方一致）。
	name string
	// scheme 上游协议（从实例 Endpoints 筛选）。
	scheme string
	// lb 负载均衡算法名。
	lb string
	// version 版本过滤（元数据 version 优先，兼容实例 Version；空为全版本）。
	version string
	// sel 负载均衡选择器。
	sel selector.Selector
	// breaker 节点被动熔断器。
	breaker *NodeBreaker
	// watcher 服务发现监视器。
	watcher registry.Watcher
	// nodes 最新节点快照，供目录路由、粘性选择和统计读取。
	nodes atomic.Pointer[[]selector.Node]
	// hits 节点地址 → 累计命中计数。
	hits sync.Map
	// ctx 生命周期上下文（Watch 循环退出信号）。
	ctx    context.Context
	cancel context.CancelFunc
	// logger 日志。
	logger *slog.Logger
}

// newUpstream 创建指定配置的上游实例池并启动 Watch 同步循环。
func newUpstream(name string, cfg UpstreamConfig, watcher registry.Watcher, logger *slog.Logger) (*Upstream, error) {
	sel, err := newSelector(cfg.LB)
	if err != nil {
		return nil, err
	}
	breaker := cfg.breakerConfig()
	ctx, cancel := context.WithCancel(context.Background())
	u := &Upstream{
		name:    name,
		scheme:  cfg.Scheme,
		lb:      cfg.LB,
		version: cfg.Version,
		sel:     sel,
		breaker: NewNodeBreaker(breaker.FailureThreshold, breaker.OpenDuration),
		watcher: watcher,
		ctx:     ctx,
		cancel:  cancel,
		logger:  logger,
	}
	go u.run()
	return u, nil
}

// Select 从池内选择节点，转发完成后调用返回的回调上报结果。
func (u *Upstream) Select(ctx context.Context) (selector.Node, selector.DoneFunc, error) {
	return u.selectNode(ctx, u.sel, nil)
}

// selectNode 使用指定选择器选路，应用共享的粘性、重试排除及熔断规则。
func (u *Upstream) selectNode(ctx context.Context, sel selector.Selector, matches func(selector.Node) bool) (selector.Node, selector.DoneFunc, error) {
	if preferred := route.StickyAddrFrom(ctx); preferred != "" && !isExcluded(ctx, preferred) {
		if node := u.nodeOf(preferred); node != nil && (matches == nil || matches(node)) {
			if permit, ok := u.breaker.Allow(preferred); ok {
				return u.selected(node, nil, permit)
			}
		}
	}
	var filters []selector.NodeFilter
	if len(route.ExcludedFrom(ctx)) > 0 {
		filters = append(filters, excludeAttempted)
	}
	filters = append(filters, u.breaker.Filter)
	filter := selector.WithNodeFilter(filters...)
	for {
		node, done, err := sel.Select(ctx, filter)
		if err != nil {
			return nil, nil, err
		}
		if permit, ok := u.breaker.Allow(node.Address()); ok {
			return u.selected(node, done, permit)
		}
		// 过滤后另一请求可能抢先占用半开探测，重新筛选剩余节点。
		done(ctx, selector.DoneInfo{Err: selector.ErrNoAvailable})
		if err = ctx.Err(); err != nil {
			return nil, nil, err
		}
	}
}

// selected 包装节点的半开探测及转发完成回调，并累加命中次数。
func (u *Upstream) selected(node selector.Node, done selector.DoneFunc, permit breakerPermit) (selector.Node, selector.DoneFunc, error) {
	addr := node.Address()
	u.hitOf(addr).Add(1)
	var probe *probeNode
	if permit.probe {
		probe = &probeNode{Node: node, breaker: u.breaker, permit: permit}
		node = probe
	}
	return node, func(ctx context.Context, di selector.DoneInfo) {
		if probe == nil || !probe.recovered || di.Err != nil {
			u.breaker.Record(ctx, addr, permit, di.Err)
		}
		if done != nil {
			done(ctx, di)
		}
	}, nil
}

// probeNode 记录已经开始接收响应的半开探测，避免在流结束时重复记为成功。
type probeNode struct {
	selector.Node
	breaker   *NodeBreaker
	permit    breakerPermit
	recovered bool
}

// ResponseReceived 在收到有效响应时结束半开探测。
func (n *probeNode) ResponseReceived() {
	n.recovered = n.recovered || n.breaker.recoverProbe(n.Address(), n.permit)
}

// Snapshot 上游状态快照：服务名、协议、算法与节点命中统计（/gw-stats 数据源）。
func (u *Upstream) Snapshot() UpstreamStat {
	nodes := u.currentNodes()
	stats := make([]NodeStat, 0, len(nodes))
	for _, node := range nodes {
		var hits int64
		if c, ok := u.hits.Load(node.Address()); ok {
			hits = c.(*atomic.Int64).Load()
		}
		weight := 0
		if w := node.InitialWeight(); w != nil {
			weight = int(*w)
		}
		stats = append(stats, NodeStat{Address: node.Address(), Weight: weight, Hits: hits})
	}
	return UpstreamStat{Service: u.name, Scheme: u.scheme, LB: u.lb, Nodes: stats}
}

// run Watch 同步循环：实例表变更时发布节点快照并更新共享选择器，出错后退避重试。
func (u *Upstream) run() {
	for {
		ins, err := u.watcher.Next()
		if u.ctx.Err() != nil {
			return
		}
		if err != nil {
			u.logger.Warn("监听实例变更失败，1s 后重试", "service", u.name, "error", err)
			select {
			case <-u.ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		u.applyNodes(ins)
	}
}

// applyNodes 筛选匹配版本的实例，将同一节点表发布给均衡和粘性选路。
func (u *Upstream) applyNodes(ins []*registry.ServiceInstance) {
	if u.version != "" {
		matched := make([]*registry.ServiceInstance, 0, len(ins))
		for _, instance := range ins {
			version := instance.Metadata["version"]
			if version == "" {
				version = instance.Version
			}
			if version == u.version {
				matched = append(matched, instance)
			}
		}
		ins = matched
	}
	nodes := BuildNodes(ins, u.scheme)
	u.sel.Apply(nodes)
	u.nodes.Store(&nodes)
}

// stop 停止上游：关闭监视器并终止同步循环。
func (u *Upstream) stop() {
	u.cancel()
	if err := u.watcher.Stop(); err != nil {
		u.logger.Warn("关闭 watcher 失败", "service", u.name, "error", err)
	}
}

// hitOf 取节点命中计数器（无则创建）。
func (u *Upstream) hitOf(addr string) *atomic.Int64 {
	if c, ok := u.hits.Load(addr); ok {
		return c.(*atomic.Int64)
	}
	c := &atomic.Int64{}
	actual, _ := u.hits.LoadOrStore(addr, c)
	return actual.(*atomic.Int64)
}

// nodeOf 按地址查找当前节点表中的节点（粘性复用路径；未找到返回 nil）。
func (u *Upstream) nodeOf(addr string) selector.Node {
	for _, node := range u.currentNodes() {
		if node.Address() == addr {
			return node
		}
	}
	return nil
}

// currentNodes 读取共享节点快照，首次发现前返回空列表。
func (u *Upstream) currentNodes() []selector.Node {
	if nodes := u.nodes.Load(); nodes != nil {
		return *nodes
	}
	return nil
}

// UpstreamStat 上游状态快照。
type UpstreamStat struct {
	// Service 规范化服务名。
	Service string
	// Scheme 上游协议。
	Scheme string
	// LB 负载均衡算法。
	LB string
	// Nodes 节点命中统计列表。
	Nodes []NodeStat
}

// NodeStat 节点命中统计。
type NodeStat struct {
	// Address 节点地址（host:port）。
	Address string
	// Weight 注册权重（未注册为 0）。
	Weight int
	// Hits 累计命中次数。
	Hits int64
}

// BuildNodes 从实例列表提取匹配 scheme 的端点节点（按地址去重；权重读实例 Metadata["weight"]）。
func BuildNodes(ins []*registry.ServiceInstance, scheme string) []selector.Node {
	seen := make(map[string]struct{}, len(ins))
	var nodes []selector.Node
	for _, in := range ins {
		for _, ep := range in.Endpoints {
			u, err := url.Parse(ep)
			if err != nil || u.Scheme != scheme || u.Host == "" {
				continue
			}
			if _, dup := seen[u.Host]; dup {
				continue
			}
			seen[u.Host] = struct{}{}
			nodes = append(nodes, selector.NewNode(scheme, u.Host, in))
		}
	}
	return nodes
}
