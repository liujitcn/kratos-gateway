package upstream

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/go-kratos/kratos/v3/selector"

	"github.com/liujitcn/kratos-gateway/internal/biz/gateway/route"
)

// DiscoveryTarget 服务发现转发目标，共享实例池及熔断状态，自描述路由各自维护选择器。
type DiscoveryTarget struct {
	// upstream 目标上游实例池。
	upstream *Upstream
	// key 管理器缓存键（Keep 回收计算用）。
	key string
	// breakerConfig 候选路由发布时生效的共享熔断参数。
	breakerConfig BreakerConfig
	// matches 自描述路由的实例筛选条件。
	matches func(selector.Node) bool
	// sel 自描述路由专用的选择器。
	sel selector.Selector
	// mu 串行更新选择器及其已应用的节点快照。
	mu       sync.Mutex
	snapshot *[]selector.Node
}

// NewDiscoveryTarget 使用已发现的共享上游构造转发目标并保留待发布的熔断配置。
func NewDiscoveryTarget(u *Upstream, cfg UpstreamConfig, matches func(selector.Node) bool) *DiscoveryTarget {
	t := &DiscoveryTarget{upstream: u, key: cfg.Key(), breakerConfig: cfg.breakerConfig(), matches: matches}
	if matches != nil {
		t.sel = selectorFactories[u.lb]()
	}
	return t
}

// ApplyBreakerConfig 发布待生效的熔断参数，保留共享上游的当前状态。
func (t *DiscoveryTarget) ApplyBreakerConfig() {
	t.upstream.breaker.applyConfig(t.breakerConfig)
}

// Select 按当前路由的实例范围选路，目录选择器仅在节点快照变化后更新。
func (t *DiscoveryTarget) Select(ctx context.Context) (selector.Node, selector.DoneFunc, error) {
	if t.sel == nil {
		return t.upstream.Select(ctx)
	}
	t.mu.Lock()
	if snapshot := t.upstream.nodes.Load(); snapshot != t.snapshot {
		matched := make([]selector.Node, 0, len(*snapshot))
		for _, node := range *snapshot {
			if t.matches(node) {
				matched = append(matched, node)
			}
		}
		t.sel.Apply(matched)
		t.snapshot = snapshot
	}
	t.mu.Unlock()
	return t.upstream.selectNode(ctx, t.sel, t.matches)
}

// HasDiscoveredNodes 判断当前快照是否存在匹配本路由的健康节点，用于就绪探针。
func (t *DiscoveryTarget) HasDiscoveredNodes() bool {
	nodes := t.upstream.currentNodes()
	if t.matches == nil {
		return len(nodes) > 0
	}
	return slices.ContainsFunc(nodes, t.matches)
}

// Key 返回管理器缓存键（热更新时计算路由引用集合）。
func (t *DiscoveryTarget) Key() string {
	return t.key
}

// StickyKey 返回共享服务发现池的粘性标识。
func (t *DiscoveryTarget) StickyKey() string {
	return "discovery:" + t.key
}

// StaticTarget 静态地址转发目标：构造时固化节点表（联调/降级场景，不参与服务发现与熔断）。
type StaticTarget struct {
	// sel 静态节点选择器。
	sel selector.Selector
	// nodes 固化的静态节点表，用于粘性选择。
	nodes []selector.Node
	// stickyKey 静态地址配置对应的粘性标识。
	stickyKey string
}

// NewStaticTarget 创建静态转发目标：解析地址列表并固化到选择器（构造时 Apply 一次，此后不再变更）。
func NewStaticTarget(urls []string, lb string) (*StaticTarget, error) {
	sel, err := newSelector(lb)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(urls))
	addresses := make([]string, 0, len(urls))
	var nodes []selector.Node
	for _, s := range urls {
		var scheme, addr string
		scheme, addr, err = parseStaticURL(s)
		if err != nil {
			return nil, err
		}
		if _, dup := seen[addr]; dup {
			continue
		}
		seen[addr] = struct{}{}
		nodes = append(nodes, selector.NewNode(scheme, addr, nil))
		addresses = append(addresses, scheme+"://"+addr)
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("静态地址列表为空")
	}
	sel.Apply(nodes)
	return &StaticTarget{
		sel:       sel,
		nodes:     nodes,
		stickyKey: "static:" + lb + "\x00" + strings.Join(addresses, "\x00"),
	}, nil
}

// Select 优先复用仍可选的粘性节点，重试时排除已经尝试过的静态节点。
func (t *StaticTarget) Select(ctx context.Context) (selector.Node, selector.DoneFunc, error) {
	if preferred := route.StickyAddrFrom(ctx); preferred != "" && !isExcluded(ctx, preferred) {
		for _, node := range t.nodes {
			if node.Address() == preferred {
				return node, func(context.Context, selector.DoneInfo) {}, nil
			}
		}
	}
	if len(route.ExcludedFrom(ctx)) > 0 {
		return t.sel.Select(ctx, selector.WithNodeFilter(excludeAttempted))
	}
	return t.sel.Select(ctx)
}

// StickyKey 返回静态节点池的粘性标识。
func (t *StaticTarget) StickyKey() string {
	return t.stickyKey
}

// parseStaticURL 解析静态地址：完整 URL（含 http/https scheme）取 scheme 与 host；裸地址默认 http。
func parseStaticURL(s string) (string, string, error) {
	u, err := url.Parse(s)
	if err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" {
		return u.Scheme, u.Host, nil
	}
	if strings.Contains(s, "://") {
		return "", "", fmt.Errorf("静态地址 %q 协议仅支持 http/https", s)
	}
	if s == "" {
		return "", "", fmt.Errorf("静态地址为空")
	}
	return "http", s, nil
}

// excludeAttempted 在均衡选路时跳过已经尝试过的节点。
func excludeAttempted(ctx context.Context, nodes []selector.Node) []selector.Node {
	kept := make([]selector.Node, 0, len(nodes))
	for _, node := range nodes {
		if !isExcluded(ctx, node.Address()) {
			kept = append(kept, node)
		}
	}
	return kept
}

// isExcluded 判断地址是否在转发重试排除集中。
func isExcluded(ctx context.Context, addr string) bool {
	_, ok := route.ExcludedFrom(ctx)[addr]
	return ok
}
