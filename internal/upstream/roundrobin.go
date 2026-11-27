// Package upstream 维护服务发现节点表与负载均衡选择器：
// Manager 统一缓存共享 Upstream（Watch 同步循环、节点快照和熔断状态）。
// 自描述路由各自维护轮询状态，发现快照变化时更新选择器的候选节点。
package upstream

import (
	"context"
	"sync/atomic"

	"github.com/go-kratos/kratos/v3/selector"
)

// RoundRobin 自建简单计数轮询选择器：等权重轮询，等价于 wrr 全权重为 1 的场景（dev 场景足够）。
type RoundRobin struct {
	// index 轮询计数器。
	index atomic.Int64
	// nodes 当前节点表。
	nodes atomic.Value // []selector.Node
}

// Apply 实现 selector.Rebalancer：重灌节点表。
func (r *RoundRobin) Apply(nodes []selector.Node) {
	if nodes == nil {
		nodes = []selector.Node{}
	}
	r.nodes.Store(nodes)
}

// Select 实现 selector.Selector：应用节点过滤（熔断/重试排除）后按计数轮询挑一个节点。
func (r *RoundRobin) Select(ctx context.Context, opts ...selector.SelectOption) (selector.Node, selector.DoneFunc, error) {
	nodes, _ := r.nodes.Load().([]selector.Node)
	candidates := nodes
	var options selector.SelectOptions
	for _, opt := range opts {
		opt(&options)
	}
	for _, nodeFilter := range options.NodeFilters {
		candidates = nodeFilter(ctx, candidates)
	}
	if len(candidates) == 0 {
		return nil, nil, selector.ErrNoAvailable
	}
	node := candidates[int(r.index.Add(1)-1)%len(candidates)]
	return node, func(context.Context, selector.DoneInfo) {}, nil
}
