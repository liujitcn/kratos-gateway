package upstream

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kratos/kratos/v3/selector"
)

// NodeBreaker 节点级被动熔断器：转发连续失败达到阈值后摘除节点，冷却后半开放行单笔探测恢复。
// 仅统计传输类失败（转发错误）；上游业务 5xx 属于正常响应，不参与熔断。
type NodeBreaker struct {
	// config 热更新时整体替换熔断参数，节点状态独立保留。
	config atomic.Pointer[BreakerConfig]
	// states 节点地址 → 熔断状态。
	states sync.Map
}

// NewNodeBreaker 创建使用指定阈值和冷却期的节点熔断器。
func NewNodeBreaker(threshold int, openDuration time.Duration) *NodeBreaker {
	b := &NodeBreaker{}
	b.config.Store(&BreakerConfig{FailureThreshold: threshold, OpenDuration: openDuration})
	return b
}

// breakerPermit 标识已获准请求所属的熔断周期与探测资格。
type breakerPermit struct {
	generation uint64
	probe      bool
}

// Allow 为已选节点发放请求许可，冷却后只发放一个半开探测许可。
func (b *NodeBreaker) Allow(addr string) (breakerPermit, bool) {
	st := b.stateOf(addr)
	st.mu.Lock()
	defer st.mu.Unlock()
	if !b.available(st) {
		return breakerPermit{}, false
	}
	if st.open {
		st.probing = true
	}
	return breakerPermit{generation: st.generation, probe: st.open}, true
}

// Record 记录当前一轮熔断状态的请求结果，请求取消或其期限到期时只释放半开探测。
func (b *NodeBreaker) Record(ctx context.Context, addr string, permit breakerPermit, err error) {
	st := b.stateOf(addr)
	st.mu.Lock()
	defer st.mu.Unlock()
	if permit.generation != st.generation {
		return
	}
	if err == nil {
		st.failures = 0
		st.open = false
		st.probing = false
		return
	}
	if ctx.Err() != nil {
		st.probing = false
		return
	}
	st.failures++
	if st.probing || st.failures >= b.config.Load().FailureThreshold {
		st.generation++
		st.probing = false
		st.open = true
		st.openedAt = time.Now()
		st.failures = 0
	}
}

// recoverProbe 在有效响应到达时恢复节点，流传输期间仍保留故障上报。
func (b *NodeBreaker) recoverProbe(addr string, permit breakerPermit) bool {
	st := b.stateOf(addr)
	st.mu.Lock()
	defer st.mu.Unlock()
	if permit.generation != st.generation || !st.probing {
		return false
	}
	st.failures = 0
	st.open = false
	st.probing = false
	return true
}

// applyConfig 更新熔断参数，保留节点的失败计数和冷却起点。
func (b *NodeBreaker) applyConfig(cfg BreakerConfig) {
	if *b.config.Load() != cfg {
		b.config.Store(&cfg)
	}
}

// Filter 按当前熔断状态过滤候选节点，实际选中后再占用半开探测资格。
func (b *NodeBreaker) Filter(_ context.Context, nodes []selector.Node) []selector.Node {
	allowed := make([]selector.Node, 0, len(nodes))
	for _, node := range nodes {
		st := b.stateOf(node.Address())
		st.mu.Lock()
		available := b.available(st)
		st.mu.Unlock()
		if available {
			allowed = append(allowed, node)
		}
	}
	return allowed
}

// available 在持有节点锁时按当前冷却期判断节点能否接收请求。
func (b *NodeBreaker) available(st *breakerState) bool {
	return !st.open || (!st.probing && time.Since(st.openedAt) >= b.config.Load().OpenDuration)
}

// breakerState 单节点熔断状态。
type breakerState struct {
	mu         sync.Mutex
	failures   int
	open       bool
	probing    bool
	openedAt   time.Time
	generation uint64
}

// stateOf 取节点熔断状态（无则创建）。
func (b *NodeBreaker) stateOf(addr string) *breakerState {
	if st, ok := b.states.Load(addr); ok {
		return st.(*breakerState)
	}
	st := &breakerState{}
	actual, _ := b.states.LoadOrStore(addr, st)
	return actual.(*breakerState)
}
