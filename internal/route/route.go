// Package route 定义网关核心路由模型：Route/Target/Predicate/Filter 接口与路由器。
// 过滤器用标准 http.Handler 装饰链（链尾收敛到转发器），与 net/http 生态零摩擦。
package route

import (
	"context"
	"net/http"
	"sync"

	"github.com/go-kratos/kratos/v3/selector"
)

// Route 一条转发规则：谓词全匹配命中后，经过滤器链转发到 target。
type Route struct {
	// ID 路由唯一标识。
	ID string
	// Predicates 匹配断言列表（全部命中才匹配，AND 语义）。
	Predicates []Predicate
	// Filters 路由级过滤器（装配时按 Order 升序、声明顺序稳定排序后逐层包装 Handler）。
	Filters []Filter
	// Target 转发目标：选出本次转发的节点。
	Target Target
	// Metadata 路由扩展元数据（服务自描述路由聚合时携带原始注册元数据）。
	Metadata map[string]string
	// Handler 装配后的处理器（过滤器链收敛到 ForwardHandler），由构建方填充。
	Handler http.Handler
}

// Target 转发目标：选出本次转发的节点（服务发现或静态地址）。
type Target interface {
	// Select 返回本次转发的节点与完成回调；无可用节点时返回 selector.ErrNoAvailable。
	// 转发完成后必须调用 DoneFunc（喂 RTT/统计，kratos selector 接口约定）。
	Select(ctx context.Context) (selector.Node, selector.DoneFunc, error)
}

// Predicate 请求匹配断言（借鉴 Spring Cloud Gateway）。
type Predicate interface {
	// Match 判断请求是否命中断言。
	Match(r *http.Request) bool
}

// Filter 过滤器：装饰 next 构造处理链；Order 决定装配顺序（链尾为 ForwardHandler，不参与 Order）。
type Filter interface {
	// Order 装配顺序（升序；同 Order 按配置声明顺序）。
	Order() int
	// Do 装饰 next 返回新的处理器。
	Do(next http.Handler) http.Handler
}

// ReqInfo 请求观测信息：全局过滤器创建并挂入请求上下文，路由匹配与转发阶段逐步填充。
type ReqInfo struct {
	// RouteID 命中的路由 ID（未匹配为空）。
	RouteID string
	// Node 实际转发的上游节点地址。
	Node string
}

// reqInfoKey ReqInfo 上下文键。
type reqInfoKey struct{}

// WithReqInfo 将请求观测信息挂入上下文。
func WithReqInfo(ctx context.Context, info *ReqInfo) context.Context {
	return context.WithValue(ctx, reqInfoKey{}, info)
}

// ReqInfoFrom 读取上下文中的请求观测信息（不存在时返回 nil）。
func ReqInfoFrom(ctx context.Context) *ReqInfo {
	info, _ := ctx.Value(reqInfoKey{}).(*ReqInfo)
	return info
}

// excludedKey 转发重试排除集上下文键。
type excludedKey struct{}

// WithExcluded 在上下文中标记转发重试时应排除的节点地址集合（Target.Select 消费）。
func WithExcluded(ctx context.Context, addrs map[string]struct{}) context.Context {
	return context.WithValue(ctx, excludedKey{}, addrs)
}

// ExcludedFrom 读取转发重试排除集（未标记时返回 nil）。
func ExcludedFrom(ctx context.Context) map[string]struct{} {
	addrs, _ := ctx.Value(excludedKey{}).(map[string]struct{})
	return addrs
}

// StickySession 连接级粘性会话：为同一客户端 TCP 连接按上游池保存节点绑定（target.sticky）。
// 由 http.Server.ConnContext 创建并挂入连接上下文，随连接关闭被 GC 回收——无全局状态、无过期清理。
type StickySession struct {
	mu    sync.Mutex
	addrs map[string]string
}

// Addr 返回指定上游池的粘性节点地址（未绑定时为空）。
func (s *StickySession) Addr(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addrs[key]
}

// Bind 为指定上游池绑定节点地址，重试换节点时更新绑定。
func (s *StickySession) Bind(key, addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.addrs == nil {
		s.addrs = make(map[string]string)
	}
	s.addrs[key] = addr
}

// stickySessionKey 连接级粘性会话上下文键。
type stickySessionKey struct{}

// WithStickySession 将连接级粘性会话挂入上下文（由 http.Server.ConnContext 调用）。
func WithStickySession(ctx context.Context, sess *StickySession) context.Context {
	return context.WithValue(ctx, stickySessionKey{}, sess)
}

// StickySessionFrom 读取连接级粘性会话（不存在时返回 nil）。
func StickySessionFrom(ctx context.Context) *StickySession {
	sess, _ := ctx.Value(stickySessionKey{}).(*StickySession)
	return sess
}

// stickyAddrKey 期望粘住的节点地址上下文键。
type stickyAddrKey struct{}

// WithStickyAddr 标记本次选择期望粘住的节点地址（Target.Select 消费；节点不可用时回退常规选择）。
func WithStickyAddr(ctx context.Context, addr string) context.Context {
	return context.WithValue(ctx, stickyAddrKey{}, addr)
}

// StickyAddrFrom 读取期望粘住的节点地址（未标记为空）。
func StickyAddrFrom(ctx context.Context) string {
	addr, _ := ctx.Value(stickyAddrKey{}).(string)
	return addr
}
