package route

import (
	"net/http"
	"sort"
	"sync/atomic"
)

// Router 路由器：按段前缀最长优先、配置声明顺序匹配请求，支持路由表原子热切换。
// 匹配语义：候选路由按路径谓词段数降序预排序，线性扫描返回首个全谓词命中的路由；
// 该语义等价于前缀树最长匹配，在百级路由规模下线性扫描更简单且开销可忽略。
type Router struct {
	// routes 预排序路由表（段数降序、声明顺序稳定），指针形式原子替换。
	routes atomic.Pointer[[]*Route]
}

// NewRouter 创建空路由表的路由器（待 Update 填充）。
func NewRouter() *Router {
	r := &Router{}
	empty := []*Route{}
	r.routes.Store(&empty)
	return r
}

// Update 原子替换路由表：排序（路径段数降序、声明顺序稳定）后存储，替换期间在途请求继续走旧表。
func (rt *Router) Update(routes []*Route) {
	sorted := make([]*Route, len(routes))
	copy(sorted, routes)
	sort.SliceStable(sorted, func(i, j int) bool {
		return pathDepth(sorted[i]) > pathDepth(sorted[j])
	})
	rt.routes.Store(&sorted)
}

// Match 返回首个全谓词命中的路由；未命中返回 nil。
func (rt *Router) Match(r *http.Request) *Route {
	routes := *rt.routes.Load()
	for _, route := range routes {
		if matchRoute(route, r) {
			return route
		}
	}
	return nil
}

// Routes 返回当前路由表快照（/gw-stats 展示用）。
func (rt *Router) Routes() []*Route {
	return *rt.routes.Load()
}

// ServeHTTP 实现 http.Handler：命中路由走其处理器，未命中回 404。
func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	route := rt.Match(r)
	if route == nil {
		http.Error(w, "gateway: no route", http.StatusNotFound)
		return
	}
	if info := ReqInfoFrom(r.Context()); info != nil {
		info.RouteID = route.ID
	}
	route.Handler.ServeHTTP(w, r)
}

// matchRoute 判断路由全部谓词是否命中。
func matchRoute(route *Route, r *http.Request) bool {
	for _, p := range route.Predicates {
		if !p.Match(r) {
			return false
		}
	}
	return true
}

// pathDepth 路由的路径谓词段数（取首个路径谓词；无路径谓词记 0，匹配任意路径）。
func pathDepth(route *Route) int {
	for _, p := range route.Predicates {
		if pp, ok := p.(*PathPredicate); ok {
			return pp.Segments
		}
	}
	return 0
}
