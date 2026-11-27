package catalog

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v3/registry"
	"github.com/go-kratos/kratos/v3/selector"

	"github.com/liujitcn/kratos-gateway/internal/config"
)

// fakeLister 构造固定目录数据的列表器。
func fakeLister(services map[string][]*registry.ServiceInstance) Lister {
	return func(context.Context) (map[string][]*registry.ServiceInstance, error) {
		return services, nil
	}
}

// TestCatalogAggregate 验证有效实例生成路由，跳过无声明服务和算法无效的实例，重复快照去重。
func TestCatalogAggregate(t *testing.T) {
	services := map[string][]*registry.ServiceInstance{
		"bi-service": {
			{ID: "bi-bad", Name: "bi-service", Metadata: map[string]string{RoutesMetaKey: "/bad-peer", LBMetaKey: "unsupported"}},
			{ID: "bi-1", Name: "bi-service", Metadata: map[string]string{RoutesMetaKey: "/api/v1/bi,/api/v1/report"}},
			{ID: "bi-2", Name: "bi-service", Metadata: map[string]string{RoutesMetaKey: "/api/v1/bi", LBMetaKey: "wrr"}},
		},
		"plain-service": {
			{ID: "p-1", Name: "plain-service", Metadata: map[string]string{"other": "x"}},
		},
		"bad-service": {
			{ID: "bad-1", Name: "bad-service", Metadata: map[string]string{RoutesMetaKey: "/bad", LBMetaKey: "unsupported"}},
		},
	}
	c := New(fakeLister(services), nil, 20*time.Millisecond, slog.New(slog.DiscardHandler))

	routes, changed, err := c.poll(context.Background())
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}
	if !changed {
		t.Fatal("首次聚合应视为变更")
	}
	if len(routes) != 2 {
		t.Fatalf("应生成 2 条路由（前缀取并集）: %d", len(routes))
	}
	if routes[0].ID != "catalog-bi-service-/api/v1/bi" || routes[0].Predicates[0].Path != "/api/v1/bi" {
		t.Errorf("路由生成错误: %+v", routes[0])
	}
	if routes[1].Predicates[0].Path != "/api/v1/report" {
		t.Errorf("并集前缀缺失: %+v", routes[1])
	}
	if routes[0].Target.LB != "wrr" {
		t.Errorf("算法元数据应生效（后实例声明覆盖空值）: %+v", routes[0].Target)
	}

	// 相同目录再次聚合不应触发变更回调
	_, changed, err = c.poll(context.Background())
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}
	if changed {
		t.Error("目录无变化不应触发回调")
	}
}

// TestCatalogConsulMetadata 验证路由聚合与节点筛选只读取下划线元数据键。
func TestCatalogConsulMetadata(t *testing.T) {
	current := &registry.ServiceInstance{Metadata: map[string]string{
		"gateway_routes": "/current",
		"gateway.lb":     "unsupported",
	}}
	legacy := &registry.ServiceInstance{Metadata: map[string]string{"gateway.routes": "/legacy"}}
	routes := routesOf("app", []*registry.ServiceInstance{current, legacy}, slog.New(slog.DiscardHandler))
	if len(routes) != 1 || routes[0].Predicates[0].Path != "/current" || routes[0].Target.LB != "round_robin" {
		t.Errorf("应忽略带点的元数据键，并使用默认负载均衡算法: %+v", routes)
	}
	currentNode := selector.NewNode("http", "127.0.0.1:8080", current)
	legacyNode := selector.NewNode("http", "127.0.0.1:8081", legacy)
	if !RouteNodeMatcher("/current")(currentNode) || RouteNodeMatcher("/legacy")(legacyNode) {
		t.Error("节点筛选应只读取下划线元数据键")
	}
}

// TestCatalogServicePrefix 验证服务名前缀过滤。
func TestCatalogServicePrefix(t *testing.T) {
	services := map[string][]*registry.ServiceInstance{
		"app-a":   {{ID: "a", Name: "app-a", Metadata: map[string]string{RoutesMetaKey: "/a"}}},
		"infra-b": {{ID: "b", Name: "infra-b", Metadata: map[string]string{RoutesMetaKey: "/b"}}},
	}
	c := New(fakeLister(services), []string{"app-"}, 20*time.Millisecond, slog.New(slog.DiscardHandler))
	routes, _, err := c.poll(context.Background())
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}
	if len(routes) != 1 || routes[0].ID != "catalog-app-a-/a" {
		t.Errorf("前缀过滤错误: %+v", routes)
	}
}

// TestCatalogRun 验证轮询发布首次空目录和后续目录变化。
func TestCatalogRun(t *testing.T) {
	var mu sync.Mutex
	current := map[string][]*registry.ServiceInstance{}
	lister := func(context.Context) (map[string][]*registry.ServiceInstance, error) {
		mu.Lock()
		defer mu.Unlock()
		return current, nil
	}
	c := New(lister, nil, 10*time.Millisecond, slog.New(slog.DiscardHandler))

	got := make(chan []config.Route, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx, func(routes []config.Route) { got <- routes })

	select {
	case routes := <-got:
		if routes == nil || len(routes) != 0 {
			t.Errorf("首次成功查询应发布空目录: %+v", routes)
		}
	case <-time.After(time.Second):
		t.Fatal("首次成功查询未发布空目录")
	}

	// 声明路由的服务出现后触发回调。
	mu.Lock()
	current = map[string][]*registry.ServiceInstance{"bi": {{ID: "bi", Metadata: map[string]string{RoutesMetaKey: "/bi"}}}}
	mu.Unlock()

	select {
	case routes := <-got:
		if len(routes) != 1 || routes[0].Predicates[0].Path != "/bi" {
			t.Errorf("回调路由错误: %+v", routes)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("等待聚合回调超时")
	}
}
