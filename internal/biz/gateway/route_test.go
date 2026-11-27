package gateway

import (
	"errors"
	"log/slog"
	"testing"
	"testing/synctest"

	"github.com/go-kratos/kratos/v3/registry"
	"github.com/go-kratos/kratos/v3/selector"

	"github.com/liujitcn/kratos-gateway/internal/biz/gateway/route"
	"github.com/liujitcn/kratos-gateway/internal/config"
	"github.com/liujitcn/kratos-gateway/internal/data/catalog"
	"github.com/liujitcn/kratos-gateway/internal/data/testutil"
	"github.com/liujitcn/kratos-gateway/internal/data/upstream"
)

// newManager 创建测试管理器。
func newManager(t *testing.T) *upstream.Manager {
	t.Helper()
	return upstream.NewManager(testutil.NewFakeDiscovery(), "consul", slog.New(slog.DiscardHandler))
}

// TestBuildValidation 验证缺少必填项、未知过滤器和负数重试次数都会导致装配失败。
func TestBuildValidation(t *testing.T) {
	mgr := newManager(t)
	defer mgr.Stop()
	logger := slog.New(slog.DiscardHandler)

	cases := []config.Route{
		{Predicates: []config.Predicate{{Path: "/a"}}},
		{ID: "r", Predicates: []config.Predicate{}},
		{ID: "r", Predicates: []config.Predicate{{Path: "/a"}}},
		{ID: "r", Predicates: []config.Predicate{{Path: "/a"}}, Filters: []config.Filter{{Name: "no-such"}}},
		{ID: "r", Predicates: []config.Predicate{{Path: "/a"}}, Target: config.Target{URLs: []string{"http://127.0.0.1:18081"}, MaxRetry: new(-1)}},
	}
	for i, r := range cases {
		cfg := &config.Config{Routes: []config.Route{r}}
		if _, err := Build(cfg, mgr, nil, nil, logger); err == nil {
			t.Errorf("第 %d 项非法配置应报错", i+1)
		}
	}
}

// TestBuild 验证装配：路由构建、处理器可用、静态目标、引用键计算。
func TestBuild(t *testing.T) {
	mgr := newManager(t)
	defer mgr.Stop()
	logger := slog.New(slog.DiscardHandler)
	cfg := &config.Config{Routes: []config.Route{
		{
			ID:         "demo",
			Predicates: []config.Predicate{{Path: "/api/v1/demo"}},
			Filters:    []config.Filter{{Name: "add-header", Args: map[string]any{"key": "X-Mark", "value": "gw"}}},
			Target:     config.Target{Service: "demo-service"},
		},
		{
			ID:         "static",
			Predicates: []config.Predicate{{Path: "/static"}},
			Target:     config.Target{URLs: []string{"http://127.0.0.1:9001"}},
		},
	}}
	routes, err := Build(cfg, mgr, nil, nil, logger)
	if err != nil {
		t.Fatalf("装配失败: %v", err)
	}
	if len(routes) != 2 {
		t.Fatalf("路由数量错误: %d", len(routes))
	}
	if routes[0].Handler == nil {
		t.Error("处理器未装配")
	}
	keys := ReferencedKeys(routes)
	if len(keys) != 1 || keys[0] != "demo-service/http/round_robin" {
		t.Errorf("引用键错误: %v", keys)
	}
}

// TestBuildCatalogMerge 验证 catalog 路由合并：静态路由在前（同深度优先）。
func TestBuildCatalogMerge(t *testing.T) {
	mgr := newManager(t)
	defer mgr.Stop()
	logger := slog.New(slog.DiscardHandler)
	cfg := &config.Config{Routes: []config.Route{
		{ID: "static", Predicates: []config.Predicate{{Path: "/api/v1/x"}}, Target: config.Target{Service: "svc-a"}},
	}}
	catalogRoutes := []config.Route{
		{ID: "catalog-svc-b-/api/v1/x", Predicates: []config.Predicate{{Path: "/api/v1/x"}}, Target: config.Target{Service: "svc-b"}},
	}
	routes, err := Build(cfg, mgr, catalogRoutes, nil, logger)
	if err != nil {
		t.Fatalf("装配失败: %v", err)
	}
	// 同深度按声明顺序：静态路由先声明 → 排序稳定后仍在前
	if routes[0].ID != "static" {
		t.Errorf("静态路由应优先于同前缀 catalog 路由: %s", routes[0].ID)
	}
	if len(ReferencedKeys(routes)) != 2 {
		t.Errorf("应引用两个上游: %v", ReferencedKeys(routes))
	}
}

// TestBuildCatalogSelectsDeclaringInstances 验证自描述路由只选声明该前缀的实例，手写路由仍共享完整节点池。
func TestBuildCatalogSelectsDeclaringInstances(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := slog.New(slog.DiscardHandler)
		disc := testutil.NewFakeDiscovery()
		mgr := upstream.NewManager(disc, "consul", logger)
		defer mgr.Stop()
		target := config.Target{Service: "demo", LB: "wrr"}
		cfg := &config.Config{Routes: []config.Route{{
			ID: "manual", Predicates: []config.Predicate{{Path: "/manual"}}, Target: target,
		}}}
		routes, err := Build(cfg, mgr, []config.Route{{
			ID: "catalog-demo-/new", Predicates: []config.Predicate{{Path: "/new"}}, Target: target,
		}}, nil, logger)
		if err != nil {
			t.Fatal(err)
		}
		disc.Push("demo", []*registry.ServiceInstance{
			{ID: "old", Endpoints: []string{"http://127.0.0.1:18081"}, Metadata: map[string]string{catalog.RoutesMetaKey: "/old,/newest"}},
			{ID: "new", Endpoints: []string{"http://127.0.0.1:18082"}, Metadata: map[string]string{catalog.RoutesMetaKey: "/old,/new/"}},
		})
		synctest.Wait()
		ctx := route.WithStickyAddr(t.Context(), "127.0.0.1:18081")
		var node selector.Node
		var done selector.DoneFunc
		for range 4 {
			node, done, err = routes[1].Target.Select(ctx)
			if err != nil {
				t.Fatal(err)
			}
			done(ctx, selector.DoneInfo{})
			if node.Address() != "127.0.0.1:18082" {
				t.Fatalf("自描述路由应跳过未声明前缀的粘性节点，实际 %s", node.Address())
			}
		}
		node, done, err = routes[0].Target.Select(ctx)
		if err != nil {
			t.Fatal(err)
		}
		done(ctx, selector.DoneInfo{})
		if node.Address() != "127.0.0.1:18081" || len(mgr.Snapshot()) != 1 {
			t.Fatal("手写路由应继续使用未声明前缀的实例，并复用上游池")
		}
		ctx = route.WithExcluded(ctx, map[string]struct{}{"127.0.0.1:18082": {}})
		if _, _, err = routes[1].Target.Select(ctx); !errors.Is(err, selector.ErrNoAvailable) {
			t.Fatalf("排除唯一声明路由的实例后应返回无可用节点，实际 %v", err)
		}
	})
}

// TestBuildCatalogBalancesEachRoute 验证同一上游的目录路由独立轮询并跟随共享节点表更新。
func TestBuildCatalogBalancesEachRoute(t *testing.T) {
	const first, second = "127.0.0.1:18081", "127.0.0.1:18082"
	for _, tt := range []struct {
		lb   string
		want []string
	}{
		{lb: "round_robin", want: []string{first, second, first, second}},
		{lb: "wrr", want: []string{second, first, second, second}},
	} {
		t.Run(tt.lb, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				logger := slog.New(slog.DiscardHandler)
				disc := testutil.NewFakeDiscovery()
				mgr := upstream.NewManager(disc, "consul", logger)
				defer mgr.Stop()
				target := config.Target{Service: "demo", LB: tt.lb}
				routes, err := Build(&config.Config{}, mgr, []config.Route{
					{ID: "many", Predicates: []config.Predicate{{Path: "/many"}}, Target: target},
					{ID: "one", Predicates: []config.Predicate{{Path: "/one"}}, Target: target},
				}, nil, logger)
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err = routes[0].Target.Select(t.Context()); !errors.Is(err, selector.ErrNoAvailable) {
					t.Fatalf("首次发现前应返回无可用节点，实际 %v", err)
				}
				disc.Push("demo", []*registry.ServiceInstance{
					{Endpoints: []string{"http://" + first}, Metadata: map[string]string{catalog.RoutesMetaKey: "/many,/one", "weight": "1"}},
					{Endpoints: []string{"http://" + second}, Metadata: map[string]string{catalog.RoutesMetaKey: "/many", "weight": "3"}},
					{Endpoints: []string{"http://127.0.0.1:18083"}, Metadata: map[string]string{catalog.RoutesMetaKey: "/many,/one", catalog.LBMetaKey: "unsupported"}},
				})
				synctest.Wait()
				assertNode := func(r *route.Route, want string) {
					t.Helper()
					node, done, err := r.Target.Select(t.Context())
					if err != nil {
						t.Fatal(err)
					}
					done(t.Context(), selector.DoneInfo{})
					if node.Address() != want {
						t.Fatalf("路由 %s 选中 %s，期望 %s", r.ID, node.Address(), want)
					}
				}
				for _, want := range tt.want {
					assertNode(routes[0], want)
					assertNode(routes[1], first)
				}
				disc.Push("demo", []*registry.ServiceInstance{{
					Endpoints: []string{"http://" + second}, Metadata: map[string]string{catalog.RoutesMetaKey: "/many,/one"},
				}})
				synctest.Wait()
				assertNode(routes[0], second)
				assertNode(routes[1], second)
			})
		})
	}
}

// TestBuildWeightGroup 验证灰度组权重校验与区间分配。
func TestBuildWeightGroup(t *testing.T) {
	mgr := newManager(t)
	defer mgr.Stop()
	logger := slog.New(slog.DiscardHandler)

	// 权重之和 != 100 应报错
	bad := &config.Config{Routes: grayRoutes(80, 70)}
	if _, err := Build(bad, mgr, nil, nil, logger); err == nil {
		t.Error("权重之和不为 100 应报错")
	}

	inconsistent := &config.Config{Routes: grayRoutes(80, 20)}
	inconsistent.Routes[1].Predicates[1].Weight.Key = "X-User"
	if _, err := Build(inconsistent, mgr, nil, nil, logger); err == nil {
		t.Error("同一灰度组的哈希键不一致应报错")
	}

	good := &config.Config{Routes: grayRoutes(80, 20)}
	good.Routes[0].Predicates[1].Weight.Key = "X-User"
	good.Routes[1].Predicates[1].Weight.Key = "x-user"
	routes, err := Build(good, mgr, nil, nil, logger)
	if err != nil {
		t.Fatalf("装配失败: %v", err)
	}
	var stable, canary *route.WeightPredicate
	for _, r := range routes {
		for _, p := range r.Predicates {
			if wp, ok := p.(*route.WeightPredicate); ok {
				if r.ID == "gray-stable" {
					stable = wp
				} else {
					canary = wp
				}
			}
		}
	}
	if stable == nil || canary == nil {
		t.Fatal("灰度谓词未装配")
	}
	if stable.Start != 0 || stable.End != 80 {
		t.Errorf("稳定版区间错误: [%d,%d)", stable.Start, stable.End)
	}
	if canary.Start != 80 || canary.End != 100 {
		t.Errorf("灰度版区间错误: [%d,%d)", canary.Start, canary.End)
	}
}

// grayRoutes 构造灰度路由配置。
func grayRoutes(stable, canary int) []config.Route {
	return []config.Route{
		{
			ID:         "gray-stable",
			Predicates: []config.Predicate{{Path: "/api/v1/wf"}, {Weight: &config.Weight{Group: "g", Percent: stable}}},
			Target:     config.Target{Service: "wf-service"},
		},
		{
			ID:         "gray-canary",
			Predicates: []config.Predicate{{Path: "/api/v1/wf"}, {Weight: &config.Weight{Group: "g", Percent: canary}}},
			Target:     config.Target{Service: "wf-service", LB: "wrr"},
		},
	}
}
