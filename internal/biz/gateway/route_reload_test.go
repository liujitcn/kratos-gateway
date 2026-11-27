package gateway

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-kratos/kratos/v3/registry"
	"github.com/go-kratos/kratos/v3/selector"

	"github.com/liujitcn/kratos-gateway/internal/biz/gateway/route"
	"github.com/liujitcn/kratos-gateway/internal/config"
	"github.com/liujitcn/kratos-gateway/internal/data/testutil"
	"github.com/liujitcn/kratos-gateway/internal/data/upstream"
)

// TestRebuildRoutesWaitsForDiscovery 验证热更新等待新上游的首次成功快照，随后回收旧上游。
func TestRebuildRoutesWaitsForDiscovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := slog.New(slog.DiscardHandler)
		disc := testutil.NewFakeDiscovery()
		mgr := upstream.NewManager(disc, "consul", logger)
		defer mgr.Stop()
		router := route.NewRouter()
		cfg := &config.Config{Routes: []config.Route{{
			ID:         "demo",
			Predicates: []config.Predicate{{Path: "/demo"}},
			Target:     config.Target{Service: "old-service"},
		}}}
		updated, err := RebuildRoutes(cfg, nil, router, mgr, logger)
		if err != nil || !updated {
			t.Fatalf("应在首次装配时发布启动路由: %v", err)
		}
		disc.Push("old-service", []*registry.ServiceInstance{{
			ID: "old-1", Endpoints: []string{"http://127.0.0.1:18081"},
		}})
		synctest.Wait()
		old := router.Routes()[0]

		cfg.Routes[0].Target.LB = "unsupported"
		updated, err = RebuildRoutes(cfg, nil, router, mgr, logger)
		if err == nil || updated || router.Routes()[0] != old || len(mgr.Snapshot()) != 1 {
			t.Fatalf("装配失败应保留旧路由和订阅: %v", err)
		}
		cfg.Routes[0].Target.LB = ""
		cfg.Routes[0].Target.Service = "new-service"
		updated, err = RebuildRoutes(cfg, nil, router, mgr, logger)
		if err != nil || updated {
			t.Fatalf("新上游没有快照时应等待: %v", err)
		}
		if router.Routes()[0] != old || len(mgr.Snapshot()) != 2 {
			t.Fatal("等待期间应保留旧路由表及两个上游订阅")
		}
		disc.Push("old-service", []*registry.ServiceInstance{{
			ID: "old-2", Endpoints: []string{"http://127.0.0.1:18082"},
		}})
		synctest.Wait()
		var node selector.Node
		var done selector.DoneFunc
		node, done, err = old.Target.Select(t.Context())
		if err != nil || node.Address() != "127.0.0.1:18082" {
			t.Fatalf("等待期间旧路由应继续接收健康实例，实际 %v，错误 %v", node, err)
		}
		done(t.Context(), selector.DoneInfo{})

		disc.Push("new-service", nil)
		synctest.Wait()
		updated, err = RebuildRoutes(cfg, nil, router, mgr, logger)
		if err != nil || !updated {
			t.Fatalf("成功的空快照也应结束等待: %v", err)
		}
		if router.Routes()[0] == old {
			t.Fatal("收到首次成功快照后应替换路由表")
		}
		if stats := mgr.Snapshot(); len(stats) != 1 || stats[0].Service != "new-service" {
			t.Fatalf("发布后只应保留新上游，实际 %+v", stats)
		}
	})
}

// TestRebuildRoutesUpdatesBreaker 验证有效热更新调整共享熔断阈值和冷却期。
func TestRebuildRoutesUpdatesBreaker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := slog.New(slog.DiscardHandler)
		disc := testutil.NewFakeDiscovery()
		mgr := upstream.NewManager(disc, "consul", logger)
		defer mgr.Stop()
		router := route.NewRouter()
		cfg := &config.Config{Routes: []config.Route{
			{ID: "api", Predicates: []config.Predicate{{Path: "/api"}}, Target: config.Target{Service: "demo"}},
			{ID: "events", Predicates: []config.Predicate{{Path: "/events"}}, Target: config.Target{
				Service: "demo", Breaker: &config.Breaker{FailureThreshold: 4, OpenDuration: config.Duration(time.Hour)},
			}},
		}}
		updated, err := RebuildRoutes(cfg, nil, router, mgr, logger)
		if err != nil || !updated {
			t.Fatalf("首次装配失败: %v", err)
		}
		disc.Push("demo", []*registry.ServiceInstance{{Endpoints: []string{"http://127.0.0.1:18081"}}})
		synctest.Wait()
		fail := func(target route.Target) {
			t.Helper()
			_, done, err := target.Select(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			done(t.Context(), selector.DoneInfo{Err: io.ErrUnexpectedEOF})
		}
		oldRoutes := router.Routes()
		cfg.Routes[1].Target.Breaker = &config.Breaker{FailureThreshold: 2, OpenDuration: config.Duration(time.Second)}
		cfg.Routes = append(cfg.Routes, config.Route{ID: "invalid", Predicates: []config.Predicate{{Path: "/invalid"}}})
		updated, err = RebuildRoutes(cfg, nil, router, mgr, logger)
		if err == nil || updated || router.Routes()[0] != oldRoutes[0] {
			t.Fatalf("装配失败时应保留旧表: %v", err)
		}
		fail(oldRoutes[0].Target)
		fail(oldRoutes[1].Target)
		var done selector.DoneFunc
		_, done, err = oldRoutes[0].Target.Select(t.Context())
		if err != nil {
			t.Fatalf("装配失败不应应用新阈值: %v", err)
		}
		done(t.Context(), selector.DoneInfo{})
		fail(oldRoutes[0].Target)

		cfg.Routes = cfg.Routes[:2]
		updated, err = RebuildRoutes(cfg, nil, router, mgr, logger)
		if err != nil || !updated || len(mgr.Snapshot()) != 1 {
			t.Fatalf("应复用池并更新熔断配置: %v", err)
		}
		routes := router.Routes()
		fail(routes[0].Target)
		if _, _, err = routes[1].Target.Select(t.Context()); !errors.Is(err, selector.ErrNoAvailable) {
			t.Fatalf("热更新后第二次失败应触发共享熔断: %v", err)
		}
		time.Sleep(time.Second)
		_, done, err = routes[1].Target.Select(t.Context())
		if err != nil {
			t.Fatalf("新冷却期到期后应允许探测: %v", err)
		}
		done(t.Context(), selector.DoneInfo{})
	})
}

// TestRebuildRoutesKeepsRateLimit 验证无关配置热更新后原路由的限流桶仍生效。
func TestRebuildRoutesKeepsRateLimit(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(up.Close)
	logger := slog.New(slog.DiscardHandler)
	mgr := upstream.NewManager(nil, "", logger)
	defer mgr.Stop()
	router := route.NewRouter()
	target := config.Target{URLs: []string{up.URL}}
	cfg := &config.Config{Routes: []config.Route{{
		ID: "limited", Predicates: []config.Predicate{{Path: "/limited"}}, Target: target,
		Filters: []config.Filter{{Name: "rate-limit", Args: map[string]any{"rate": 0.001, "burst": 1}}},
	}}}
	rebuild := func() {
		t.Helper()
		if updated, err := RebuildRoutes(cfg, nil, router, mgr, logger); err != nil || !updated {
			t.Fatalf("装配失败: %v", err)
		}
	}
	check := func(path string, want int) {
		t.Helper()
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
		if w.Code != want {
			t.Fatalf("请求 %s 返回 %d，期望 %d", path, w.Code, want)
		}
	}
	rebuild()
	check("/limited", http.StatusOK)
	check("/limited", http.StatusTooManyRequests)
	cfg.Routes = append(cfg.Routes, config.Route{ID: "new", Predicates: []config.Predicate{{Path: "/new"}}, Target: target})
	rebuild()
	check("/new", http.StatusOK)
	check("/limited", http.StatusTooManyRequests)
}
