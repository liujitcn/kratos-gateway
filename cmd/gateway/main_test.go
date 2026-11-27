package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-kratos/kratos/v3/registry"
	"github.com/go-kratos/kratos/v3/selector"
	consulapi "github.com/hashicorp/consul/api"

	"github.com/liujitcn/kratos-gateway/internal/config"
	"github.com/liujitcn/kratos-gateway/internal/route"
	"github.com/liujitcn/kratos-gateway/internal/testutil"
	"github.com/liujitcn/kratos-gateway/internal/upstream"
)

// TestMainRejectsInvalidInitialRoutes 验证不可装配的启动路由会导致进程退出。
func TestMainRejectsInvalidInitialRoutes(t *testing.T) {
	const conf = "server:\n  addr: 127.0.0.1:0\nroutes:\n  - id: missing-target\n    predicates:\n      - path: /demo\n"
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	output, err := gatewayProcess(t, ctx, conf).CombinedOutput()
	if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 || !strings.Contains(string(output), "missing-target") {
		t.Fatalf("无效路由应导致启动以状态码 1 退出，实际错误 %v，日志 %s", err, output)
	}
}

// TestMainDoesNotRegisterWhenListenFails 验证端口占用时进程退出且不写入注册中心。
func TestMainDoesNotRegisterWhenListenFails(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	var writes atomic.Int32
	consul := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/agent/service/") {
			writes.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(consul.Close)
	conf := "server:\n  addr: " + listener.Addr().String() + "\n  register: true\nregistry:\n  type: consul\n  consul:\n    address: " + consul.Listener.Addr().String() + "\n"
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var output []byte
	output, err = gatewayProcess(t, ctx, conf).CombinedOutput()
	if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 || !strings.Contains(string(output), "监听") {
		t.Fatalf("端口被占用时应因监听失败退出，实际错误 %v，日志 %s", err, output)
	}
	if got := writes.Load(); got != 0 {
		t.Fatalf("监听失败后应没有注册或注销请求，实际 %d 次", got)
	}
}

// TestGatewayProcess 是启动测试使用的子进程入口。
func TestGatewayProcess(t *testing.T) {
	if path := os.Getenv("GW_TEST_CONFIG"); path != "" {
		flag.CommandLine = flag.NewFlagSet("gateway", flag.ExitOnError)
		os.Args = []string{os.Args[0], "-conf", path}
		main()
	}
}

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
		updated, err := rebuildRoutes(cfg, nil, router, mgr, logger)
		if err != nil || !updated {
			t.Fatalf("应在首次装配时发布启动路由: %v", err)
		}
		disc.Push("old-service", []*registry.ServiceInstance{{
			ID: "old-1", Endpoints: []string{"http://127.0.0.1:18081"},
		}})
		synctest.Wait()
		old := router.Routes()[0]

		cfg.Routes[0].Target.LB = "unsupported"
		updated, err = rebuildRoutes(cfg, nil, router, mgr, logger)
		if err == nil || updated || router.Routes()[0] != old || len(mgr.Snapshot()) != 1 {
			t.Fatalf("装配失败应保留旧路由和订阅: %v", err)
		}
		cfg.Routes[0].Target.LB = ""
		cfg.Routes[0].Target.Service = "new-service"
		updated, err = rebuildRoutes(cfg, nil, router, mgr, logger)
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
		updated, err = rebuildRoutes(cfg, nil, router, mgr, logger)
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
		updated, err := rebuildRoutes(cfg, nil, router, mgr, logger)
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
		updated, err = rebuildRoutes(cfg, nil, router, mgr, logger)
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
		updated, err = rebuildRoutes(cfg, nil, router, mgr, logger)
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
		if updated, err := rebuildRoutes(cfg, nil, router, mgr, logger); err != nil || !updated {
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

// TestRunCatalogSkipsExcludedService 验证目录前缀之外的服务失败不会影响范围内的路由发布。
func TestRunCatalogSkipsExcludedService(t *testing.T) {
	var excluded atomic.Int32
	consul := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/catalog/services":
			_, _ = io.WriteString(w, `{"app-service":[],"infra-service":[]}`)
		case "/v1/health/service/app-service":
			_, _ = io.WriteString(w, `[{"Service":{"ID":"app","Service":"app-service","Meta":{"gateway_routes":"/app"}}}]`)
		default:
			excluded.Add(1)
			w.WriteHeader(http.StatusForbidden)
		}
	}))
	t.Cleanup(consul.Close)
	cfg := &config.Config{
		Registry: config.Registry{Type: "consul", Consul: &config.Consul{Address: consul.Listener.Addr().String()}},
		Catalog:  config.Catalog{ServicePrefixes: []string{"app-"}, PollInterval: config.Duration(10 * time.Millisecond)},
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	got := make(chan []config.Route, 1)
	go runCatalog(ctx, cfg, slog.New(slog.DiscardHandler), func(routes []config.Route) { got <- routes })
	select {
	case routes := <-got:
		if len(routes) != 1 || routes[0].Target.Service != "app-service" || excluded.Load() != 0 {
			t.Fatalf("应仅查询并发布 app-service，实际路由 %+v，范围外调用 %d", routes, excluded.Load())
		}
	case <-ctx.Done():
		t.Fatal("未发布匹配前缀的目录路由")
	}
}

// TestConsulWatchRecoversAndClearsNodes 验证首查失败可恢复，后续查询错误保留节点，空快照摘除节点。
func TestConsulWatchRecoversAndClearsNodes(t *testing.T) {
	var mode atomic.Int32
	failed := make(chan struct{}, 8)
	consul := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/health/service/demo-service" || r.URL.Query().Get("passing") != "1" {
			http.Error(w, "unexpected query", http.StatusBadRequest)
			return
		}
		index := r.URL.Query().Get("index")
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			current := mode.Load()
			if current == 0 {
				http.Error(w, "registry unavailable", http.StatusServiceUnavailable)
				select {
				case failed <- struct{}{}:
				default:
				}
				return
			}
			currentIndex := strconv.Itoa(int(current))
			if index != currentIndex {
				w.Header().Set("X-Consul-Index", currentIndex)
				if current == 1 {
					_, _ = io.WriteString(w, `[{"Node":{"Node":"test"},"Service":{"ID":"demo-1","Service":"demo-service","Meta":{"version":"v2"},"Tags":["version=old"],"TaggedAddresses":{"http":{"Address":"http://127.0.0.1:18081","Port":18081}}}}]`)
				} else {
					_, _ = io.WriteString(w, "[]")
				}
				return
			}
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
			}
		}
	}))
	t.Cleanup(consul.Close)
	disc, err := newDiscovery(&config.Registry{Type: "consul", Consul: &config.Consul{Address: consul.URL}})
	if err != nil {
		t.Fatal(err)
	}
	mgr := upstream.NewManager(disc, "consul", slog.New(slog.DiscardHandler))
	t.Cleanup(mgr.Stop)
	u, err := mgr.Get(upstream.UpstreamConfig{Service: "demo-service", Scheme: "http", LB: "round_robin", Version: "v2"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-failed:
	case <-time.After(time.Second):
		t.Fatal("订阅未执行首次查询")
	}
	mode.Store(1)
	waitForConsulNodes(t, u, 1)

	mode.Store(0)
	for range 2 {
		select {
		case <-failed:
		case <-time.After(4 * time.Second):
			t.Fatal("订阅未重试失败的查询")
		}
	}
	var node selector.Node
	var done selector.DoneFunc
	node, done, err = u.Select(t.Context())
	if err != nil {
		t.Fatal("查询失败期间应保留健康快照", err)
	}
	done(t.Context(), selector.DoneInfo{})
	if node.Address() != "127.0.0.1:18081" {
		t.Fatalf("健康实例地址错误: %s", node.Address())
	}

	mode.Store(2)
	waitForConsulNodes(t, u, 0)
	if _, _, err = u.Select(t.Context()); !errors.Is(err, selector.ErrNoAvailable) {
		t.Fatalf("成功的空快照应清空选择器，实际 %v", err)
	}
}

// TestConsulRegisterReadinessCheck 验证启用 Consul 健康检查时同时登记就绪探针。
func TestConsulRegisterReadinessCheck(t *testing.T) {
	registered := make(chan consulapi.AgentServiceRegistration, 1)
	consul := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/agent/service/register" {
			var service consulapi.AgentServiceRegistration
			if err := json.NewDecoder(r.Body).Decode(&service); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			registered <- service
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(consul.Close)
	cfg := &config.Config{
		Server:   config.Server{Register: true},
		Registry: config.Registry{Type: "consul", Consul: &config.Consul{Address: consul.URL, HealthCheck: true}},
	}
	deregister := registerSelf(cfg, 8080, slog.New(slog.DiscardHandler))
	if deregister == nil {
		t.Fatal("网关自注册失败")
	}
	defer deregister()
	select {
	case service := <-registered:
		for _, check := range service.Checks {
			if check.HTTP == "http://"+net.JoinHostPort(service.Address, "8080")+"/readyz" && check.Status == "critical" {
				return
			}
		}
		t.Fatal("Consul 注册缺少初始为 critical 的 HTTP 就绪探针")
	default:
		t.Fatal("未收到 Consul 注册请求")
	}
}

// TestConsulDeregisterTimeout 验证 Consul 注销请求在 HTTP 对端无响应时有实际期限。
func TestConsulDeregisterTimeout(t *testing.T) {
	requested := make(chan struct{}, 1)
	release := make(chan struct{})
	consul := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/agent/service/deregister/") {
			select {
			case requested <- struct{}{}:
			default:
			}
			select {
			case <-r.Context().Done():
			case <-release:
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(consul.Close)
	defer close(release)

	cfg := &config.Config{
		Server:   config.Server{Addr: ":8080", Register: true, ShutdownTimeout: config.Duration(100 * time.Millisecond)},
		Registry: config.Registry{Type: "consul", Consul: &config.Consul{Address: consul.URL}},
	}
	deregister := registerSelf(cfg, 8080, slog.New(slog.DiscardHandler))
	if deregister == nil {
		t.Fatal("网关自注册失败")
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		deregister()
	}()
	select {
	case <-requested:
	case <-time.After(time.Second):
		t.Fatal("网关未发送 Consul 注销请求")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Consul 无响应时注销未在期限内退出")
	}
}

// gatewayProcess 为指定配置创建可以运行网关入口的测试子进程。
func gatewayProcess(t *testing.T, ctx context.Context, conf string) *exec.Cmd {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestGatewayProcess$")
	cmd.Env = append(os.Environ(), "GW_TEST_CONFIG="+path)
	return cmd
}

// waitForConsulNodes 等待订阅发布指定数量的健康实例。
func waitForConsulNodes(t *testing.T, u *upstream.Upstream, count int) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for {
		if len(u.Snapshot().Nodes) == count {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待节点数量更新为 %d 超时: %+v", count, u.Snapshot())
		}
		time.Sleep(5 * time.Millisecond)
	}
}
