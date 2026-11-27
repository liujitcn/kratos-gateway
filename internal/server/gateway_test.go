package server

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v3/registry"
	configv1 "github.com/liujitcn/kratos-kit/api/gen/go/config/v1"
	"github.com/liujitcn/kratos-kit/bootstrap"

	"github.com/liujitcn/kratos-gateway/internal/biz/gateway"
	"github.com/liujitcn/kratos-gateway/internal/biz/gateway/route"
	"github.com/liujitcn/kratos-gateway/internal/config"
	"github.com/liujitcn/kratos-gateway/internal/data/testutil"
	"github.com/liujitcn/kratos-gateway/internal/data/upstream"
)

// upstreamServer 启动计数回显上游实例。
func upstreamServer(t *testing.T, name string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		fmt.Fprintf(w, "%s %s", name, r.URL.Path)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

// gatewayStack 组装完整网关栈（fake 注册中心 + 路由 + 服务）并推送上游实例，返回网关测试服务。
func gatewayStack(t *testing.T, routes []config.Route, ups ...*httptest.Server) (*httptest.Server, *Server, *upstream.Manager, *testutil.FakeDiscovery) {
	t.Helper()
	gw, mgr, disc := gatewayHandler(t, routes, ups...)
	gatewaySrv := httptest.NewUnstartedServer(testHTTPServer(t, gw))
	gatewaySrv.Config.ConnContext = ConnContext
	gatewaySrv.Start()
	t.Cleanup(gatewaySrv.Close)
	return gatewaySrv, gw, mgr, disc
}

// gatewayHandler 装配路由并等待服务发现节点就绪。
func gatewayHandler(t *testing.T, routes []config.Route, ups ...*httptest.Server) (*Server, *upstream.Manager, *testutil.FakeDiscovery) {
	t.Helper()
	disc := testutil.NewFakeDiscovery()
	mgr := upstream.NewManager(disc, "consul", slog.New(slog.DiscardHandler))
	t.Cleanup(mgr.Stop)

	logger := slog.New(slog.DiscardHandler)
	built, err := gateway.Build(&config.Config{Routes: routes}, mgr, nil, nil, logger)
	if err != nil {
		t.Fatalf("装配路由失败: %v", err)
	}
	router := route.NewRouter()
	router.Update(built)

	cfg := &config.Config{}
	gw := New(cfg, router, mgr, logger)
	// 推送上游实例（服务名 demo-service 已经 NormalizeForRegistry 规范化）
	ins := make([]*registry.ServiceInstance, 0, len(ups))
	for _, up := range ups {
		ins = append(ins, &registry.ServiceInstance{
			ID:        up.URL,
			Name:      "demo-service",
			Endpoints: []string{up.URL},
		})
	}
	disc.Push("demo-service", ins)

	// 等待节点表就绪
	deadline := time.Now().Add(2 * time.Second)
	for {
		ready := false
		for _, stat := range mgr.Snapshot() {
			if len(stat.Nodes) == len(ups) {
				ready = true
			}
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待上游节点就绪超时: %+v", mgr.Snapshot())
		}
		time.Sleep(5 * time.Millisecond)
	}
	return gw, mgr, disc
}

// listenGateway 在临时端口启动由 Stop 管理的网关 HTTP 服务。
func listenGateway(t *testing.T, gw *Server) (string, <-chan struct{}) {
	t.Helper()
	testHTTPServer(t, gw)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if serveErr := gw.http.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			t.Error(serveErr)
		}
	}()
	t.Cleanup(func() {
		if closeErr := gw.http.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		<-done
	})
	return ln.Addr().String(), done
}

// testHTTPServer 将网关装到启用共享恢复中间件的 HTTP 服务供集成测试使用。
func testHTTPServer(t *testing.T, gw *Server) *HTTPServer {
	t.Helper()
	cfg := &configv1.Bootstrap{Server: &configv1.Server{Http: &configv1.Server_Http{
		Middleware: &configv1.Server_Middleware{EnableRecovery: true},
	}}}
	ctx := bootstrap.NewContextWithParam(t.Context(), nil, cfg, gw.logger)
	srv, err := NewHTTPServer(ctx, gw)
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

// TestIntegrationRoundRobin 验证 v0.1 验收场景：双实例 20 请求 10/10 均分、X-GW-Upstream、404。
func TestIntegrationRoundRobin(t *testing.T) {
	up1, hits1 := upstreamServer(t, "inst-1")
	up2, hits2 := upstreamServer(t, "inst-2")
	gw, _, _, _ := gatewayStack(t, []config.Route{
		{
			ID:         "demo",
			Predicates: []config.Predicate{{Path: "/api/v1/base"}},
			Target:     config.Target{Service: "demo-service"},
		},
	}, up1, up2)

	client := &http.Client{Timeout: 5 * time.Second}
	upstreams := map[string]int{}
	for i := 0; i < 20; i++ {
		resp, err := client.Get(gw.URL + "/api/v1/base/captcha")
		if err != nil {
			t.Fatalf("请求失败: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("请求应 200: %d", resp.StatusCode)
		}
		if !strings.HasPrefix(string(body), "inst-") || !strings.HasSuffix(string(body), "/api/v1/base/captcha") {
			t.Errorf("转发内容错误: %s", body)
		}
		upstreams[resp.Header.Get("X-GW-Upstream")]++
	}
	if hits1.Load() != 10 || hits2.Load() != 10 {
		t.Errorf("双实例应 10/10 均分: %d/%d", hits1.Load(), hits2.Load())
	}
	if len(upstreams) != 2 {
		t.Errorf("应命中两个上游节点: %v", upstreams)
	}

	// 未匹配路由 404
	resp, err := client.Get(gw.URL + "/api/v1/none")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("未匹配应 404: %d", resp.StatusCode)
	}
}

// TestIntegrationStats 验证 /gw-stats 与 /healthz 端点。
func TestIntegrationStats(t *testing.T) {
	up1, _ := upstreamServer(t, "inst-1")
	up2, _ := upstreamServer(t, "inst-2")
	gws, _, _ := gatewayHandler(t, []config.Route{
		{
			ID:         "demo",
			Predicates: []config.Predicate{{Path: "/api/v1/base"}},
			Target:     config.Target{Service: "demo-service"},
		},
	}, up1, up2)
	gws.cfg.Server.Stats = new(true)
	gw := httptest.NewServer(gws)
	t.Cleanup(gw.Close)

	client := &http.Client{Timeout: 5 * time.Second}
	for i := 0; i < 4; i++ {
		resp, err := client.Get(gw.URL + "/api/v1/base/x")
		if err != nil {
			t.Fatalf("请求失败: %v", err)
		}
		_ = resp.Body.Close()
	}

	resp, err := client.Get(gw.URL + "/gw-stats")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()
	var stats struct {
		Routes []struct {
			ID     string `json:"id"`
			Path   string `json:"path"`
			Target string `json:"target"`
		} `json:"routes"`
		Upstreams []struct {
			Service string `json:"Service"`
			Nodes   []struct {
				Address string `json:"Address"`
				Hits    int64  `json:"Hits"`
			} `json:"Nodes"`
		} `json:"upstreams"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("解析 /gw-stats 失败: %v", err)
	}
	if len(stats.Routes) != 1 || stats.Routes[0].ID != "demo" || stats.Routes[0].Path != "/api/v1/base" {
		t.Errorf("路由展示错误: %+v", stats.Routes)
	}
	found := 0
	var totalHits int64
	for _, up := range stats.Upstreams {
		for _, n := range up.Nodes {
			totalHits += n.Hits
			found++
		}
	}
	if found != 2 || totalHits != 4 {
		t.Errorf("节点命中统计错误: 节点数=%d 总命中=%d", found, totalHits)
	}

	// /healthz
	resp, err = client.Get(gw.URL + "/healthz")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("healthz 应 200: %d", resp.StatusCode)
	}
}

// TestIntegrationSSE 验证 SSE 流式透传：首块到达时间显著早于完整响应（FlushInterval=-1 生效）。
func TestIntegrationSSE(t *testing.T) {
	sse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: first\n\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(500 * time.Millisecond)
		fmt.Fprint(w, "data: second\n\n")
	}))
	t.Cleanup(sse.Close)

	gw, _, _, _ := gatewayStack(t, []config.Route{
		{
			ID:         "events",
			Predicates: []config.Predicate{{Path: "/events"}},
			Target:     config.Target{Service: "demo-service"},
		},
	}, sse)

	start := time.Now()
	resp, err := http.Get(gw.URL + "/events")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 64)
	n, readErr := resp.Body.Read(buf)
	firstElapsed := time.Since(start)
	if readErr != nil && readErr != io.EOF {
		t.Fatalf("读取首块失败: %v", readErr)
	}
	if !strings.Contains(string(buf[:n]), "first") {
		t.Fatalf("首块内容错误: %q", buf[:n])
	}
	// 首块应在第二块写入（500ms）之前到达；容忍 150ms 调度余量
	if firstElapsed > 350*time.Millisecond {
		t.Errorf("SSE 首块应即时透传，实际 %s 后到达（FlushInterval 未生效？）", firstElapsed)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "second") {
		t.Error("完整流内容缺失")
	}
}

// TestIntegrationStreamingProbe 验证 SSE 响应头就绪后，同一节点可接收其他路由的请求。
func TestIntegrationStreamingProbe(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/fail":
			panic(http.ErrAbortHandler)
		case "/events":
			w.Header().Set("Content-Type", "text/event-stream")
			if _, err := io.WriteString(w, "data: ready\n\n"); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(up.Close)
	gw, _, _, _ := gatewayStack(t, []config.Route{
		{ID: "api", Predicates: []config.Predicate{{Path: "/api"}}, Target: config.Target{
			Service: "demo-service", Breaker: &config.Breaker{FailureThreshold: 1}, MaxRetry: new(0),
		}},
		{ID: "events", Predicates: []config.Predicate{{Path: "/events"}}, Target: config.Target{Service: "demo-service"}},
	}, up)
	client := gw.Client()
	client.Timeout = 2 * time.Second
	failure, err := client.Get(gw.URL + "/api/fail")
	if err != nil {
		t.Fatal(err)
	}
	failure.Body.Close()
	if failure.StatusCode != http.StatusBadGateway {
		t.Fatalf("首个请求应触发上游故障，实际 %d", failure.StatusCode)
	}
	var stream *http.Response
	stream, err = client.Get(gw.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Body.Close()
	if stream.StatusCode != http.StatusOK {
		t.Fatalf("半开 SSE 探测应成功，实际 %d", stream.StatusCode)
	}
	var response *http.Response
	response, err = client.Get(gw.URL + "/api")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("SSE 仍打开时普通 API 应恢复，实际 %d", response.StatusCode)
	}
}

// TestIntegrationRouteFilters 验证路由级过滤器链（rewrite + add-header）端到端生效。
func TestIntegrationRouteFilters(t *testing.T) {
	var gotPath, gotURI, gotMark atomic.Value
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath.Store(r.URL.Path)
		gotURI.Store(r.RequestURI)
		gotMark.Store(r.Header.Get("X-Mark"))
	}))
	t.Cleanup(up.Close)

	gw, _, _, _ := gatewayStack(t, []config.Route{
		{
			ID:         "rewritten",
			Predicates: []config.Predicate{{Path: "/api/v1/bi"}},
			Filters: []config.Filter{
				{Name: "rewrite", Args: map[string]any{"prefix": "/api/v1/bi", "replacement": "/internal/bi"}},
				{Name: "add-header", Args: map[string]any{"key": "X-Mark", "value": "gateway"}},
			},
			Target: config.Target{Service: "demo-service"},
		},
	}, up)

	resp, err := http.Get(gw.URL + "/%61pi/v1/bi/report%2fmay?key=1")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("应 200: %d", resp.StatusCode)
	}
	if gotPath.Load() != "/internal/bi/report/may" {
		t.Errorf("路径改写未生效: %v", gotPath.Load())
	}
	if gotURI.Load() != "/internal/bi/report%2fmay?key=1" {
		t.Errorf("路径改写未保留后缀转义和查询参数: %v", gotURI.Load())
	}
	if gotMark.Load() != "gateway" {
		t.Errorf("请求头注入未生效: %v", gotMark.Load())
	}
}

// TestIntegrationPreservesRawPath 验证共享 HTTP 路由不重定向或改写网关的原始路径。
func TestIntegrationPreservesRawPath(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.RequestURI)
	}))
	t.Cleanup(up.Close)
	gw, _, _, _ := gatewayStack(t, []config.Route{
		{ID: "demo", Predicates: []config.Predicate{{Path: "/"}}, Target: config.Target{Service: "demo-service"}},
	}, up)
	client := &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	for _, uri := range []string{"/", "/a/b/", "/a//b?key=1", "/a/./b", "/a/../b", "/a/%2f/b", "/a/%2e%2e/b", "/a/%0Ab"} {
		t.Run(uri, func(t *testing.T) {
			resp, err := client.Get(gw.URL + uri)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			var body []byte
			body, err = io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK || string(body) != uri {
				t.Fatalf("上游应收到 %q，实际状态 %d，响应 %q", uri, resp.StatusCode, body)
			}
			if resp.Header.Get("X-Request-ID") == "" {
				t.Fatal("未执行共享请求 ID 中间件")
			}
		})
	}
}

// TestIntegrationStatsDisabled 验证 /gw-stats 缺省关闭，显式关闭时也返回 404。
func TestIntegrationStatsDisabled(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	mgr := upstream.NewManager(nil, "", logger)
	for _, stats := range []*bool{nil, new(false)} {
		cfg := &config.Config{Server: config.Server{Stats: stats}}
		gw := New(cfg, route.NewRouter(), mgr, logger)
		w := httptest.NewRecorder()
		gw.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/gw-stats", nil))
		if w.Code != http.StatusNotFound {
			t.Errorf("关闭后 /gw-stats 应 404，实际 %d", w.Code)
		}
	}
}

// TestIntegrationMetrics 验证 /metrics 按路由记录请求，并归并扩展 HTTP 方法。
func TestIntegrationMetrics(t *testing.T) {
	up, _ := upstreamServer(t, "inst-1")
	gw, _, _, _ := gatewayStack(t, []config.Route{
		{ID: "demo", Predicates: []config.Predicate{{Path: "/a"}}, Target: config.Target{Service: "demo-service"}},
	}, up)

	resp, err := http.Get(gw.URL + "/a/x")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	_ = resp.Body.Close()

	for _, method := range []string{"EXTENSION-A", "EXTENSION-B"} {
		var req *http.Request
		req, err = http.NewRequestWithContext(t.Context(), method, gw.URL+"/a/x", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err = gw.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}

	resp, err = http.Get(gw.URL + "/metrics")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "gateway_requests_total") {
		t.Error("指标应包含 gateway_requests_total")
	}
	if !strings.Contains(string(body), `route="demo"`) {
		t.Errorf("指标应按路由维度打标: %s", body[:min(len(body), 200)])
	}
	if strings.Contains(string(body), `method="EXTENSION-`) || !strings.Contains(string(body), `gateway_requests_total{code="200",method="OTHER",route="demo"}`) {
		t.Error("扩展 HTTP 方法应使用同一个 OTHER 标签")
	}
}

// TestIntegrationSticky 验证同一连接按上游池固定节点，节点下线时自动切换。
func TestIntegrationSticky(t *testing.T) {
	up1, _ := upstreamServer(t, "inst-1")
	up2, _ := upstreamServer(t, "inst-2")
	static1, _ := upstreamServer(t, "static-1")
	static2, _ := upstreamServer(t, "static-2")
	gw, _, mgr, disc := gatewayStack(t, []config.Route{
		{
			ID:         "sticky-events",
			Predicates: []config.Predicate{{Path: "/events"}},
			Target:     config.Target{Service: "demo-service", Sticky: true},
		},
		{
			ID:         "plain",
			Predicates: []config.Predicate{{Path: "/plain"}},
			Target:     config.Target{Service: "demo-service"},
		},
		{
			ID:         "sticky-static",
			Predicates: []config.Predicate{{Path: "/static"}},
			Target:     config.Target{URLs: []string{static1.URL, static2.URL}, Sticky: true},
		},
	}, up1, up2)

	// 单连接客户端（keep-alive 复用同一 TCP 连接；需读尽 body 才能复用）
	client := &http.Client{Timeout: 5 * time.Second}

	// 对照：非粘性路由同连接轮询交替命中两节点
	seen := map[string]int{}
	for i := 0; i < 4; i++ {
		resp, err := client.Get(gw.URL + "/plain/x")
		if err != nil {
			t.Fatalf("请求失败: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		seen[resp.Header.Get("X-GW-Upstream")]++
	}
	if len(seen) != 2 {
		t.Errorf("非粘性路由应轮询双节点: %v", seen)
	}

	// 同一连接交替访问两个上游池，各自的节点绑定应独立。
	bound := map[string]string{}
	for i := 0; i < 6; i++ {
		for _, path := range []string{"/events/stream", "/static/x"} {
			resp, err := client.Get(gw.URL + path)
			if err != nil {
				t.Fatalf("请求失败: %v", err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			node := resp.Header.Get("X-GW-Upstream")
			if resp.StatusCode != http.StatusOK || node == "" {
				t.Fatalf("路由 %s 转发失败，状态码 %d，节点 %q", path, resp.StatusCode, node)
			}
			if bound[path] == "" {
				bound[path] = node
			} else if node != bound[path] {
				t.Errorf("路由 %s 应固定节点 %s，实际 %s", path, bound[path], node)
			}
		}
	}

	// 粘住的节点下线（推送单实例列表）→ 粘性失效自动切换到剩余节点
	stickyNode := bound["/events/stream"]
	remaining := up2
	if stickyNode == hostPortOf(t, up2) {
		remaining = up1
	}
	disc.Push("demo-service", []*registry.ServiceInstance{
		{ID: remaining.URL, Name: "demo-service", Endpoints: []string{remaining.URL}},
	})
	deadline := time.Now().Add(2 * time.Second)
	for {
		if len(mgr.Snapshot()[0].Nodes) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待节点表更新超时: %+v", mgr.Snapshot())
		}
		time.Sleep(5 * time.Millisecond)
	}
	resp, err := client.Get(gw.URL + "/events/stream")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	switched := resp.Header.Get("X-GW-Upstream")
	if switched == stickyNode || switched == "" {
		t.Errorf("粘住的节点下线后应切换到剩余节点: 旧=%s 新=%s", stickyNode, switched)
	}
}

// hostPortOf 提取 httptest.Server 的 host:port。
func hostPortOf(_ *testing.T, srv *httptest.Server) string {
	return srv.URL[len("http://"):]
}

// TestIntegrationReadiness 验证就绪探针在发现匹配的上游后放行，并在节点消失时拒绝接流。
func TestIntegrationReadiness(t *testing.T) {
	up, _ := upstreamServer(t, "healthy")
	gw, _, disc := gatewayHandler(t, []config.Route{
		{ID: "demo", Predicates: []config.Predicate{{Path: "/a"}}, Target: config.Target{Service: "demo-service", Version: "v2"}},
	})
	probe := func(path string) int {
		w := httptest.NewRecorder()
		gw.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		return w.Code
	}
	if got := probe("/healthz"); got != http.StatusOK {
		t.Fatalf("等待上游时进程仍应存活: %d", got)
	}
	if got := probe("/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("没有匹配上游时应返回 503: %d", got)
	}
	for _, version := range []string{"v2", "v1"} {
		disc.Push("demo-service", []*registry.ServiceInstance{{ID: up.URL, Version: version, Endpoints: []string{up.URL}}})
		want := http.StatusServiceUnavailable
		if version == "v2" {
			want = http.StatusOK
		}
		deadline := time.Now().Add(time.Second)
		for probe("/readyz") != want {
			if time.Now().After(deadline) {
				t.Fatalf("版本 %s 的就绪状态应为 %d", version, want)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// TestReadinessCatalogAndDrain 验证目录首个快照发布前和开始排空后均不接流。
func TestReadinessCatalogAndDrain(t *testing.T) {
	gw := New(&config.Config{Catalog: config.Catalog{Enabled: true}}, route.NewRouter(), nil, slog.New(slog.DiscardHandler))
	check := func(path string, want int) {
		t.Helper()
		w := httptest.NewRecorder()
		gw.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != want {
			t.Fatalf("%s 状态为 %d，期望 %d", path, w.Code, want)
		}
	}
	check("/readyz", http.StatusServiceUnavailable)
	gw.MarkCatalogReady()
	check("/readyz", http.StatusOK)
	gw.BeginDrain()
	check("/readyz", http.StatusServiceUnavailable)
	check("/healthz", http.StatusOK)
}

// TestIntegrationGracefulShutdown 验证实际网关监听器在停机时等待在途 HTTP 请求完成。
func TestIntegrationGracefulShutdown(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		fmt.Fprint(w, "done")
	}))
	t.Cleanup(slow.Close)

	gw, _, _ := gatewayHandler(t, []config.Route{
		{ID: "demo", Predicates: []config.Predicate{{Path: "/a"}}, Target: config.Target{Service: "demo-service"}},
	}, slow)
	gw.cfg.Server.ShutdownTimeout = config.Duration(2 * time.Second)
	host, listenerStopped := listenGateway(t, gw)
	releaseRequest := sync.OnceFunc(func() { close(release) })
	defer releaseRequest()

	type result struct {
		body string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := (&http.Client{Timeout: 3 * time.Second}).Get("http://" + host + "/a/slow")
		if err != nil {
			done <- result{err: err}
			return
		}
		var body []byte
		body, err = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		done <- result{body: string(body), err: err}
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("请求未到达上游")
	}
	stopped := make(chan error, 1)
	go func() { stopped <- gw.Stop() }()
	select {
	case <-listenerStopped:
	case <-time.After(time.Second):
		t.Fatal("停机未关闭网关监听器")
	}
	select {
	case <-stopped:
		t.Fatal("在途请求尚未完成时停机已返回")
	default:
	}
	releaseRequest()

	select {
	case r := <-done:
		if r.err != nil {
			t.Errorf("在途请求不应失败: %v", r.err)
		} else if r.body != "done" {
			t.Errorf("在途请求响应错误: %q", r.body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("在途请求未在停机排空内完成")
	}
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("请求完成后网关未退出")
	}
}

// TestIntegrationWebSocket 验证 WebSocket 升级透传、自定义响应头及半开探测恢复。
func TestIntegrationWebSocket(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ws/fail":
			panic(http.ErrAbortHandler)
		case "/ws/plain":
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Header.Get("Upgrade") != "websocket" {
			http.Error(w, "not websocket", http.StatusBadRequest)
			return
		}
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		// 手工回写 101 升级响应并回显后续字节（模拟 WS 双向流）
		if _, err := conn.Write([]byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nX-WS-Policy: upstream\r\n\r\n")); err != nil {
			return
		}
		echo := make([]byte, 32)
		n, err := buf.Read(echo)
		if err != nil {
			return
		}
		_, _ = conn.Write(echo[:n])
	}))
	t.Cleanup(up.Close)

	gw, _, _, _ := gatewayStack(t, []config.Route{{
		ID: "ws", Predicates: []config.Predicate{{Path: "/ws"}},
		Filters: []config.Filter{{Name: "response-header", Args: map[string]any{"key": "X-WS-Policy", "value": "gateway"}}},
		Target:  config.Target{Service: "demo-service", Breaker: &config.Breaker{FailureThreshold: 1}, MaxRetry: new(0)},
	}}, up)
	client := gw.Client()
	client.Timeout = 2 * time.Second
	failure, err := client.Get(gw.URL + "/ws/fail")
	if err != nil {
		t.Fatal(err)
	}
	failure.Body.Close()
	if failure.StatusCode != http.StatusBadGateway {
		t.Fatalf("首个请求应触发上游故障，实际 %d", failure.StatusCode)
	}

	host := strings.TrimPrefix(gw.URL, "http://")
	var conn net.Conn
	conn, err = net.DialTimeout("tcp", host, 2*time.Second)
	if err != nil {
		t.Fatalf("连接网关失败: %v", err)
	}
	defer conn.Close()
	if err = conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("GET /ws HTTP/1.1\r\nHost: " + host + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")); err != nil {
		t.Fatalf("发送升级请求失败: %v", err)
	}

	reader := bufio.NewReader(conn)
	var resp *http.Response
	resp, err = http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatalf("读取升级响应失败: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("应收到 101 升级响应: %d", resp.StatusCode)
	}
	if got := resp.Header.Values("X-WS-Policy"); len(got) != 1 || got[0] != "gateway" {
		t.Errorf("101 响应应覆盖配置头，实际 %v", got)
	}
	var plain *http.Response
	plain, err = client.Get(gw.URL + "/ws/plain")
	if err != nil {
		t.Fatal(err)
	}
	plain.Body.Close()
	if plain.StatusCode != http.StatusOK {
		t.Errorf("WebSocket 仍打开时普通请求应恢复，实际 %d", plain.StatusCode)
	}
	if _, err := conn.Write([]byte("ping-ws")); err != nil {
		t.Fatalf("发送数据失败: %v", err)
	}
	echo := make([]byte, 32)
	var n int
	n, err = reader.Read(echo)
	if err != nil {
		t.Fatalf("读取回显失败: %v", err)
	}
	if string(echo[:n]) != "ping-ws" {
		t.Errorf("回显内容错误: %q", echo[:n])
	}
}

// TestIntegrationWebSocketShutdown 验证停机等待 WebSocket 排空，并在到期后断开连接。
func TestIntegrationWebSocketShutdown(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
		force   bool
	}{{"客户端先关闭", 2 * time.Second, false}, {"排空期限到期", 100 * time.Millisecond, true}} {
		t.Run(tc.name, func(t *testing.T) {
			upstreamClosed := make(chan struct{})
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					return
				}
				defer close(upstreamClosed)
				defer conn.Close()
				if _, err = io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"); err != nil {
					return
				}
				_, _ = io.Copy(io.Discard, conn)
			}))
			t.Cleanup(up.Close)
			gw, _, _ := gatewayHandler(t, []config.Route{{
				ID: "ws", Predicates: []config.Predicate{{Path: "/ws"}}, Target: config.Target{Service: "demo-service"},
			}}, up)
			gw.cfg.Server.ShutdownTimeout = config.Duration(tc.timeout)
			host, listenerStopped := listenGateway(t, gw)
			conn, err := net.DialTimeout("tcp", host, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err = conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err = io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"); err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(conn)
			var resp *http.Response
			resp, err = http.ReadResponse(reader, nil)
			if err != nil || resp.StatusCode != http.StatusSwitchingProtocols {
				t.Fatalf("WebSocket 升级失败: %v %v", resp, err)
			}

			stopped := make(chan error, 1)
			go func() { stopped <- gw.Stop() }()
			select {
			case <-listenerStopped:
			case <-time.After(time.Second):
				t.Fatal("停机未关闭网关监听器")
			}
			if !tc.force {
				select {
				case <-stopped:
					t.Fatal("WebSocket 尚未退出时停机已返回")
				case <-time.After(50 * time.Millisecond):
				}
				if err = conn.Close(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err = <-stopped:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("网关排空未按期限完成")
			}
			if tc.force {
				_, err = reader.ReadByte()
				if !errors.Is(err, io.EOF) {
					t.Fatalf("排空期限到期后应断开客户端连接，实际 %v", err)
				}
			}
			select {
			case <-upstreamClosed:
			case <-time.After(time.Second):
				t.Fatal("上游 WebSocket 连接未关闭")
			}
		})
	}
}

// TestIntegrationInterruptedStream 验证共享恢复中间件保留流中断语义。
func TestIntegrationInterruptedStream(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := io.WriteString(w, "partial payload"); err != nil {
			return
		}
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}))
	t.Cleanup(up.Close)
	gw, _, _, _ := gatewayStack(t, []config.Route{{
		ID: "stream", Predicates: []config.Predicate{{Path: "/stream"}}, Target: config.Target{Service: "demo-service"},
	}}, up)
	resp, err := gw.Client().Get(gw.URL + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body []byte
	body, err = io.ReadAll(resp.Body)
	if !errors.Is(err, io.ErrUnexpectedEOF) || string(body) != "partial payload" {
		t.Fatalf("客户端应收到部分响应和中断错误，实际 %q %v", body, err)
	}
}
