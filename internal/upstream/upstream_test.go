package upstream

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-kratos/kratos/v3/registry"
	"github.com/go-kratos/kratos/v3/selector"

	"github.com/liujitcn/kratos-gateway/internal/route"
	"github.com/liujitcn/kratos-gateway/internal/testutil"
)

// testLogger 测试日志（丢弃输出）。
func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// TestBuildNodes 验证节点构建：scheme 筛选、地址去重、权重元数据读取。
func TestBuildNodes(t *testing.T) {
	ins := []*registry.ServiceInstance{
		{
			ID:        "i1",
			Name:      "demo",
			Metadata:  map[string]string{"weight": "30"},
			Endpoints: []string{"http://10.0.0.1:7001", "grpc://10.0.0.1:6101"},
		},
		{
			ID:        "i2",
			Name:      "demo",
			Endpoints: []string{"http://10.0.0.2:7002", "http://10.0.0.1:7001"},
		},
	}
	nodes := BuildNodes(ins, "http")
	if len(nodes) != 2 {
		t.Fatalf("应筛选出 2 个 http 节点（含去重）: %d", len(nodes))
	}
	if nodes[0].Address() != "10.0.0.1:7001" || nodes[0].Scheme() != "http" {
		t.Errorf("节点地址/协议错误: %s %s", nodes[0].Address(), nodes[0].Scheme())
	}
	if w := nodes[0].InitialWeight(); w == nil || *w != 30 {
		t.Errorf("权重元数据未读取: %v", w)
	}
	if w := nodes[1].InitialWeight(); w != nil {
		t.Errorf("未声明权重应为 nil: %v", *w)
	}
}

// TestRoundRobinDistribution 验证轮询均分与无节点报错。
func TestRoundRobinDistribution(t *testing.T) {
	r := &RoundRobin{}
	r.Apply(BuildNodes([]*registry.ServiceInstance{
		{ID: "i1", Name: "d", Endpoints: []string{"http://10.0.0.1:1"}},
		{ID: "i2", Name: "d", Endpoints: []string{"http://10.0.0.2:2"}},
	}, "http"))
	counts := map[string]int{}
	for i := 0; i < 20; i++ {
		node, done, err := r.Select(context.Background())
		if err != nil {
			t.Fatalf("选择失败: %v", err)
		}
		counts[node.Address()]++
		done(context.Background(), selector.DoneInfo{})
	}
	if counts["10.0.0.1:1"] != 10 || counts["10.0.0.2:2"] != 10 {
		t.Errorf("轮询应 10/10 均分: %v", counts)
	}

	r.Apply(nil)
	if _, _, err := r.Select(context.Background()); !errors.Is(err, selector.ErrNoAvailable) {
		t.Errorf("空节点表应返回 ErrNoAvailable: %v", err)
	}
}

// TestRoundRobinFilter 验证节点过滤器（排除集）。
func TestRoundRobinFilter(t *testing.T) {
	r := &RoundRobin{}
	r.Apply(BuildNodes([]*registry.ServiceInstance{
		{ID: "i1", Name: "d", Endpoints: []string{"http://10.0.0.1:1"}},
		{ID: "i2", Name: "d", Endpoints: []string{"http://10.0.0.2:2"}},
	}, "http"))
	exclude := map[string]struct{}{"10.0.0.1:1": {}}
	for i := 0; i < 5; i++ {
		node, _, err := r.Select(context.Background(), selector.WithNodeFilter(func(_ context.Context, nodes []selector.Node) []selector.Node {
			kept := make([]selector.Node, 0, len(nodes))
			for _, n := range nodes {
				if _, skip := exclude[n.Address()]; !skip {
					kept = append(kept, n)
				}
			}
			return kept
		}))
		if err != nil {
			t.Fatalf("选择失败: %v", err)
		}
		if node.Address() == "10.0.0.1:1" {
			t.Errorf("排除节点不应被选中")
		}
	}
}

// TestNodeBreaker 验证熔断：阈值触发打开、冷却后半开探测、成功关闭。
func TestNodeBreaker(t *testing.T) {
	b := NewNodeBreaker(2, 30*time.Millisecond)
	addr := "10.0.0.1:1"

	generation, allowed := b.Allow(addr)
	if !allowed {
		t.Fatal("初始应放行")
	}
	b.Record(t.Context(), addr, generation, errors.New("dial failed"))
	generation, allowed = b.Allow(addr)
	if !allowed {
		t.Fatal("单次失败未达阈值应放行")
	}
	b.Record(t.Context(), addr, generation, errors.New("dial failed"))
	if _, allowed = b.Allow(addr); allowed {
		t.Fatal("连续失败达阈值应熔断")
	}
	time.Sleep(50 * time.Millisecond)
	previous := generation
	generation, allowed = b.Allow(addr)
	if !allowed {
		t.Fatal("冷却结束应放行半开探测")
	}
	b.Record(t.Context(), addr, previous, nil)
	if _, allowed = b.Allow(addr); allowed {
		t.Fatal("半开探测在途时不应再放行")
	}
	b.Record(t.Context(), addr, generation, nil)
	if _, allowed = b.Allow(addr); !allowed {
		t.Fatal("探测成功应关闭熔断")
	}
}

// TestNodeBreakerHalfOpenFail 验证半开探测失败重新熔断。
func TestNodeBreakerHalfOpenFail(t *testing.T) {
	b := NewNodeBreaker(1, 20*time.Millisecond)
	addr := "10.0.0.1:1"
	generation, _ := b.Allow(addr)
	b.Record(t.Context(), addr, generation, errors.New("fail"))
	time.Sleep(30 * time.Millisecond)
	generation, allowed := b.Allow(addr)
	if !allowed {
		t.Fatal("冷却后应放行探测")
	}
	b.Record(t.Context(), addr, generation, errors.New("fail again"))
	if _, allowed = b.Allow(addr); allowed {
		t.Fatal("半开探测失败应重新熔断")
	}
}

// TestUpstreamClientCancellationKeepsBreakerState 验证客户端取消保留失败计数并释放半开探测。
func TestUpstreamClientCancellationKeepsBreakerState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		disc := testutil.NewFakeDiscovery()
		mgr := NewManager(disc, "consul", testLogger())
		defer mgr.Stop()
		u, err := mgr.Get(UpstreamConfig{
			Service: "svc", Scheme: "http", LB: "round_robin",
			Breaker: &BreakerConfig{FailureThreshold: 2, OpenDuration: time.Second},
		})
		if err != nil {
			t.Fatal(err)
		}
		disc.Push("svc", []*registry.ServiceInstance{{Endpoints: []string{"http://127.0.0.1:18081"}}})
		synctest.Wait()

		failure := errors.New("upstream disconnected")
		for _, result := range []error{failure, http.ErrAbortHandler, failure} {
			ctx, cancel := context.WithCancel(t.Context())
			var done selector.DoneFunc
			_, done, err = u.Select(ctx)
			if err != nil {
				cancel()
				t.Fatal("取消流式请求应保留前一次失败并继续放行", err)
			}
			if result == http.ErrAbortHandler {
				cancel()
			}
			done(ctx, selector.DoneInfo{Err: result})
			cancel()
		}
		if _, _, err = u.Select(t.Context()); !errors.Is(err, selector.ErrNoAvailable) {
			t.Fatalf("两次上游故障应触发熔断，实际 %v", err)
		}

		time.Sleep(time.Second)
		ctx, cancel := context.WithCancel(t.Context())
		var done selector.DoneFunc
		_, done, err = u.Select(ctx)
		cancel()
		if err != nil {
			t.Fatal("冷却结束应放行半开探测", err)
		}
		done(ctx, selector.DoneInfo{Err: context.Canceled})
		_, done, err = u.Select(t.Context())
		if err != nil {
			t.Fatal("取消半开请求后应允许下一次探测", err)
		}
		done(t.Context(), selector.DoneInfo{})
	})
}

// TestNodeBreakerRequestDeadlineKeepsState 验证路由期限到期保留失败计数并释放半开探测。
func TestNodeBreakerRequestDeadlineKeepsState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const addr = "127.0.0.1:18081"
		b := NewNodeBreaker(2, time.Second)
		ctx, cancel := context.WithDeadline(t.Context(), time.Now())
		defer cancel()

		generation, allowed := b.Allow(addr)
		if !allowed {
			t.Fatal("应放行首次请求")
		}
		b.Record(t.Context(), addr, generation, context.DeadlineExceeded)
		b.Record(ctx, addr, generation, ctx.Err())
		generation, allowed = b.Allow(addr)
		if !allowed {
			t.Fatal("路由期限到期不应触发熔断")
		}
		b.Record(t.Context(), addr, generation, context.DeadlineExceeded)
		if _, allowed = b.Allow(addr); allowed {
			t.Fatal("两次上游传输超时应触发熔断")
		}

		time.Sleep(time.Second)
		generation, allowed = b.Allow(addr)
		if !allowed {
			t.Fatal("冷却后应放行探测")
		}
		b.Record(ctx, addr, generation, ctx.Err())
		if _, allowed = b.Allow(addr); !allowed {
			t.Fatal("探测请求期限到期后应立即放行下一笔探测")
		}
	})
}

// TestManagerWatchAndSelect 验证 Manager 生命周期：Watch 推送后节点可用、命中统计、Keep 回收。
func TestManagerWatchAndSelect(t *testing.T) {
	disc := testutil.NewFakeDiscovery()
	mgr := NewManager(disc, "consul", testLogger())
	defer mgr.Stop()

	u, err := mgr.Get(UpstreamConfig{Service: "Demo_Service", Scheme: "http", LB: "round_robin"})
	if err != nil {
		t.Fatalf("创建上游失败: %v", err)
	}

	// 推送双实例（服务名规范化后为 demo-service）
	disc.Push("demo-service", []*registry.ServiceInstance{
		{ID: "i1", Name: "demo-service", Endpoints: []string{"http://10.0.0.1:7001"}},
		{ID: "i2", Name: "demo-service", Endpoints: []string{"http://10.0.0.2:7002"}},
	})

	// 等待 Watch 循环重灌节点表
	deadline := time.Now().Add(2 * time.Second)
	for {
		nodes := u.Snapshot().Nodes
		if len(nodes) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待节点表超时: %+v", u.Snapshot())
		}
		time.Sleep(5 * time.Millisecond)
	}

	counts := map[string]int{}
	for i := 0; i < 20; i++ {
		node, done, err := u.Select(context.Background())
		if err != nil {
			t.Fatalf("选择失败: %v", err)
		}
		counts[node.Address()]++
		done(context.Background(), selector.DoneInfo{})
	}
	if counts["10.0.0.1:7001"] != 10 || counts["10.0.0.2:7002"] != 10 {
		t.Errorf("轮询应 10/10 均分: %v", counts)
	}

	snap := mgr.Snapshot()
	if len(snap) != 1 || snap[0].Service != "demo-service" {
		t.Fatalf("快照服务错误: %+v", snap)
	}
	totalHits := int64(0)
	for _, n := range snap[0].Nodes {
		totalHits += n.Hits
	}
	if totalHits != 20 {
		t.Errorf("累计命中应为 20: %d", totalHits)
	}

	// Keep 空集合 → 回收上游，Watch 循环退出
	mgr.Keep(nil)
	time.Sleep(50 * time.Millisecond)
	if len(mgr.Snapshot()) != 0 {
		t.Errorf("Keep 后上游应被回收: %+v", mgr.Snapshot())
	}
}

// TestManagerSharedUpstream 验证同键路由共享上游。
func TestManagerSharedUpstream(t *testing.T) {
	disc := testutil.NewFakeDiscovery()
	mgr := NewManager(disc, "consul", testLogger())
	defer mgr.Stop()

	u1, err := mgr.Get(UpstreamConfig{Service: "svc", Scheme: "http", LB: "round_robin"})
	if err != nil {
		t.Fatalf("创建上游失败: %v", err)
	}
	u2, err := mgr.Get(UpstreamConfig{Service: "svc", Scheme: "http", LB: "round_robin"})
	if err != nil {
		t.Fatalf("创建上游失败: %v", err)
	}
	if u1 != u2 {
		t.Error("同服务+协议+算法应共享上游实例池")
	}
	if len(mgr.Snapshot()) != 1 {
		t.Errorf("应只有一个上游: %d", len(mgr.Snapshot()))
	}
}

// TestUpstreamVersionFilter 验证版本过滤：仅选择匹配实例元数据 version 的节点。
func TestUpstreamVersionFilter(t *testing.T) {
	disc := testutil.NewFakeDiscovery()
	mgr := NewManager(disc, "consul", testLogger())
	defer mgr.Stop()

	u, err := mgr.Get(UpstreamConfig{Service: "svc", Scheme: "http", LB: "round_robin", Version: "v2"})
	if err != nil {
		t.Fatalf("创建上游失败: %v", err)
	}
	disc.Push("svc", []*registry.ServiceInstance{
		{ID: "i1", Name: "svc", Version: "v1", Endpoints: []string{"http://10.0.0.1:7001"}},
		{ID: "i2", Name: "svc", Version: "v2", Endpoints: []string{"http://10.0.0.2:7002"}},
	})

	deadline := time.Now().Add(2 * time.Second)
	for {
		nodes := u.Snapshot().Nodes
		if len(nodes) == 1 && nodes[0].Address == "10.0.0.2:7002" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待节点表超时: %+v", u.Snapshot())
		}
		time.Sleep(5 * time.Millisecond)
	}

	for i := 0; i < 10; i++ {
		node, _, err := u.Select(context.Background())
		if err != nil {
			t.Fatalf("选择失败: %v", err)
		}
		if node.Address() != "10.0.0.2:7002" {
			t.Errorf("版本过滤应仅命中 v2 节点，实际: %s", node.Address())
		}
	}

	// 无匹配版本实例 → ErrNoAvailable
	u1Only, err := mgr.Get(UpstreamConfig{Service: "svc", Scheme: "http", LB: "wrr", Version: "v1-only"})
	if err != nil {
		t.Fatalf("创建上游失败: %v", err)
	}
	if _, _, err := u1Only.Select(context.Background()); !errors.Is(err, selector.ErrNoAvailable) {
		t.Errorf("无匹配版本应返回 ErrNoAvailable: %v", err)
	}
}

// TestUpstreamStickySelect 验证粘性选择：期望节点存在时直接复用，节点消失后回退常规选择。
func TestUpstreamStickySelect(t *testing.T) {
	disc := testutil.NewFakeDiscovery()
	mgr := NewManager(disc, "consul", testLogger())
	defer mgr.Stop()

	u, err := mgr.Get(UpstreamConfig{Service: "svc", Scheme: "http", LB: "round_robin"})
	if err != nil {
		t.Fatalf("创建上游失败: %v", err)
	}
	disc.Push("svc", []*registry.ServiceInstance{
		{ID: "i1", Name: "svc", Endpoints: []string{"http://10.0.0.1:7001"}},
		{ID: "i2", Name: "svc", Endpoints: []string{"http://10.0.0.2:7002"}},
	})
	deadline := time.Now().Add(2 * time.Second)
	for {
		if len(u.Snapshot().Nodes) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待节点表超时")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// 粘住 7002：无论轮询计数如何，应始终复用
	ctx := route.WithStickyAddr(context.Background(), "10.0.0.2:7002")
	for i := 0; i < 5; i++ {
		node, _, err := u.Select(ctx)
		if err != nil {
			t.Fatalf("选择失败: %v", err)
		}
		if node.Address() != "10.0.0.2:7002" {
			t.Errorf("粘性节点应被复用，实际: %s", node.Address())
		}
	}

	// 期望节点已被排除（转发重试场景）→ 回退常规选择
	ctx = route.WithStickyAddr(route.WithExcluded(context.Background(), map[string]struct{}{"10.0.0.2:7002": {}}), "10.0.0.2:7002")
	node, _, err := u.Select(ctx)
	if err != nil {
		t.Fatalf("选择失败: %v", err)
	}
	if node.Address() != "10.0.0.1:7001" {
		t.Errorf("排除粘性节点后应回退常规选择，实际: %s", node.Address())
	}

	// 期望节点下线（节点表移除）→ 回退常规选择
	disc.Push("svc", []*registry.ServiceInstance{
		{ID: "i1", Name: "svc", Endpoints: []string{"http://10.0.0.1:7001"}},
	})
	deadline = time.Now().Add(2 * time.Second)
	for {
		if len(u.Snapshot().Nodes) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待节点表更新超时")
		}
		time.Sleep(5 * time.Millisecond)
	}
	ctx = route.WithStickyAddr(context.Background(), "10.0.0.2:7002")
	node, _, err = u.Select(ctx)
	if err != nil {
		t.Fatalf("选择失败: %v", err)
	}
	if node.Address() != "10.0.0.1:7001" {
		t.Errorf("粘性节点下线后应回退常规选择，实际: %s", node.Address())
	}
}

// TestManagerInvalidLB 验证非法算法报错。
func TestManagerInvalidLB(t *testing.T) {
	disc := testutil.NewFakeDiscovery()
	mgr := NewManager(disc, "consul", testLogger())
	defer mgr.Stop()
	if _, err := mgr.Get(UpstreamConfig{Service: "svc", Scheme: "http", LB: "unknown"}); err == nil {
		t.Error("非法负载均衡算法应报错")
	}
}

// TestStaticTarget 验证静态目标构造与选择。
func TestStaticTarget(t *testing.T) {
	target, err := NewStaticTarget([]string{"http://127.0.0.1:9001", "127.0.0.1:9002"}, "round_robin")
	if err != nil {
		t.Fatalf("构造静态目标失败: %v", err)
	}
	addrs := map[string]int{}
	for i := 0; i < 10; i++ {
		node, _, err := target.Select(context.Background())
		if err != nil {
			t.Fatalf("选择失败: %v", err)
		}
		addrs[node.Address()]++
	}
	if addrs["127.0.0.1:9001"] != 5 || addrs["127.0.0.1:9002"] != 5 {
		t.Errorf("静态目标轮询应 5/5 均分: %v", addrs)
	}

	if _, err := NewStaticTarget([]string{"ftp://x"}, "round_robin"); err == nil {
		t.Error("非法协议应报错")
	}
	if _, err := NewStaticTarget(nil, "round_robin"); err == nil {
		t.Error("空地址列表应报错")
	}
}

// TestUnselectedHalfOpenNode 验证未被选中的节点保留探测资格。
func TestUnselectedHalfOpenNode(t *testing.T) {
	mgr := NewManager(testutil.NewFakeDiscovery(), "consul", testLogger())
	t.Cleanup(mgr.Stop)
	u, err := mgr.Get(UpstreamConfig{
		Service: "review-service", Scheme: "http", LB: "round_robin",
		Breaker: &BreakerConfig{FailureThreshold: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	u.applyNodes([]*registry.ServiceInstance{
		{Endpoints: []string{"http://127.0.0.1:18081"}},
		{Endpoints: []string{"http://127.0.0.1:18082"}},
	})
	node, done, err := u.Select(t.Context())
	if err != nil || node.Address() != "127.0.0.1:18081" {
		t.Fatalf("unexpected initial selection: %v %v", node, err)
	}
	done(t.Context(), selector.DoneInfo{Err: errors.New("dial failed")})
	for range 4 {
		node, done, err = u.Select(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		done(t.Context(), selector.DoneInfo{})
		if node.Address() == "127.0.0.1:18081" {
			return
		}
	}
	t.Fatal("cooled-down node was never selected again")
}

// TestStaticRetryExclusion 验证静态节点选择遵循重试排除集。
func TestStaticRetryExclusion(t *testing.T) {
	target, err := NewStaticTarget([]string{"http://127.0.0.1:18081", "http://127.0.0.1:18082"}, "round_robin")
	if err != nil {
		t.Fatal(err)
	}
	ctx := route.WithExcluded(t.Context(), map[string]struct{}{"127.0.0.1:18081": {}})
	node, _, err := target.Select(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if node.Address() != "127.0.0.1:18082" {
		t.Fatalf("picked an excluded node: %s", node.Address())
	}
}

// TestUpstreamMetadataVersion 验证路由能匹配约定的 version 元数据。
func TestUpstreamMetadataVersion(t *testing.T) {
	mgr := NewManager(testutil.NewFakeDiscovery(), "consul", testLogger())
	t.Cleanup(mgr.Stop)
	u, err := mgr.Get(UpstreamConfig{Service: "review-service", Scheme: "http", LB: "round_robin", Version: "v2"})
	if err != nil {
		t.Fatal(err)
	}
	u.applyNodes([]*registry.ServiceInstance{
		{Metadata: map[string]string{"version": "v2"}, Endpoints: []string{"http://127.0.0.1:18082"}},
	})
	if _, _, err = u.Select(t.Context()); err != nil {
		t.Errorf("unable to select version from registration metadata: %v", err)
	}
}

// TestUpstreamStickyVersion 验证粘性偏好不跳过版本隔离。
func TestUpstreamStickyVersion(t *testing.T) {
	mgr := NewManager(testutil.NewFakeDiscovery(), "consul", testLogger())
	t.Cleanup(mgr.Stop)
	u, err := mgr.Get(UpstreamConfig{Service: "review-service", Scheme: "http", LB: "round_robin", Version: "v2"})
	if err != nil {
		t.Fatal(err)
	}
	u.applyNodes([]*registry.ServiceInstance{
		{Version: "v1", Metadata: map[string]string{"version": "v1"}, Endpoints: []string{"http://127.0.0.1:18081"}},
		{Version: "v2", Metadata: map[string]string{"version": "v2"}, Endpoints: []string{"http://127.0.0.1:18082"}},
	})
	node, _, err := u.Select(route.WithStickyAddr(t.Context(), "127.0.0.1:18081"))
	if err != nil {
		t.Fatal(err)
	}
	if node.Version() != "v2" {
		t.Errorf("wanted v2 but got %s", node.Version())
	}
}
