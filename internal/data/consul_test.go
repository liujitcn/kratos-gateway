package data

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v3/selector"
	configv1 "github.com/liujitcn/kratos-kit/api/gen/go/config/v1"

	"github.com/liujitcn/kratos-gateway/internal/config"
	"github.com/liujitcn/kratos-gateway/internal/data/upstream"
)

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
	cfg := config.Catalog{ServicePrefixes: []string{"app-"}, PollInterval: config.Duration(10 * time.Millisecond)}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	got := make(chan []config.Route, 1)
	go RunCatalog(ctx, cfg, &configv1.Registry_Consul{Address: consul.Listener.Addr().String()}, slog.New(slog.DiscardHandler), func(routes []config.Route) { got <- routes })
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
	disc, err := NewDiscovery(&configv1.Registry{Type: "consul", Consul: &configv1.Registry_Consul{Address: consul.URL}})
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
