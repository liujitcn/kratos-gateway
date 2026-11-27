package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v3/selector"

	"github.com/liujitcn/kratos-gateway/internal/biz/gateway/route"
	"github.com/liujitcn/kratos-gateway/internal/data/upstream"
)

// testLogger 测试日志（丢弃输出）。
func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// echoUpstream 启动回显测试上游（记录命中数）。
func echoUpstream(t *testing.T, name string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(name + "|" + r.Header.Get("X-Forwarded-Host") + "|" + r.Header.Get("X-Forwarded-Proto")))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// deadAddr 返回一个确定无监听的本地地址（连接拒绝）。
func deadAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("占用端口失败: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("释放端口失败: %v", err)
	}
	return addr
}

// hostPort 从 httptest.Server URL 提取 host:port。
func hostPort(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	host, port, err := net.SplitHostPort(srv.URL[len("http://"):])
	if err != nil {
		t.Fatalf("解析地址失败: %v", err)
	}
	return host + ":" + port
}

// TestForwardHappyPath 验证转发：X-GW-Upstream 响应头、X-Forwarded 转发头、保留原始 Host。
func TestForwardHappyPath(t *testing.T) {
	up := echoUpstream(t, "up-a")
	target, err := upstream.NewStaticTarget([]string{up.URL}, "round_robin")
	if err != nil {
		t.Fatalf("构造目标失败: %v", err)
	}
	h := NewForwardHandler(target, 0, false, nil, testLogger())

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "http://gw.local/demo/path", nil)
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("转发应 200: %d %s", w.Code, w.Body.String())
	}
	if w.Header().Get("X-GW-Upstream") != hostPort(t, up) {
		t.Errorf("X-GW-Upstream 标注错误: %q", w.Header().Get("X-GW-Upstream"))
	}
	if w.Body.String() != "up-a|gw.local|http" {
		t.Errorf("转发头规范错误: %q", w.Body.String())
	}
}

// TestForwardNoInstance 验证无健康实例回 503。
func TestForwardNoInstance(t *testing.T) {
	target := &stubTarget{err: selector.ErrNoAvailable}
	h := NewForwardHandler(target, 0, false, nil, testLogger())
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("无实例应 503: %d", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Error("503 应带 Retry-After")
	}
}

// TestForwardRetryGet 验证幂等 GET 转发失败换节点重试成功。
func TestForwardRetryGet(t *testing.T) {
	up := echoUpstream(t, "alive")
	// 静态节点顺序：死地址在前，轮询先选死节点，失败后重试换到存活节点
	target, err := upstream.NewStaticTarget([]string{"http://" + deadAddr(t), up.URL}, "round_robin")
	if err != nil {
		t.Fatalf("构造目标失败: %v", err)
	}
	h := NewForwardHandler(target, 1, false, nil, testLogger())

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://gw.local/x", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("GET 重试后应 200: %d %s", w.Code, w.Body.String())
	}
	if w.Body.String() != "alive|gw.local|http" {
		t.Errorf("重试后应由存活节点服务: %q", w.Body.String())
	}
}

// TestForwardRetryGetBody 验证可重放的 GET 请求体在重试时完整重建。
func TestForwardRetryGetBody(t *testing.T) {
	for _, tc := range []struct {
		name     string
		getBody  bool
		attempts int
		status   int
	}{
		{"stream", false, 1, http.StatusBadGateway},
		{"replayable", true, 2, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target, err := upstream.NewStaticTarget([]string{"127.0.0.1:18081", "127.0.0.1:18082"}, "round_robin")
			if err != nil {
				t.Fatal(err)
			}
			h := NewForwardHandler(target, 1, false, nil, testLogger())
			attempts := 0
			h.proxy.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				attempts++
				body, err := io.ReadAll(r.Body)
				if err != nil || string(body) != "payload" {
					t.Errorf("第 %d 次转发请求体错误 %q，错误 %v", attempts, body, err)
				}
				if attempts == 1 {
					return nil, errors.New("upstream disconnected")
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody, Request: r}, nil
			})

			var r *http.Request
			r, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "http://gw.local/x", strings.NewReader("payload"))
			if err != nil {
				t.Fatal(err)
			}
			if !tc.getBody {
				r.GetBody = nil
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.status || attempts != tc.attempts {
				t.Fatalf("状态码 %d，转发 %d 次，预期状态码 %d，转发 %d 次", w.Code, attempts, tc.status, tc.attempts)
			}
		})
	}
}

// TestForwardRetriesAfterResponseHeaderTimeout 验证上游迟迟不发响应头时会重试。
func TestForwardRetriesAfterResponseHeaderTimeout(t *testing.T) {
	release := make(chan struct{})
	hung := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(hung.Close)
	defer close(release)
	alive := echoUpstream(t, "alive")
	target, err := upstream.NewStaticTarget([]string{hung.URL, alive.URL}, "round_robin")
	if err != nil {
		t.Fatal(err)
	}
	h := NewForwardHandler(target, 1, false, nil, testLogger())
	transport, ok := h.proxy.Transport.(*http.Transport)
	if !ok || transport.ResponseHeaderTimeout <= 0 {
		t.Fatal("转发传输层应设置响应头超时")
	}
	transport = transport.Clone()
	transport.ResponseHeaderTimeout = 50 * time.Millisecond
	t.Cleanup(transport.CloseIdleConnections)
	h.proxy.Transport = transport

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequestWithContext(ctx, http.MethodGet, "http://gw.local/x", nil))
	if w.Code != http.StatusOK || w.Header().Get("X-GW-Upstream") != hostPort(t, alive) {
		t.Fatalf("等待响应头超时后应转发到备选节点，实际 %d %q", w.Code, w.Body.String())
	}
}

// TestForwardCanceledRequestDoesNotRetry 验证取消的请求不会再转发给下一节点。
func TestForwardCanceledRequestDoesNotRetry(t *testing.T) {
	target, err := upstream.NewStaticTarget([]string{"127.0.0.1:18081", "127.0.0.1:18082"}, "round_robin")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	h := NewForwardHandler(target, 1, false, nil, testLogger())
	attempts := 0
	h.proxy.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts++
		cancel()
		return nil, r.Context().Err()
	})
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(ctx, http.MethodGet, "/x", nil))
	if attempts != 1 {
		t.Fatalf("客户端已取消，应只转发一次，实际 %d", attempts)
	}
}

// TestForwardNoRetryPost 验证非幂等方法不重试，直接回 502。
func TestForwardNoRetryPost(t *testing.T) {
	up := echoUpstream(t, "alive")
	target, err := upstream.NewStaticTarget([]string{"http://" + deadAddr(t), up.URL}, "round_robin")
	if err != nil {
		t.Fatalf("构造目标失败: %v", err)
	}
	h := NewForwardHandler(target, 1, false, nil, testLogger())

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "http://gw.local/x", nil))
	if w.Code != http.StatusBadGateway {
		t.Fatalf("POST 失败不重试应 502: %d", w.Code)
	}
}

// TestForwardAllDead 验证全部节点失败：重试耗尽后回 502。
func TestForwardAllDead(t *testing.T) {
	target, err := upstream.NewStaticTarget([]string{"http://" + deadAddr(t)}, "round_robin")
	if err != nil {
		t.Fatalf("构造目标失败: %v", err)
	}
	h := NewForwardHandler(target, 1, false, nil, testLogger())
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://gw.local/x", nil))
	if w.Code != http.StatusBadGateway {
		t.Fatalf("全部节点失败应 502: %d", w.Code)
	}
}

// stubTarget 桩目标：固定返回预设节点或错误。
type stubTarget struct {
	node selector.Node
	err  error
}

// roundTripFunc 把测试函数适配为 http.RoundTripper。
type roundTripFunc func(*http.Request) (*http.Response, error)

// RoundTrip 执行测试定义的转发逻辑。
func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

// Select 实现 route.Target。
func (t *stubTarget) Select(_ context.Context) (selector.Node, selector.DoneFunc, error) {
	if t.err != nil {
		return nil, nil, t.err
	}
	return t.node, func(context.Context, selector.DoneInfo) {}, nil
}

// TestForwardNoRetryCandidate 保留首轮转发错误，即使下一轮已无可选节点。
func TestForwardNoRetryCandidate(t *testing.T) {
	addr := deadAddr(t)
	h := NewForwardHandler(&singleAttemptTarget{node: selector.NewNode("http", addr, nil)}, 1, false, nil, testLogger())
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
	if w.Code != http.StatusBadGateway || w.Header().Get("Retry-After") != "" {
		t.Fatalf("转发已失败且无备选节点时应返回 502，实际 %d", w.Code)
	}
}

// TestForwardReportsInterruptedStream 验证流中断时完成回调只收到一次失败。
func TestForwardReportsInterruptedStream(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := w.Write([]byte("partial")); err != nil {
			return
		}
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}))
	t.Cleanup(up.Close)

	reported := make(chan selector.DoneInfo, 2)
	target := &singleAttemptTarget{node: selector.NewNode("http", hostPort(t, up), nil), reported: reported}
	gw := httptest.NewServer(NewForwardHandler(target, 1, false, nil, testLogger()))
	t.Cleanup(gw.Close)

	resp, err := gw.Client().Get(gw.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err = io.ReadAll(resp.Body); err == nil {
		t.Fatal("客户端应检测到响应中断")
	}
	select {
	case result := <-reported:
		if result.Err == nil {
			t.Fatal("中断的转发应上报失败")
		}
	default:
		t.Fatal("流中断时未执行完成回调")
	}
	select {
	case <-reported:
		t.Fatal("完成回调执行了多次")
	default:
	}
}

// singleAttemptTarget 模拟首选节点失败后再无备选的目标。
type singleAttemptTarget struct {
	node     selector.Node
	reported chan<- selector.DoneInfo
}

// Select 返回首选节点并记录完成状态，重试时遵循排除集合。
func (t *singleAttemptTarget) Select(ctx context.Context) (selector.Node, selector.DoneFunc, error) {
	if len(route.ExcludedFrom(ctx)) > 0 {
		return nil, nil, selector.ErrNoAvailable
	}
	return t.node, func(_ context.Context, result selector.DoneInfo) {
		if t.reported != nil {
			t.reported <- result
		}
	}, nil
}
