package route

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liujitcn/kratos-gateway/internal/config"
)

// applyFilter 用单过滤器包装 echo 处理器并执行请求。
func applyFilter(t *testing.T, f Filter, r *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	echo := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, _ = w.Write([]byte(req.Method + " " + req.URL.Path + " " + req.Header.Get("X-Mark")))
	})
	w := httptest.NewRecorder()
	f.Do(echo).ServeHTTP(w, r)
	return w
}

// TestRewriteFilter 验证路径改写：段前缀替换与未命中不改写。
func TestRewriteFilter(t *testing.T) {
	f, err := newRewriteFilter(map[string]any{"prefix": "/api/v1/bi", "replacement": "/api/bi"})
	if err != nil {
		t.Fatalf("构造过滤器失败: %v", err)
	}
	w := applyFilter(t, f, req(http.MethodGet, "/api/v1/bi/report", nil))
	if !strings.Contains(w.Body.String(), "/api/bi/report") {
		t.Errorf("路径应已改写: %s", w.Body.String())
	}
	// 精确命中前缀自身
	w = applyFilter(t, f, req(http.MethodGet, "/api/v1/bi", nil))
	if !strings.Contains(w.Body.String(), "/api/bi") || strings.Contains(w.Body.String(), "/api/v1/bi") {
		t.Errorf("前缀自身应改写: %s", w.Body.String())
	}
	// 段边界不误伤
	w = applyFilter(t, f, req(http.MethodGet, "/api/v1/bi2", nil))
	if !strings.Contains(w.Body.String(), "/api/v1/bi2") {
		t.Errorf("段边界路径不应改写: %s", w.Body.String())
	}
}

// TestAddHeaderFilter 验证请求头注入。
func TestAddHeaderFilter(t *testing.T) {
	f, err := newAddHeaderFilter(map[string]any{"key": "X-Mark", "value": "injected"})
	if err != nil {
		t.Fatalf("构造过滤器失败: %v", err)
	}
	w := applyFilter(t, f, req(http.MethodGet, "/x", nil))
	if !strings.Contains(w.Body.String(), "injected") {
		t.Errorf("请求头应已注入: %s", w.Body.String())
	}
}

// TestResponseHeaderFilter 验证响应头注入（含状态码透传）。
func TestResponseHeaderFilter(t *testing.T) {
	f, err := newResponseHeaderFilter(map[string]any{"key": "X-GW-Served", "value": "yes"})
	if err != nil {
		t.Fatalf("构造过滤器失败: %v", err)
	}
	w := applyFilter(t, f, req(http.MethodGet, "/x", nil))
	if w.Header().Get("X-GW-Served") != "yes" {
		t.Errorf("响应头应已注入: %v", w.Header())
	}
}

// TestRateLimitFilter 验证令牌桶限流：burst 内放行，超限回 429。
func TestRateLimitFilter(t *testing.T) {
	f, err := newRateLimitFilter(map[string]any{"rate": float64(1), "burst": float64(2)})
	if err != nil {
		t.Fatalf("构造过滤器失败: %v", err)
	}
	r := req(http.MethodGet, "/limited", nil)
	w := applyFilter(t, f, r)
	if w.Code != http.StatusOK {
		t.Fatalf("第 1 笔应放行: %d", w.Code)
	}
	w = applyFilter(t, f, r)
	if w.Code != http.StatusOK {
		t.Fatalf("第 2 笔（burst 内）应放行: %d", w.Code)
	}
	w = applyFilter(t, f, r)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("第 3 笔应 429: %d", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Error("429 应带 Retry-After")
	}
}

// TestTimeoutFilter 验证请求超时取消上下文。
func TestTimeoutFilter(t *testing.T) {
	f, err := newTimeoutFilter(map[string]any{"duration": "20ms"})
	if err != nil {
		t.Fatalf("构造过滤器失败: %v", err)
	}
	slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			w.WriteHeader(http.StatusGatewayTimeout)
		case <-time.After(time.Second):
			_, _ = w.Write([]byte("slow"))
		}
	})
	w := httptest.NewRecorder()
	f.Do(slow).ServeHTTP(w, req(http.MethodPost, "/slow", nil))
	if w.Code != http.StatusGatewayTimeout {
		t.Errorf("超时应触发上下文取消: %d", w.Code)
	}
}

// TestBuildFilters 验证排序与未知过滤器报错。
func TestBuildFilters(t *testing.T) {
	filters, err := BuildFilters([]config.Filter{
		{Name: "timeout", Args: map[string]any{"duration": "1s"}},
		{Name: "rewrite", Args: map[string]any{"prefix": "/a", "replacement": "/b"}},
	}, nil)
	if err != nil {
		t.Fatalf("构造过滤器失败: %v", err)
	}
	if filters[0].Order() != 100 || filters[1].Order() != 400 {
		t.Errorf("过滤器应按 Order 升序: %d, %d", filters[0].Order(), filters[1].Order())
	}
	if _, err = BuildFilters([]config.Filter{{Name: "no-such"}}, nil); err == nil {
		t.Error("未注册过滤器应报错")
	}
	if _, err = BuildFilters([]config.Filter{{Name: "rewrite", Args: map[string]any{}}}, nil); err == nil {
		t.Error("缺少参数应报错")
	}
}
