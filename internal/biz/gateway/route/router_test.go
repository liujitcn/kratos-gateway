package route

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// newRoute 构造测试路由（含首个路径谓词）。
func newRoute(id, prefix string, predicates ...Predicate) *Route {
	all := append([]Predicate{NewPathPredicate(prefix)}, predicates...)
	return &Route{ID: id, Predicates: all, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(id))
	})}
}

// TestRouterLongestPrefix 验证最长前缀优先与段边界不误伤。
func TestRouterLongestPrefix(t *testing.T) {
	router := NewRouter()
	router.Update([]*Route{
		newRoute("short", "/api/v1"),
		newRoute("long", "/api/v1/base"),
	})

	cases := []struct {
		path string
		want string
	}{
		{"/api/v1/base/captcha", "long"},
		{"/api/v1/base", "long"},
		{"/api/v1/base2", "short"}, // 段边界：/api/v1/base 不吞 /api/v1/base2
		{"/api/v1/other", "short"},
		{"/api/v2", ""}, // 未命中
	}
	for _, c := range cases {
		got := router.Match(req(http.MethodGet, c.path, nil))
		if c.want == "" {
			if got != nil {
				t.Errorf("path %s 应未命中，实际命中 %s", c.path, got.ID)
			}
			continue
		}
		if got == nil || got.ID != c.want {
			id := ""
			if got != nil {
				id = got.ID
			}
			t.Errorf("path %s 命中 %s 期望 %s", c.path, id, c.want)
		}
	}
}

// TestRouterDeclarationOrder 验证同深度按声明顺序，方法谓词失败时回退后续候选。
func TestRouterDeclarationOrder(t *testing.T) {
	router := NewRouter()
	router.Update([]*Route{
		newRoute("post-only", "/api/a", NewMethodPredicate([]string{"POST"})),
		newRoute("first", "/api/a"),
	})
	// POST：post-only 全谓词命中（同深度先声明者胜）
	got := router.Match(req(http.MethodPost, "/api/a", nil))
	if got == nil || got.ID != "post-only" {
		t.Errorf("POST /api/a 应命中 post-only")
	}
	// GET：post-only 方法谓词失败，回退到后续候选 first
	got = router.Match(req(http.MethodGet, "/api/a", nil))
	if got == nil || got.ID != "first" {
		t.Errorf("GET /api/a 应回退命中 first")
	}
}

// TestRouterServeHTTP 验证 404 与命中转发。
func TestRouterServeHTTP(t *testing.T) {
	router := NewRouter()
	router.Update([]*Route{newRoute("demo", "/demo")})

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req(http.MethodGet, "/demo/x", nil))
	if w.Code != http.StatusOK || w.Body.String() != "demo" {
		t.Errorf("命中路由响应错误: code=%d body=%s", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	router.ServeHTTP(w, req(http.MethodGet, "/none", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("未命中应 404: %d", w.Code)
	}
}

// TestRouterHotSwap 验证路由表原子热切换。
func TestRouterHotSwap(t *testing.T) {
	router := NewRouter()
	router.Update([]*Route{newRoute("old", "/api")})
	router.Update([]*Route{newRoute("new", "/api")})

	got := router.Match(req(http.MethodGet, "/api", nil))
	if got == nil || got.ID != "new" {
		t.Errorf("热切换后应命中 new")
	}
}
