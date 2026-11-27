package route

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liujitcn/kratos-gateway/internal/config"
)

// req 构造测试请求。
func req(method, path string, header map[string]string) *http.Request {
	r := httptest.NewRequest(method, path, nil)
	for k, v := range header {
		r.Header.Set(k, v)
	}
	return r
}

// TestPathPredicateSegment 验证段前缀语义：含自身与子路径，不误伤同前缀串。
func TestPathPredicateSegment(t *testing.T) {
	p := NewPathPredicate("/api/v1/base")
	cases := []struct {
		path string
		want bool
	}{
		{"/api/v1/base", true},
		{"/api/v1/base/", true},
		{"/api/v1/base/captcha", true},
		{"/api/v1/base2", false},
		{"/api/v1/base64", false},
		{"/api/v1", false},
		{"/api/v2/base", false},
	}
	for _, c := range cases {
		if got := p.Match(req(http.MethodGet, c.path, nil)); got != c.want {
			t.Errorf("path %s 命中=%v 期望=%v", c.path, got, c.want)
		}
	}
}

// TestPathPredicateNormalize 验证尾斜杠归一与根路径全匹配。
func TestPathPredicateNormalize(t *testing.T) {
	p := NewPathPredicate("/api/v1/base/")
	if p.Prefix != "/api/v1/base" {
		t.Errorf("尾斜杠应归一: %q", p.Prefix)
	}
	if p.Segments != 3 {
		t.Errorf("段数错误: %d", p.Segments)
	}
	root := NewPathPredicate("/")
	if !root.Match(req(http.MethodGet, "/anything/else", nil)) {
		t.Error("根路径应匹配一切")
	}
}

// TestMethodPredicate 验证方法谓词（大小写归一、命中任一）。
func TestMethodPredicate(t *testing.T) {
	p := NewMethodPredicate([]string{"get", "POST"})
	if !p.Match(req(http.MethodGet, "/", nil)) {
		t.Error("GET 应命中")
	}
	if !p.Match(req(http.MethodPost, "/", nil)) {
		t.Error("POST 应命中")
	}
	if p.Match(req(http.MethodPut, "/", nil)) {
		t.Error("PUT 不应命中")
	}
}

// TestHeaderPredicate 验证请求头谓词（全部键值精确命中）。
func TestHeaderPredicate(t *testing.T) {
	p := NewHeaderPredicate(map[string]string{"X-Api-Key": "abc", "X-Tenant": "t1"})
	if !p.Match(req(http.MethodGet, "/", map[string]string{"X-Api-Key": "abc", "X-Tenant": "t1"})) {
		t.Error("全部命中应匹配")
	}
	if p.Match(req(http.MethodGet, "/", map[string]string{"X-Api-Key": "abc"})) {
		t.Error("缺少键不应匹配")
	}
	if p.Match(req(http.MethodGet, "/", map[string]string{"X-Api-Key": "wrong", "X-Tenant": "t1"})) {
		t.Error("值不匹配")
	}
}

// TestWeightPredicateStable 验证灰度分流：同特征稳定命中同一路由，分布接近权重。
func TestWeightPredicateStable(t *testing.T) {
	stable := NewWeightPredicate("g", 80, "X-User-Id")
	canary := NewWeightPredicate("g", 20, "X-User-Id")
	// 模拟 assignWeightRanges 的区间分配：[0,80) / [80,100)
	canary.Start = 80
	canary.End = 100

	stableCount := 0
	total := 10000
	for i := 0; i < total; i++ {
		r := req(http.MethodGet, "/", map[string]string{"X-User-Id": "user-" + string(rune('a'+i%26)) + "-" + string(rune('0'+i%10))})
		if stable.Match(r) {
			stableCount++
		} else if !canary.Match(r) {
			t.Fatal("灰度分流存在未覆盖槽位")
		}
	}
	ratio := float64(stableCount) / float64(total)
	// v0.3 验收：Weight(20/80) 分流比例误差 < 5%
	if ratio < 0.75 || ratio > 0.85 {
		t.Errorf("稳定版占比 %.3f 偏离 0.80 超过 5%%", ratio)
	}

	// 同一特征重复请求应稳定命中同一分组
	r := req(http.MethodGet, "/", map[string]string{"X-User-Id": "user-fixed"})
	if stable.Match(r) != stable.Match(r) {
		t.Error("同一特征应稳定命中")
	}
}

// TestBuildPredicates 验证谓词构造：非法项报错。
func TestBuildPredicates(t *testing.T) {
	predicates, err := BuildPredicates([]config.Predicate{{Path: "/a"}, {Method: []string{"GET"}}})
	if err != nil {
		t.Fatalf("构造谓词失败: %v", err)
	}
	if len(predicates) != 2 {
		t.Errorf("谓词数量错误: %d", len(predicates))
	}
	if _, err := BuildPredicates([]config.Predicate{{}}); err == nil {
		t.Error("空谓词项应报错")
	}
}
