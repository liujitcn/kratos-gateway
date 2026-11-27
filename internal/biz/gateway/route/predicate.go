package route

import (
	"fmt"
	"hash/fnv"
	"net"
	"net/http"
	"strings"

	"github.com/liujitcn/kratos-gateway/internal/config"
)

// PathPredicate 段前缀路径断言：/api/v1/base 命中自身与其子路径，不命中 /api/v1/base2。
type PathPredicate struct {
	// Prefix 段前缀（构造时已规范化：无尾斜杠，根路径为空串）。
	Prefix string
	// Segments 前缀段数，用于路由表排序（最长前缀优先）。
	Segments int
}

// NewPathPredicate 构造段前缀断言：去除尾部斜杠以统一段语义（"/api/v1/base/" 与 "/api/v1/base" 等价）。
func NewPathPredicate(prefix string) *PathPredicate {
	prefix = strings.TrimSuffix(prefix, "/")
	return &PathPredicate{Prefix: prefix, Segments: len(strings.Split(prefix, "/")) - 1}
}

// Match 实现 Predicate：按路径段对齐匹配前缀。
func (p *PathPredicate) Match(r *http.Request) bool {
	return matchSegmentPrefix(p.Prefix, r.URL.Path)
}

// MethodPredicate HTTP 方法断言：命中任一声明方法。
type MethodPredicate struct {
	// Methods 允许的方法集合（已统一大写）。
	Methods map[string]struct{}
}

// NewMethodPredicate 构造方法断言（方法名统一大写）。
func NewMethodPredicate(methods []string) *MethodPredicate {
	set := make(map[string]struct{}, len(methods))
	for _, m := range methods {
		set[strings.ToUpper(m)] = struct{}{}
	}
	return &MethodPredicate{Methods: set}
}

// Match 实现 Predicate。
func (p *MethodPredicate) Match(r *http.Request) bool {
	_, ok := p.Methods[r.Method]
	return ok
}

// HeaderPredicate 请求头断言：声明键值全部精确命中。
type HeaderPredicate struct {
	// Headers 必须命中的键值对。
	Headers map[string]string
}

// NewHeaderPredicate 构造请求头断言。
func NewHeaderPredicate(headers map[string]string) *HeaderPredicate {
	return &HeaderPredicate{Headers: headers}
}

// Match 实现 Predicate。
func (p *HeaderPredicate) Match(r *http.Request) bool {
	for k, v := range p.Headers {
		if r.Header.Get(k) != v {
			return false
		}
	}
	return true
}

// WeightPredicate 灰度分流断言：同组路由切分 [Start,End) 哈希槽位（0-99），基于请求特征稳定哈希。
// 区间由构建器按组内声明顺序累计分配，保证同组路由流量占比与配置权重一致。
type WeightPredicate struct {
	// Group 分流组名。
	Group string
	// Start 槽位区间起点（含）。
	Start int
	// End 槽位区间终点（不含）。
	End int
	// Key 稳定哈希特征键（请求头名）；为空时使用客户端 IP。
	Key string
}

// NewWeightPredicate 构造灰度断言（End 初始为权重值，区间由 assignWeightRanges 重排）。
func NewWeightPredicate(group string, percent int, key string) *WeightPredicate {
	return &WeightPredicate{Group: group, End: percent, Key: key}
}

// Match 实现 Predicate：请求特征哈希落在本路由槽位区间内才命中。
func (p *WeightPredicate) Match(r *http.Request) bool {
	slot := int(hashFeature(featureOf(r, p.Key)) % 100)
	return slot >= p.Start && slot < p.End
}

// BuildPredicates 按配置构造谓词列表（一项配置仅声明一种谓词）。
func BuildPredicates(specs []config.Predicate) ([]Predicate, error) {
	predicates := make([]Predicate, 0, len(specs))
	for i, spec := range specs {
		if spec.Path != "" {
			predicates = append(predicates, NewPathPredicate(spec.Path))
			continue
		}
		if len(spec.Method) > 0 {
			predicates = append(predicates, NewMethodPredicate(spec.Method))
			continue
		}
		if len(spec.Header) > 0 {
			predicates = append(predicates, NewHeaderPredicate(spec.Header))
			continue
		}
		if spec.Weight != nil {
			predicates = append(predicates, NewWeightPredicate(spec.Weight.Group, spec.Weight.Percent, spec.Weight.Key))
			continue
		}
		return nil, fmt.Errorf("第 %d 项谓词缺少 path/method/header/weight 配置", i+1)
	}
	return predicates, nil
}

// featureOf 提取分流特征：优先取指定请求头，缺省用客户端 IP。
func featureOf(r *http.Request, key string) string {
	if key != "" {
		if v := r.Header.Get(key); v != "" {
			return v
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// hashFeature 对特征做 FNV-1a 32 位哈希。
func hashFeature(feature string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(feature))
	return h.Sum32()
}

// matchSegmentPrefix 判断 path 是否命中 prefix 的段前缀（含 prefix 自身；prefix 为空匹配一切路径）。
func matchSegmentPrefix(prefix, path string) bool {
	if prefix == "" {
		return true
	}
	if path == prefix {
		return true
	}
	return strings.HasPrefix(path, prefix+"/")
}
