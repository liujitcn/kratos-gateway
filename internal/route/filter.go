package route

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"
	"time"

	"golang.org/x/time/rate"

	"github.com/liujitcn/kratos-gateway/internal/config"
)

// FilterFactory 路由过滤器工厂：按名称与参数构造过滤器。
type FilterFactory func(args map[string]any) (Filter, error)

// filterFactories 内置过滤器工厂注册表。
var filterFactories = map[string]FilterFactory{
	"rewrite":         newRewriteFilter,
	"add-header":      newAddHeaderFilter,
	"response-header": newResponseHeaderFilter,
	"rate-limit":      newRateLimitFilter,
	"timeout":         newTimeoutFilter,
}

// RegisterFilter 注册自定义过滤器工厂（供扩展；同名覆盖）。
func RegisterFilter(name string, factory FilterFactory) {
	filterFactories[name] = factory
}

// BuildFilters 构建并排序路由过滤器，复用限流顺序和参数均未变的令牌桶。
func BuildFilters(specs []config.Filter, previous []Filter) ([]Filter, error) {
	filters := make([]Filter, 0, len(specs))
	for i, spec := range specs {
		factory, ok := filterFactories[spec.Name]
		if !ok {
			return nil, fmt.Errorf("路由过滤器 %q 未注册", spec.Name)
		}
		f, err := factory(spec.Args)
		if err != nil {
			return nil, fmt.Errorf("构造路由过滤器 %q（第 %d 项）: %w", spec.Name, i+1, err)
		}
		filters = append(filters, f)
	}
	sort.SliceStable(filters, func(i, j int) bool { return filters[i].Order() < filters[j].Order() })
	var oldLimits []*rateLimitFilter
	for _, f := range previous {
		if limiter, ok := f.(*rateLimitFilter); ok {
			oldLimits = append(oldLimits, limiter)
		}
	}
	for i, f := range filters {
		if len(oldLimits) == 0 {
			break
		}
		if limiter, ok := f.(*rateLimitFilter); ok {
			old := oldLimits[0]
			oldLimits = oldLimits[1:]
			if limiter.limiter.Limit() == old.limiter.Limit() && limiter.limiter.Burst() == old.limiter.Burst() {
				filters[i] = old
			}
		}
	}
	return filters, nil
}

// UpgradeResponseHeaders 按普通响应的覆盖顺序收集升级响应所需的配置头。
func UpgradeResponseHeaders(filters []Filter) map[string]string {
	var headers map[string]string
	for _, filter := range slices.Backward(filters) {
		if header, ok := filter.(*responseHeaderFilter); ok {
			if headers == nil {
				headers = make(map[string]string)
			}
			headers[http.CanonicalHeaderKey(header.key)] = header.value
		}
	}
	return headers
}

// rewriteFilter 路径改写过滤器：命中 prefix 段前缀的路径替换为 replacement 前缀。
type rewriteFilter struct {
	order       int
	prefix      string
	replacement string
}

// newRewriteFilter 构造路径改写过滤器（参数：prefix、replacement）。
func newRewriteFilter(args map[string]any) (Filter, error) {
	prefix, err := argString(args, "prefix")
	if err != nil {
		return nil, err
	}
	var replacement string
	replacement, err = argString(args, "replacement")
	if err != nil {
		return nil, err
	}
	return &rewriteFilter{
		order:       100,
		prefix:      strings.TrimSuffix(prefix, "/"),
		replacement: strings.TrimSuffix(replacement, "/"),
	}, nil
}

// Order 实现 Filter。
func (f *rewriteFilter) Order() int { return f.order }

// Do 实现 Filter：替换路径前缀并保留原后缀的转义形式。
func (f *rewriteFilter) Do(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if matchSegmentPrefix(f.prefix, r.URL.Path) {
			path := f.replacement + r.URL.Path[len(f.prefix):]
			if path == "" {
				path = "/"
			}
			if r.URL.RawPath != "" {
				escaped := r.URL.EscapedPath()
				// 在转义路径中跳过与已解码前缀对应的字节。
				offset := 0
				for range len(f.prefix) {
					if escaped[offset] == '%' {
						offset += 3
					} else {
						offset++
					}
				}
				replacement := url.URL{Path: f.replacement}
				r.URL.RawPath = replacement.EscapedPath() + escaped[offset:]
			}
			r.URL.Path = path
		}
		next.ServeHTTP(w, r)
	})
}

// addHeaderFilter 请求头注入过滤器：转发前为上游请求设置固定请求头。
type addHeaderFilter struct {
	order int
	key   string
	value string
}

// newAddHeaderFilter 构造请求头注入过滤器（参数：key、value）。
func newAddHeaderFilter(args map[string]any) (Filter, error) {
	key, err := argString(args, "key")
	if err != nil {
		return nil, err
	}
	var value string
	value, err = argString(args, "value")
	if err != nil {
		return nil, err
	}
	return &addHeaderFilter{order: 200, key: key, value: value}, nil
}

// Order 实现 Filter。
func (f *addHeaderFilter) Order() int { return f.order }

// Do 实现 Filter：设置请求头后交由 next。
func (f *addHeaderFilter) Do(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set(f.key, f.value)
		next.ServeHTTP(w, r)
	})
}

// responseHeaderFilter 响应头注入过滤器：为响应设置固定头并覆盖同名值。
type responseHeaderFilter struct {
	order int
	key   string
	value string
}

// newResponseHeaderFilter 构造响应头注入过滤器（参数：key、value）。
func newResponseHeaderFilter(args map[string]any) (Filter, error) {
	key, err := argString(args, "key")
	if err != nil {
		return nil, err
	}
	var value string
	value, err = argString(args, "value")
	if err != nil {
		return nil, err
	}
	return &responseHeaderFilter{order: 250, key: key, value: value}, nil
}

// Order 实现 Filter。
func (f *responseHeaderFilter) Order() int { return f.order }

// Do 实现 Filter：包装 ResponseWriter 在写头时注入响应头。
func (f *responseHeaderFilter) Do(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&headerWriter{ResponseWriter: w, key: f.key, value: f.value}, r)
	})
}

// headerWriter 响应头注入包装器（透传 Flush/Hijack 以兼容 SSE/WebSocket）。
type headerWriter struct {
	http.ResponseWriter
	key   string
	value string
}

// WriteHeader 拦截首次写头，注入配置的响应头。
func (w *headerWriter) WriteHeader(code int) {
	w.Header().Set(w.key, w.value)
	w.ResponseWriter.WriteHeader(code)
}

// Write 写入前同样注入（隐式 200 路径不经过 WriteHeader）。
func (w *headerWriter) Write(b []byte) (int, error) {
	w.Header().Set(w.key, w.value)
	return w.ResponseWriter.Write(b)
}

// Flush 透传 Flush 以兼容 SSE 流式响应。
func (w *headerWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack 透传 Hijack 以兼容 WebSocket 升级。
func (w *headerWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("上游 ResponseWriter 不支持 Hijack")
	}
	return h.Hijack()
}

// rateLimitFilter 令牌桶限流过滤器：超出速率返回 429 并带 Retry-After。
type rateLimitFilter struct {
	order   int
	limiter *rate.Limiter
}

// newRateLimitFilter 构造限流过滤器（参数：rate 每秒令牌数、burst 桶容量，缺省取 rate 向上取整）。
func newRateLimitFilter(args map[string]any) (Filter, error) {
	rps, err := argFloat(args, "rate")
	if err != nil {
		return nil, err
	}
	if rps <= 0 {
		return nil, errors.New("rate 必须为正数")
	}
	var burst int
	burst, err = argInt(args, "burst", 0)
	if err != nil {
		return nil, err
	}
	if burst <= 0 {
		burst = int(math.Ceil(rps))
	}
	return &rateLimitFilter{order: 300, limiter: rate.NewLimiter(rate.Limit(rps), burst)}, nil
}

// Order 实现 Filter。
func (f *rateLimitFilter) Order() int { return f.order }

// Do 实现 Filter：未取得令牌时返回 429。
func (f *rateLimitFilter) Do(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !f.limiter.Allow() {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "gateway: rate limited", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// timeoutFilter 请求超时过滤器：超过时长取消请求上下文（SSE/WS 长连接路由勿配置）。
type timeoutFilter struct {
	order   int
	timeout time.Duration
}

// newTimeoutFilter 构造超时过滤器（参数：duration，如 "30s"）。
func newTimeoutFilter(args map[string]any) (Filter, error) {
	d, err := argDuration(args, "duration")
	if err != nil {
		return nil, err
	}
	return &timeoutFilter{order: 400, timeout: d}, nil
}

// Order 实现 Filter。
func (f *timeoutFilter) Order() int { return f.order }

// Do 实现 Filter：为请求上下文设置超时。
func (f *timeoutFilter) Do(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), f.timeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// argString 读取必填字符串参数。
func argString(args map[string]any, key string) (string, error) {
	v, ok := args[key]
	if !ok {
		return "", fmt.Errorf("缺少参数 %q", key)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("参数 %q 必须为字符串", key)
	}
	return s, nil
}

// argInt 读取整数参数（容忍 YAML/JSON 解析出的 float64），缺省返回 def。
func argInt(args map[string]any, key string, def int) (int, error) {
	v, ok := args[key]
	if !ok {
		return def, nil
	}
	switch n := v.(type) {
	case int:
		return n, nil
	case int64:
		return int(n), nil
	case float64:
		return int(n), nil
	default:
		return 0, fmt.Errorf("参数 %q 必须为整数", key)
	}
}

// argFloat 读取浮点参数（容忍整数形式），缺省返回 0。
func argFloat(args map[string]any, key string) (float64, error) {
	v, ok := args[key]
	if !ok {
		return 0, fmt.Errorf("缺少参数 %q", key)
	}
	switch n := v.(type) {
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	case float64:
		return n, nil
	default:
		return 0, fmt.Errorf("参数 %q 必须为数值", key)
	}
}

// argDuration 读取时长参数：支持 "30s" 字符串或秒数数值。
func argDuration(args map[string]any, key string) (time.Duration, error) {
	v, ok := args[key]
	if !ok {
		return 0, fmt.Errorf("缺少参数 %q", key)
	}
	switch d := v.(type) {
	case string:
		dur, err := time.ParseDuration(d)
		if err != nil {
			return 0, fmt.Errorf("参数 %q 时长格式错误: %w", key, err)
		}
		return dur, nil
	case int:
		return time.Duration(d) * time.Second, nil
	case int64:
		return time.Duration(d) * time.Second, nil
	case float64:
		return time.Duration(d * float64(time.Second)), nil
	default:
		return 0, fmt.Errorf("参数 %q 必须为时长字符串或秒数", key)
	}
}
