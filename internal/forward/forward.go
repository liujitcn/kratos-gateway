package forward

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/go-kratos/kratos/v3/selector"

	"github.com/liujitcn/kratos-gateway/internal/route"
)

// proxyTransport 供各路由共享连接池，只限制上游发送响应头前的等待。
var proxyTransport = func() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 30 * time.Second
	return transport
}()

// ForwardHandler 链尾转发器：选节点 → 反向代理 → 转发失败时对幂等安全方法换节点重试。
// sticky 开启时按客户端连接粘住上游节点（SSE/WS 路由用，设计 11.3）。
type ForwardHandler struct {
	// target 转发目标。
	target route.Target
	// maxRetry 附加重试次数（0 表示不重试；仅幂等安全方法生效）。
	maxRetry int
	// stickyKey 启用粘性的上游池标识（依赖 ConnContext 注入的 StickySession）。
	stickyKey string
	// upgradeHeaders 升级响应使用的路由响应头覆盖值。
	upgradeHeaders map[string]string
	// logger 日志。
	logger *slog.Logger
	// proxy 共享反向代理实例（并发安全）。
	proxy *httputil.ReverseProxy
}

// NewForwardHandler 创建转发器。
func NewForwardHandler(target route.Target, maxRetry int, sticky bool, upgradeHeaders map[string]string, logger *slog.Logger) *ForwardHandler {
	h := &ForwardHandler{
		target:         target,
		maxRetry:       maxRetry,
		upgradeHeaders: upgradeHeaders,
		logger:         logger,
	}
	if sticky {
		if keyed, ok := target.(interface{ StickyKey() string }); ok {
			h.stickyKey = keyed.StickyKey()
		}
	}
	h.proxy = newProxy()
	return h
}

// ServeHTTP 转发请求，仅对方法安全且请求体可重放的请求换节点重试。
func (h *ForwardHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	excluded := map[string]struct{}{}
	ctx := route.WithExcluded(r.Context(), excluded)
	// 粘性路由：本连接已有粘住节点时优先复用（节点不可用/被排除时自动回退常规选择）
	var sess *route.StickySession
	if h.stickyKey != "" {
		sess = route.StickySessionFrom(ctx)
		if sess != nil {
			if addr := sess.Addr(h.stickyKey); addr != "" {
				ctx = route.WithStickyAddr(ctx, addr)
			}
		}
	}
	r = r.WithContext(ctx)
	// 与 ReverseProxy 一致，已知长度为零时不转发请求体。
	hasBody := r.Body != nil && r.Body != http.NoBody && r.ContentLength != 0
	var lastErr error
	for attempt := 0; attempt <= h.maxRetry; attempt++ {
		if ctx.Err() == context.Canceled {
			return
		}
		var err error
		if attempt > 0 && hasBody {
			var body io.ReadCloser
			body, err = r.GetBody()
			if err != nil {
				lastErr = errors.Join(lastErr, fmt.Errorf("重建请求体: %w", err))
				break
			}
			r.Body = body
		}
		var node selector.Node
		var done selector.DoneFunc
		node, done, err = h.target.Select(ctx)
		if err != nil {
			if attempt > 0 && hasBody {
				if closeErr := r.Body.Close(); closeErr != nil {
					h.logger.Warn("关闭未转发的重试请求体失败", "error", closeErr)
				}
			}
			if lastErr != nil {
				break
			}
			h.writeSelectError(w, err)
			return
		}
		if info := route.ReqInfoFrom(ctx); info != nil {
			info.Node = node.Address()
		}
		if sess != nil {
			sess.Bind(h.stickyKey, node.Address())
		}
		err = h.proxyAttempt(w, r, node, done)
		if err == nil {
			return
		}
		ctxErr := ctx.Err()
		if ctxErr == context.Canceled {
			return
		}
		lastErr = err
		if attempt == h.maxRetry || ctxErr != nil || !safeMethod(r.Method) || (hasBody && r.GetBody == nil) {
			break
		}
		excluded[node.Address()] = struct{}{}
		h.logger.Warn("转发失败，尝试重选节点", "upstream", node.Address(), "method", r.Method, "error", err)
	}
	h.logger.Error("转发失败", "error", lastErr)
	http.Error(w, "gateway: upstream error", http.StatusBadGateway)
}

// proxyAttempt 转发单次请求，即使响应流中断也向选择器上报完成结果。
func (h *ForwardHandler) proxyAttempt(w http.ResponseWriter, r *http.Request, node selector.Node, done selector.DoneFunc) error {
	state := &attemptState{ResponseWriter: w, node: node, upgradeHeaders: h.upgradeHeaders}
	if probe, ok := node.(interface{ ResponseReceived() }); ok {
		state.onResponse = probe.ResponseReceived
	}
	ctx := context.WithValue(r.Context(), attemptKey{}, state)
	// ReverseProxy 在响应写出后遇到流错误会用 panic 中止连接。
	result := selector.DoneInfo{Err: http.ErrAbortHandler}
	defer func() { done(ctx, result) }()
	h.proxy.ServeHTTP(state, r.WithContext(ctx))
	result.Err = state.err
	return state.err
}

// writeSelectError 节点选择失败响应：无可用节点回 503（带重试语义），其他选择错误回 502。
func (h *ForwardHandler) writeSelectError(w http.ResponseWriter, err error) {
	if errors.Is(err, selector.ErrNoAvailable) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "gateway: no healthy instance", http.StatusServiceUnavailable)
		return
	}
	h.logger.Error("选择节点失败", "error", err)
	http.Error(w, "gateway: select error", http.StatusBadGateway)
}

// attemptKey 转发尝试状态上下文键。
type attemptKey struct{}

// attemptState 保存单次转发的节点、升级响应头和错误，并通知半开探测结果。
type attemptState struct {
	http.ResponseWriter
	node                  selector.Node
	upgradeHeaders        map[string]string
	upstreamUpgradeHeader http.Header
	onResponse            func()
	err                   error
}

// Unwrap 暴露原始响应写入器，供反向代理透传 Flush 等操作。
func (w *attemptState) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// Hijack 在代理校验上游握手并接管连接后覆盖升级响应头，结束半开探测。
func (w *attemptState) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err != nil {
		return conn, rw, err
	}
	for key, value := range w.upgradeHeaders {
		// 代理会将上游响应头追加到下游，先移除已有值以保持覆盖语义。
		w.Header().Del(key)
		w.upstreamUpgradeHeader.Set(key, value)
	}
	if w.onResponse != nil {
		w.onResponse()
	}
	return conn, rw, nil
}

// newProxy 构造共享反向代理：节点信息经请求上下文传递（Rewrite/ModifyResponse/ErrorHandler 读取）。
func newProxy() *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Transport: proxyTransport,
		// 全局禁缓冲：SSE/WS 流式透传；对普通小响应仅增加即时 flush，开销可忽略
		FlushInterval: -1,
		Rewrite: func(pr *httputil.ProxyRequest) {
			node := pr.Out.Context().Value(attemptKey{}).(*attemptState).node
			pr.Out.URL.Scheme = node.Scheme()
			pr.Out.URL.Host = node.Address()
			// X-Forwarded-For/Host/Proto 规范设置；pr.Out.Host 默认继承入站 Host（保留原始 Host）
			pr.SetXForwarded()
		},
		ModifyResponse: func(resp *http.Response) error {
			state := resp.Request.Context().Value(attemptKey{}).(*attemptState)
			resp.Header.Set("X-GW-Upstream", state.node.Address())
			if resp.StatusCode == http.StatusSwitchingProtocols {
				state.upstreamUpgradeHeader = resp.Header
			} else if state.onResponse != nil {
				state.onResponse()
			}
			return nil
		},
		// 出站请求失败（未写出任何响应字节）：仅暂存错误，由 ServeHTTP 决定重试或回 502
		ErrorHandler: func(_ http.ResponseWriter, r *http.Request, err error) {
			if state, ok := r.Context().Value(attemptKey{}).(*attemptState); ok {
				state.err = err
			}
		},
	}
}

// safeMethod 判断是否幂等安全方法（重试不产生副作用）。
func safeMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return true
	default:
		return false
	}
}
