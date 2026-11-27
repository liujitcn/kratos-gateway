package config

import (
	"encoding/json"
	"fmt"
	"time"

	configv1 "github.com/liujitcn/kratos-kit/api/gen/go/config/v1"
)

// Duration 兼容 "10s" 字符串写法的时长配置字段。
type Duration time.Duration

// UnmarshalJSON 实现 json.Unmarshaler：支持 "10s" 字符串与整数纳秒两种形式。
func (d *Duration) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		v, parseErr := time.ParseDuration(text)
		if parseErr != nil {
			return fmt.Errorf("解析时长 %q: %w", text, parseErr)
		}
		*d = Duration(v)
		return nil
	}
	var nanos int64
	if err := json.Unmarshal(data, &nanos); err != nil {
		return fmt.Errorf("解析时长 %s: %w", data, err)
	}
	*d = Duration(nanos)
	return nil
}

// Std 返回标准库 time.Duration 形式。
func (d Duration) Std() time.Duration {
	return time.Duration(d)
}

// Config 网关完整配置（configs/config.yaml 的结构映射）。
type Config struct {
	// Server 网关 HTTP 服务配置。
	Server Server `json:"server"`
	// Registry 注册中心配置（与 kratos-admin registry.yaml 同构）。
	Registry Registry `json:"registry"`
	// Catalog 服务自描述路由聚合配置（v0.3）。
	Catalog Catalog `json:"catalog"`
	// Routes 静态路由配置。
	Routes []Route `json:"routes"`
}

// Server 网关 HTTP 服务配置。
type Server struct {
	// Addr 监听地址。
	Addr string `json:"addr"`
	// ReadHeaderTimeout 请求头读取超时（防 slowloris 慢连接；禁止设置全局 Read/WriteTimeout，保 SSE/WS 长连接）。
	ReadHeaderTimeout Duration `json:"read_header_timeout"`
	// IdleTimeout 空闲连接超时。
	IdleTimeout Duration `json:"idle_timeout"`
	// ShutdownTimeout 优雅停机排空超时，超时后强制断开剩余连接。
	ShutdownTimeout Duration `json:"shutdown_timeout"`
	// Stats 是否开放 /gw-stats 统计端点（包含内网拓扑，默认关闭）。
	Stats *bool `json:"stats"`
	// Metrics 是否开放 /metrics Prometheus 端点（默认开放）。
	Metrics *bool `json:"metrics"`
	// Register 网关自身是否注册到注册中心（取决于部署形态，默认关闭）。
	Register bool `json:"register"`
}

// Registry 注册中心配置：字段与 kratos-admin registry.yaml 同构，加载后转换为 kit 工厂结构。
type Registry struct {
	// Type 注册中心类型（consul / etcd / nacos 等，由 kit 工厂分发）。
	Type string `json:"type"`
	// Consul Consul 配置（type=consul 时生效）。
	Consul *Consul `json:"consul"`
}

// Consul Consul 注册中心配置。
type Consul struct {
	// Address Consul 服务地址（host:port）。
	Address string `json:"address"`
	// Scheme 访问协议（http/https）。
	Scheme string `json:"scheme"`
	// Token Consul ACL 访问令牌。
	Token string `json:"token"`
	// HealthCheck 是否启用 Consul 健康检查。
	HealthCheck bool `json:"health_check"`
}

// KitRegistry 转换为 kit registry 工厂所需的 configv1.Registry。
func (r *Registry) KitRegistry() *configv1.Registry {
	kit := &configv1.Registry{Type: r.Type}
	if r.Consul != nil {
		kit.Consul = &configv1.Registry_Consul{
			Scheme:      r.Consul.Scheme,
			Address:     r.Consul.Address,
			HealthCheck: r.Consul.HealthCheck,
			Token:       r.Consul.Token,
		}
	}
	return kit
}

// Catalog 服务自描述路由聚合配置（v0.3）。
type Catalog struct {
	// Enabled 是否启用服务目录聚合。
	Enabled bool `json:"enabled"`
	// ServicePrefixes 只聚合服务名命中这些前缀的服务（空表示聚合全部声明了 gateway_routes 的服务）。
	ServicePrefixes []string `json:"service_prefixes"`
	// PollInterval 目录轮询间隔。
	PollInterval Duration `json:"poll_interval"`
}

// Route 路由配置。
type Route struct {
	// ID 路由唯一标识。
	ID string `json:"id"`
	// Predicates 谓词列表（全部命中才匹配；一项仅声明一种谓词）。
	Predicates []Predicate `json:"predicates"`
	// Filters 路由级过滤器配置（按 Order 升序、声明顺序稳定排序后装配）。
	Filters []Filter `json:"filters"`
	// Target 转发目标。
	Target Target `json:"target"`
}

// Predicate 谓词配置：path / method / header / weight 四选一。
type Predicate struct {
	// Path 段前缀路径谓词（命中自身与子路径，不误伤同前缀串）。
	Path string `json:"path"`
	// Method HTTP 方法谓词（命中任一）。
	Method []string `json:"method"`
	// Header 请求头谓词（键值全部精确命中）。
	Header map[string]string `json:"header"`
	// Weight 灰度权重谓词。
	Weight *Weight `json:"weight"`
}

// Weight 灰度分流配置：同组路由按权重切分流量，基于请求特征稳定哈希。
type Weight struct {
	// Group 分流组名。
	Group string `json:"group"`
	// Weight 组内流量占比（同组权重之和必须为 100）。
	Percent int `json:"weight"`
	// Key 稳定哈希特征键（请求头名），组内必须一致，为空时使用客户端 IP。
	Key string `json:"key"`
}

// Filter 路由级过滤器配置：按名称查找内置工厂构造。
type Filter struct {
	// Name 过滤器名称（内置：rewrite / add-header / response-header / rate-limit / timeout）。
	Name string `json:"name"`
	// Args 过滤器参数。
	Args map[string]any `json:"args"`
}

// Target 转发目标配置。
type Target struct {
	// Service 目标服务名（经 NormalizeForRegistry 规范化后查询注册中心）。
	Service string `json:"service"`
	// Scheme 上游协议（http/https，从实例 Endpoints 中筛选，默认 http）。
	Scheme string `json:"scheme"`
	// LB 负载均衡算法（round_robin / wrr，默认 round_robin）。
	LB string `json:"lb"`
	// Version 仅转发该版本实例（元数据 version 优先，兼容实例 Version；空为全版本）。
	Version string `json:"version"`
	// Sticky 连接级实例粘性：同一客户端连接按上游池固定节点（SSE/WS 路由用）。
	Sticky bool `json:"sticky"`
	// URLs 静态地址列表（联调/降级；非空时忽略 service 走静态转发）。
	URLs []string `json:"urls"`
	// MaxRetry 连接类错误重试次数，须为非负数（安全方法且请求体为空或可重放时生效，默认 1）。
	MaxRetry *int `json:"max_retry"`
	// Breaker 节点被动熔断参数。同一服务发现上游池共享一组配置，路由可省略以继承池配置。
	Breaker *Breaker `json:"breaker"`
}

// Breaker 节点级被动熔断配置。
type Breaker struct {
	// FailureThreshold 连续失败熔断阈值。
	FailureThreshold int `json:"failure_threshold"`
	// OpenDuration 熔断打开时长（冷却后半开放行单笔探测）。
	OpenDuration Duration `json:"open_duration"`
}

// applyDefaults 填充各字段缺省值（仅未显式配置时生效；路由 target 的 scheme/lb 缺省由 builder 兜底）。
func applyDefaults(cfg *Config) {
	if cfg.Server.Addr == "" {
		cfg.Server.Addr = ":8080"
	}
	if cfg.Server.ReadHeaderTimeout == 0 {
		cfg.Server.ReadHeaderTimeout = Duration(10 * time.Second)
	}
	if cfg.Server.IdleTimeout == 0 {
		cfg.Server.IdleTimeout = Duration(120 * time.Second)
	}
	if cfg.Server.ShutdownTimeout == 0 {
		cfg.Server.ShutdownTimeout = Duration(30 * time.Second)
	}
	if cfg.Catalog.PollInterval == 0 {
		cfg.Catalog.PollInterval = Duration(5 * time.Second)
	}
}
