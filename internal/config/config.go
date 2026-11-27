package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
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

// Config 表示独立的 gateway 配置块。
type Config struct {
	// Server 网关 HTTP 服务配置。
	Server Server `json:"server"`
	// Catalog 服务自描述路由聚合配置（v0.3）。
	Catalog Catalog `json:"catalog"`
	// Routes 静态路由配置。
	Routes []Route `json:"routes"`
}

// Server 网关 HTTP 服务配置。
type Server struct {
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

// decodeConfig 将网关配置树解码为运行时配置，拒绝未知字段。
func decodeConfig(values map[string]any) (*Config, error) {
	data, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("配置包含无法转换的 YAML 值: %w", err)
	}
	cfg := &Config{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(cfg); err != nil {
		// 自定义解码器的错误可能引用展开后的值，只报告字段名或结构错误。
		if typeErr, ok := errors.AsType[*json.UnmarshalTypeError](err); ok && typeErr.Field != "" {
			return nil, fmt.Errorf("配置字段 %s 类型无效", typeErr.Field)
		}
		return nil, errors.New("配置含未知字段或字段格式无效")
	}
	applyDefaults(cfg)
	return cfg, nil
}

// applyDefaults 填充各字段缺省值（仅未显式配置时生效；路由 target 的 scheme/lb 缺省由 builder 兜底）。
func applyDefaults(cfg *Config) {
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
