package data

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-kratos/kratos/v3/registry"
	consulapi "github.com/hashicorp/consul/api"
	configv1 "github.com/liujitcn/kratos-kit/api/gen/go/config/v1"
	kitreg "github.com/liujitcn/kratos-kit/registry"

	"github.com/liujitcn/kratos-gateway/internal/config"
	"github.com/liujitcn/kratos-gateway/internal/data/catalog"
	"github.com/liujitcn/kratos-gateway/internal/data/upstream"
)

// NewDiscovery 为 Consul 创建健康订阅客户端，其他注册中心沿用 kit 工厂。
func NewDiscovery(cfg *configv1.Registry) (registry.Discovery, error) {
	if cfg.GetType() != string(kitreg.Consul) {
		return kitreg.NewDiscovery(cfg)
	}
	if cfg.GetConsul() == nil {
		return nil, fmt.Errorf("缺少 registry.consul 配置")
	}
	client, err := newConsulClient(cfg.GetConsul(), 0)
	if err != nil {
		return nil, err
	}
	return upstream.NewConsulDiscovery(client), nil
}

// RunCatalog 启动服务目录聚合循环，仅查询匹配前缀的服务。
func RunCatalog(ctx context.Context, cfg config.Catalog, consul *configv1.Registry_Consul, logger *slog.Logger, fn func([]config.Route)) {
	client, err := newConsulClient(consul, 10*time.Second)
	if err != nil {
		logger.Error("创建 Consul 客户端失败，服务自描述路由不可用", "error", err)
		return
	}
	prefixes := cfg.ServicePrefixes
	c := catalog.New(catalog.NewConsulLister(client, prefixes), prefixes, cfg.PollInterval.Std(), logger)
	c.Run(ctx, fn)
}

// newConsulClient 应用 Consul 配置，按调用场景设置 HTTP 期限。
func newConsulClient(cfg *configv1.Registry_Consul, timeout time.Duration) (*consulapi.Client, error) {
	options := consulapi.DefaultConfig()
	if cfg != nil {
		options.Address = cfg.Address
		options.Scheme = cfg.Scheme
		options.Token = cfg.Token
	}
	client, err := consulapi.NewClient(options)
	if err != nil {
		return nil, err
	}
	// 目录轮询使用有限的 HTTP 超时，服务发现保留 Consul 长轮询。
	options.HttpClient.Timeout = timeout
	return client, nil
}
