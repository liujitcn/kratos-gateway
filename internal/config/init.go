package config

import (
	"errors"

	"github.com/google/wire"
	"github.com/liujitcn/kratos-kit/bootstrap"

	configv1 "github.com/liujitcn/kratos-gateway/api/gen/go/gateway/config/v1"
)

const gatewayConfigKey = "gateway"

// ProviderSet 汇总网关配置的依赖注入提供者。
var ProviderSet = wire.NewSet(FromBootstrap)

// Register 将网关配置注册给共享引导流程。
func Register(ctx *bootstrap.Context) {
	ctx.RegisterCustomConfig(gatewayConfigKey, &configv1.GatewayConfigWrapper{})
}

// FromBootstrap 读取引导流程已经加载的网关配置并校验字段。
func FromBootstrap(ctx *bootstrap.Context) (*Config, error) {
	value, ok := ctx.GetCustomConfig(gatewayConfigKey)
	if !ok {
		return nil, errors.New("未注册 gateway 配置")
	}
	return decodeConfig(value.(*configv1.GatewayConfigWrapper).GetGateway().AsMap())
}
