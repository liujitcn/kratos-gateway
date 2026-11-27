package gateway

import (
	"github.com/go-kratos/kratos/v3"
	"github.com/google/wire"
	"github.com/liujitcn/kratos-kit/bootstrap"

	"github.com/liujitcn/kratos-gateway/internal/config"
	"github.com/liujitcn/kratos-gateway/internal/module"
)

// ProviderSet 汇总网关应用的 Wire 提供者。
var ProviderSet = wire.NewSet(config.ProviderSet, module.NewModule, NewApp)

// NewApp 使用共享引导流程装配应用、服务及注册器。
func NewApp(ctx *bootstrap.Context, gateway *module.Module) *kratos.App {
	return bootstrap.NewApp(ctx, gateway)
}
