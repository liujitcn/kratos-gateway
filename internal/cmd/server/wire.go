//go:build wireinject

package main

import (
	"github.com/go-kratos/kratos/v3"
	"github.com/google/wire"
	"github.com/liujitcn/kratos-kit/bootstrap"

	gateway "github.com/liujitcn/kratos-gateway"
	"github.com/liujitcn/kratos-gateway/internal/config"
)

// NewApp 通过 Wire 装配网关应用。
func NewApp(ctx *bootstrap.Context, files config.LocalFiles) (*kratos.App, func(), error) {
	panic(wire.Build(gateway.ProviderSet))
}
