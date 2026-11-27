package main

import (
	"context"
	"os"

	"github.com/go-kratos/kratos/v3"
	"github.com/go-kratos/kratos/v3/log"
	configv1 "github.com/liujitcn/kratos-kit/api/gen/go/config/v1"
	"github.com/liujitcn/kratos-kit/bootstrap"
	_ "github.com/liujitcn/kratos-kit/logger/zap"
	_ "github.com/liujitcn/kratos-kit/registry/consul"
	"github.com/spf13/cobra"

	"github.com/liujitcn/kratos-gateway/internal/config"
)

// main 使用统一的引导流程启动网关。
func main() {
	ctx := bootstrap.NewContext(context.Background(), &configv1.AppInfo{
		Project: "kratos",
		AppId:   "gateway",
		Name:    "Kratos Gateway",
	})
	config.Register(ctx)
	var files config.LocalFiles
	err := bootstrap.RunApp(ctx, func(ctx *bootstrap.Context) (*kratos.App, func(), error) {
		return NewApp(ctx, files)
	}, func(cmd *cobra.Command) {
		cmd.PreRunE = func(cmd *cobra.Command, _ []string) error {
			// kit 已完成参数解析，此处保留同一配置目录和环境供本地路由热更新。
			files.Dir = cmd.Flags().Lookup("conf").Value.String()
			files.Env = cmd.Flags().Lookup("env").Value.String()
			return nil
		}
	})
	if err != nil {
		log.Error("服务启动失败", "error", err)
		os.Exit(1)
	}
}
