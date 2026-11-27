package config

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	kratosconfig "github.com/go-kratos/kratos/v3/config"
	"github.com/go-kratos/kratos/v3/config/file"
	kitconfig "github.com/liujitcn/kratos-kit/config"
	"github.com/liujitcn/kratos-kit/sdk"

	configv1 "github.com/liujitcn/kratos-gateway/api/gen/go/gateway/config/v1"
)

const defaultPollInterval = 2 * time.Second

// configKeyPurpose 与 kit 的配置密文用途一致，热更新沿用启动时的密钥。
const configKeyPurpose = "kratos-kit:config"

// LocalFiles 保存与引导流程相同的配置目录和环境。
type LocalFiles struct {
	Dir string
	Env string
}

// WatchRoutes 轮询本地网关配置，只发布静态路由变化并在更新失败时保留旧路由。
func (f LocalFiles) WatchRoutes(ctx context.Context, logger *slog.Logger, routes []Route, publish func([]Route)) {
	key, err := sdk.Runtime.GetKey().Derive(ctx, configKeyPurpose)
	if err != nil {
		logger.Warn("无法派生配置密钥，保留启动路由表", "error", err)
		return
	}
	var cipher *kitconfig.SecretCipher
	cipher, err = kitconfig.NewSecretCipher(key)
	if err != nil {
		logger.Warn("无法初始化配置解密器，保留启动路由表", "error", err)
		return
	}
	f.watchRoutes(ctx, logger, routes, publish, cipher.Decoder(), defaultPollInterval)
}

// watchRoutes 按指定间隔读取有效配置，只有路由变化才发布。
func (f LocalFiles) watchRoutes(ctx context.Context, logger *slog.Logger, routes []Route, publish func([]Route), decoder kratosconfig.Decoder, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		cfg, err := f.loadGateway(decoder)
		if err != nil {
			logger.Warn("热更新加载配置失败，保留旧路由表", "error", err)
			continue
		}
		if !reflect.DeepEqual(routes, cfg.Routes) {
			routes = cfg.Routes
			publish(routes)
		}
	}
}

// loadGateway 按 kit 的基础文件和环境覆盖文件顺序合并配置，保留跨文件引用。
func (f LocalFiles) loadGateway(decoder kratosconfig.Decoder) (*Config, error) {
	entries, err := os.ReadDir(f.Dir)
	if err != nil {
		return nil, err
	}
	var base, override []kratosconfig.Source
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || strings.HasPrefix(name, ".") || name == "root.key" || strings.HasPrefix(name, "key.") {
			continue
		}
		ext := filepath.Ext(name)
		if ext == "" {
			continue
		}
		stem := strings.TrimSuffix(name, ext)
		if !strings.Contains(stem, ".") {
			base = append(base, file.NewSource(filepath.Join(f.Dir, name)))
		} else if prefix, ok := strings.CutSuffix(stem, "."+f.Env); f.Env != "" && ok && !strings.Contains(prefix, ".") {
			override = append(override, file.NewSource(filepath.Join(f.Dir, name)))
		}
	}
	var value configv1.GatewayConfigWrapper
	if err = kitconfig.LoadConfigWithoutWatch(append(base, override...), decoder, &value); err != nil {
		return nil, errors.New("gateway 配置文件格式无效")
	}
	return decodeConfig(value.GetGateway().AsMap())
}
