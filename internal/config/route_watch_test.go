package config

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"
)

// TestLocalFilesLoadGateway 验证环境覆盖、跨文件引用、路由列表替换及超时默认值。
func TestLocalFilesLoadGateway(t *testing.T) {
	files := LocalFiles{Dir: t.TempDir(), Env: "prod"}
	for name, content := range map[string]string{
		"common.yaml":       "values: { upstream: http://127.0.0.1:9000 }\n",
		"gateway.yaml":      "gateway:\n  server: {read_header_timeout: 3s, stats: false}\n  routes: [{id: base}, {id: another}]\n",
		"gateway.dev.yaml":  "gateway: {unknown: true}\n",
		"gateway.prod.yaml": "gateway:\n  server: {stats: true}\n  routes:\n    - id: prod\n      target: {urls: ['${values.upstream}']}\n",
	} {
		if err := os.WriteFile(filepath.Join(files.Dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := files.loadGateway(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Stats == nil || !*cfg.Server.Stats || cfg.Server.ReadHeaderTimeout.Std() != 3*time.Second || cfg.Server.ShutdownTimeout.Std() != 30*time.Second {
		t.Fatalf("HTTP 配置覆盖或缺省值错误: %+v", cfg.Server)
	}
	if len(cfg.Routes) != 1 || cfg.Routes[0].ID != "prod" || len(cfg.Routes[0].Target.URLs) != 1 || cfg.Routes[0].Target.URLs[0] != "http://127.0.0.1:9000" {
		t.Fatalf("路由合并错误: %+v", cfg.Routes)
	}
}

// TestWatchRoutes 验证热更新跳过非法配置并支持新增、替换和清空路由。
func TestWatchRoutes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const interval = 20 * time.Millisecond
		files := LocalFiles{Dir: t.TempDir(), Env: "prod"}
		path := filepath.Join(files.Dir, "gateway.prod.yaml")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		got := make(chan []Route, 1)
		go files.watchRoutes(ctx, slog.New(slog.DiscardHandler), nil, func(routes []Route) { got <- routes }, nil, interval)

		for _, step := range []struct {
			content string
			publish bool
			id      string
		}{
			{"gateway: {routes: [{id: first}]}\n", true, "first"},
			{"gateway: {routes: [", false, ""},
			{"gateway: {route: []}\n", false, ""},
			{"gateway: {routes: [{id: broken, targte: {}}]}\n", false, ""},
			{"gateway: {routes: [{id: second}]}\n", true, "second"},
			{"gateway: {server: {metrics: false}}\n", true, ""},
		} {
			if err := os.WriteFile(path, []byte(step.content), 0o600); err != nil {
				t.Fatal(err)
			}
			if !step.publish {
				time.Sleep(2 * interval)
				synctest.Wait()
				select {
				case <-got:
					t.Fatalf("热更新发布了无效配置: %q", step.content)
				default:
				}
				continue
			}
			select {
			case routes := <-got:
				if (step.id == "" && len(routes) != 0) || (step.id != "" && (len(routes) != 1 || routes[0].ID != step.id)) {
					t.Fatalf("热更新配置内容错误: %+v", routes)
				}
			case <-time.After(5 * interval):
				t.Fatalf("等待热更新配置 %q 超时", step.content)
			}
		}
	})
}
