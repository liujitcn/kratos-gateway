package config

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

const testYAML = `
server:
  addr: :9090
registry:
  type: consul
  consul:
    address: 127.0.0.1:8500
    scheme: http
    token: "${GW_TEST_TOKEN}"
routes:
  - id: demo
    predicates:
      - path: /api/v1/demo
    filters:
      - name: add-header
        args:
          key: X-Mark
          value: demo
    target:
      service: demo-service
`

// writeConfig 写入测试配置文件。
func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("写入测试配置失败: %v", err)
	}
	return path
}

// TestLoad 验证配置加载：结构解析、缺省值填充、${VAR} 环境变量展开。
func TestLoad(t *testing.T) {
	const token = "a\"b\\c\n${LITERAL}"
	t.Setenv("GW_TEST_TOKEN", token)
	path := writeConfig(t, testYAML)
	cfg, _, err := Load(path)
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	if cfg.Server.Addr != ":9090" {
		t.Errorf("监听地址解析错误: %s", cfg.Server.Addr)
	}
	if cfg.Registry.Consul.Token != token {
		t.Errorf("token 占位符未展开: %q", cfg.Registry.Consul.Token)
	}
	if cfg.Server.ReadHeaderTimeout.Std() != 10*time.Second {
		t.Errorf("ReadHeaderTimeout 缺省值错误: %s", cfg.Server.ReadHeaderTimeout.Std())
	}
	if cfg.Server.IdleTimeout.Std() != 120*time.Second {
		t.Errorf("IdleTimeout 缺省值错误: %s", cfg.Server.IdleTimeout.Std())
	}
	if cfg.Server.ShutdownTimeout.Std() != 30*time.Second {
		t.Errorf("ShutdownTimeout 缺省值错误: %s", cfg.Server.ShutdownTimeout.Std())
	}
	if len(cfg.Routes) != 1 {
		t.Fatalf("路由解析错误: %+v", cfg.Routes)
	}
	filters := cfg.Routes[0].Filters
	if len(filters) != 1 {
		t.Fatalf("过滤器解析错误: %+v", filters)
	}
	if args := filters[0].Args; args["key"] != "X-Mark" || args["value"] != "demo" {
		t.Errorf("过滤器参数解析错误: %+v", args)
	}
}

// TestLoadMissingEnv 验证未设置的环境变量占位符导致加载报错（快速暴露配置缺失）。
func TestLoadMissingEnv(t *testing.T) {
	path := writeConfig(t, `
registry:
  type: consul
  consul:
    token: "${GW_MISSING_TOKEN}"
routes: []
`)
	_, _, err := Load(path)
	if err == nil {
		t.Fatal("未设置的环境变量应导致加载失败")
	}
	if !strings.Contains(err.Error(), "GW_MISSING_TOKEN") {
		t.Errorf("错误信息应包含未设置变量名: %v", err)
	}
}

// TestLoadMalformedYAMLDoesNotLogToken 验证解析失败时不会记录已展开的令牌。
func TestLoadMalformedYAMLDoesNotLogToken(t *testing.T) {
	const token = "secret-token-for-test"
	t.Setenv("GW_TEST_TOKEN", token)
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	path := writeConfig(t, testYAML+"\ninvalid: [\n")
	_, _, err := Load(path)
	if err == nil {
		t.Fatal("无效的 YAML 应加载失败")
	}
	if strings.Contains(err.Error(), token) || strings.Contains(logs.String(), token) {
		t.Fatal("解析错误泄漏了环境变量的值")
	}
}

// TestDurationUnmarshal 验证时长字段支持 "10s" 字符串形式。
func TestDurationUnmarshal(t *testing.T) {
	path := writeConfig(t, "server:\n  read_header_timeout: 3s\n")
	cfg, _, err := Load(path)
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	if cfg.Server.ReadHeaderTimeout.Std() != 3*time.Second {
		t.Errorf("时长解析错误: %s", cfg.Server.ReadHeaderTimeout.Std())
	}
}

// TestWatchRoutes 验证路由热更新：文件内容变化后回调收到新配置。
func TestWatchRoutes(t *testing.T) {
	t.Setenv("GW_TEST_TOKEN", "secret-token")
	path := writeConfig(t, testYAML)
	_, file, err := load(path, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	got := make(chan []Route, 1)
	go file.WatchRoutes(t.Context(), slog.New(slog.DiscardHandler), func(routes []Route) { got <- routes })

	updated := "server:\n  addr: :9090\nregistry:\n  type: consul\nroutes:\n  - id: demo2\n    predicates:\n      - path: /api/v2/demo\n    target:\n      service: demo-service\n"
	if err = os.WriteFile(path, []byte(updated), 0o644); err != nil {
		t.Fatalf("更新配置失败: %v", err)
	}

	select {
	case routes := <-got:
		if len(routes) != 1 || routes[0].ID != "demo2" {
			t.Errorf("热更新配置内容错误: %+v", routes)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("等待热更新回调超时")
	}
}

// TestWatchRoutesAddAndRemove 验证从缺省配置新增路由后也能清空路由表。
func TestWatchRoutesAddAndRemove(t *testing.T) {
	t.Setenv("GW_TEST_TOKEN", "secret-token")
	const initial = "server:\n  addr: :9090\n"
	path := writeConfig(t, initial)
	_, file, err := load(path, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan []Route, 2)
	go file.WatchRoutes(t.Context(), slog.New(slog.DiscardHandler), func(routes []Route) { got <- routes })
	for _, step := range []struct {
		content string
		count   int
	}{{testYAML, 1}, {initial, 0}} {
		if err = os.WriteFile(path, []byte(step.content), 0o644); err != nil {
			t.Fatal(err)
		}
		select {
		case routes := <-got:
			if len(routes) != step.count {
				t.Fatalf("路由数量为 %d，期望 %d", len(routes), step.count)
			}
		case <-time.After(time.Second):
			t.Fatalf("等待路由数量更新为 %d 超时", step.count)
		}
	}
}

// TestWatchRoutesInvalidUpdate 验证格式错误或含未知字段的配置保留旧路由，合法省略 routes 可以清空。
func TestWatchRoutesInvalidUpdate(t *testing.T) {
	t.Setenv("GW_TEST_TOKEN", "secret-token")
	synctest.Test(t, func(t *testing.T) {
		const interval = 20 * time.Millisecond
		path := writeConfig(t, testYAML)
		_, file, err := load(path, interval)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		got := make(chan []Route, 2)
		go file.WatchRoutes(ctx, slog.New(slog.DiscardHandler), func(routes []Route) { got <- routes })

		for _, invalid := range []string{"routes: [", "route: []\n", "routes:\n  - id: demo\n    targte: {}\n"} {
			if err = os.WriteFile(path, []byte(invalid), 0o644); err != nil {
				t.Fatal(err)
			}
			time.Sleep(2 * interval)
			synctest.Wait()
			select {
			case <-got:
				t.Fatalf("配置解析失败后不应发布新路由: %q", invalid)
			default:
			}
		}

		if err = os.WriteFile(path, []byte("server:\n  addr: :9090\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		select {
		case routes := <-got:
			if len(routes) != 0 {
				t.Fatalf("修正文件后应收到空路由表，实际 %+v", routes)
			}
		case <-time.After(5 * interval):
			t.Fatal("修正文件后未发布新路由")
		}
	})
}
