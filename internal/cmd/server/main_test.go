package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// TestBootstrapRegistersAndServes 验证环境覆盖、扩展配置、共享请求 ID 及 Consul 注册注销。
func TestBootstrapRegistersAndServes(t *testing.T) {
	type registration struct {
		ID, Name string
		Port     int
	}
	registered := make(chan registration, 1)
	deregistered := make(chan string, 1)
	consul := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/agent/service/register":
			var service registration
			if json.NewDecoder(r.Body).Decode(&service) == nil {
				registered <- service
			}
		case strings.HasPrefix(r.URL.Path, "/v1/agent/service/deregister/"):
			deregistered <- strings.TrimPrefix(r.URL.Path, "/v1/agent/service/deregister/")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer consul.Close()
	dir := t.TempDir()
	files := map[string]string{
		"server.yaml":              "server:\n  http:\n    addr: 127.0.0.1:0\n",
		"registry.yaml":            "registry:\n  type: consul\n  consul:\n    address: " + consul.Listener.Addr().String() + "\n",
		"gateway.yaml":             "gateway:\n  server:\n    stats: false\n",
		"gateway.integration.yaml": "gateway:\n  server:\n    stats: true\n",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	cmd := bootstrapProcess(ctx, dir)
	var logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var service registration
	select {
	case service = <-registered:
	case err := <-done:
		t.Fatalf("注册前退出: %v\n%s", err, logs.String())
	case <-ctx.Done():
		t.Fatal("未注册到 Consul")
	}
	if service.ID == "" || service.Name != "test-edge" || service.Port <= 0 {
		t.Fatalf("注册信息错误: %+v", service)
	}
	checkBootstrapHTTP(t, ctx, service.Port)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()
	updated := fmt.Sprintf("gateway:\n  routes:\n    - id: live\n      predicates: [{path: /live}]\n      target: {urls: [%q]}\n", upstream.URL)
	err := os.WriteFile(filepath.Join(dir, "gateway.integration.yaml"), []byte(updated), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	checkHotRoute(t, ctx, service.Port)
	err = cmd.Process.Signal(syscall.SIGTERM)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err != nil {
			t.Fatalf("网关退出失败: %v\n%s", err, logs.String())
		}
	case <-ctx.Done():
		t.Fatal("网关停机超时")
	}
	select {
	case id := <-deregistered:
		if id != service.ID {
			t.Fatalf("注销 ID %q 不匹配 %q", id, service.ID)
		}
	default:
		t.Fatal("退出前未注销 Consul 实例")
	}
}

// TestBootstrapProcess 通过实际入口执行启动测试。
func TestBootstrapProcess(t *testing.T) {
	if path := os.Getenv("GW_BOOTSTRAP_TEST_DIR"); path != "" {
		os.Args = []string{os.Args[0], "-c", path, "-e", "integration", "-p", "test", "-a", "edge"}
		main()
	}
}

// checkBootstrapHTTP 检查扩展配置生效且探针经过共享传输中间件。
func checkBootstrapHTTP(t *testing.T, ctx context.Context, port int) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/gw-stats", port), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Request-Id", "gateway-bootstrap-test")
	var resp *http.Response
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Request-Id") != "gateway-bootstrap-test" {
		t.Fatalf("状态 %d，请求 ID %q", resp.StatusCode, resp.Header.Get("X-Request-Id"))
	}
}

// TestBootstrapRejectsStartupBeforeRegistration 验证初始路由错误或端口占用时不注册实例。
func TestBootstrapRejectsStartupBeforeRegistration(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var registrations atomic.Int32
	consul := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		registrations.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer consul.Close()
	for _, tc := range []struct {
		name, httpAddress, gateway, errorText string
	}{
		{"invalid_routes", "127.0.0.1:0", "gateway: {routes: [{id: missing-target, predicates: [{path: /demo}]}]}\n", "missing-target"},
		{"occupied_port", listener.Addr().String(), "gateway: {}\n", "address already in use"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, contents := range map[string]string{
				"server.yaml":   "server: {http: {addr: '" + tc.httpAddress + "'}}\n",
				"registry.yaml": "registry: {type: consul, consul: {address: '" + consul.Listener.Addr().String() + "'}}\n",
				"gateway.yaml":  tc.gateway,
			} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			output, err := bootstrapProcess(ctx, dir).CombinedOutput()
			if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 || !strings.Contains(string(output), tc.errorText) {
				t.Fatalf("无效配置或监听失败应使进程退出，错误 %v，日志 %s", err, output)
			}
			if got := registrations.Load(); got != 0 {
				t.Fatalf("失败启动不应访问 Consul 注册端点，实际 %d 次", got)
			}
		})
	}
}

// bootstrapProcess 创建使用临时配置目录的网关子进程。
func bootstrapProcess(ctx context.Context, dir string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBootstrapProcess$")
	cmd.Env = append(os.Environ(), "GW_BOOTSTRAP_TEST_DIR="+dir)
	return cmd
}

// checkHotRoute 等待环境配置中的路由更新通过真实入口生效。
func checkHotRoute(t *testing.T, ctx context.Context, port int) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatal("路由热更新未生效")
		case <-ticker.C:
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/live", port), nil)
		if err != nil {
			t.Fatal(err)
		}
		var resp *http.Response
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(io.Discard, resp.Body)
		if closeErr := resp.Body.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode == http.StatusNoContent {
			return
		}
	}
}
