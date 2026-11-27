package config

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// envPlaceholder 匹配 ${VAR} 形式的环境变量占位符。
var envPlaceholder = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// defaultPollInterval 配置文件轮询间隔缺省值（热更新探测）。
const defaultPollInterval = 2 * time.Second

// File 保存初始配置快照，供后续检测路由变更。
type File struct {
	path      string
	pollEvery time.Duration
	content   []byte
	routes    []Route
}

// Load 读取 YAML 配置，展开字符串中的环境变量并填充缺省值。
func Load(path string) (*Config, *File, error) {
	return load(path, defaultPollInterval)
}

// WatchRoutes 轮询完整文件并发布静态路由快照，直到上下文取消。解析失败时保留已发布的路由。
func (f *File) WatchRoutes(ctx context.Context, logger *slog.Logger, fn func([]Route)) {
	ticker := time.NewTicker(f.pollEvery)
	defer ticker.Stop()
	last, routes := f.content, f.routes
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		content, err := os.ReadFile(f.path)
		if err != nil {
			logger.Warn("热更新读取配置失败，保留旧路由表", "error", err)
			continue
		}
		if bytes.Equal(content, last) {
			continue
		}
		last = content
		var cfg *Config
		cfg, err = parseConfig(content)
		if err != nil {
			logger.Warn("热更新解析配置失败，保留旧路由表", "path", f.path, "error", err)
			continue
		}
		if reflect.DeepEqual(routes, cfg.Routes) {
			continue
		}
		routes = cfg.Routes
		fn(routes)
	}
}

// load 按指定轮询间隔加载配置并保留初始文件内容。
func load(path string, pollEvery time.Duration) (*Config, *File, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("读取配置 %s: %w", path, err)
	}
	var cfg *Config
	cfg, err = parseConfig(content)
	if err != nil {
		return nil, nil, fmt.Errorf("解析配置 %s: %w", path, err)
	}
	return cfg, &File{path: path, pollEvery: pollEvery, content: content, routes: cfg.Routes}, nil
}

// parseConfig 解析 YAML 并展开字符串值，转换为网关配置时拒绝未知字段。
func parseConfig(content []byte) (*Config, error) {
	var values map[string]any
	err := yaml.Unmarshal(content, &values)
	if err != nil {
		return nil, err
	}
	missing := map[string]struct{}{}
	expandEnv(values, missing)
	if len(missing) > 0 {
		names := make([]string, 0, len(missing))
		for name := range missing {
			names = append(names, name)
		}
		slices.Sort(names)
		return nil, fmt.Errorf("环境变量未设置: %s", strings.Join(names, ", "))
	}
	var data []byte
	data, err = json.Marshal(values)
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

// expandEnv 展开值中的占位符并收集缺失变量，替换内容保留为字符串。
func expandEnv(value any, missing map[string]struct{}) any {
	switch value := value.(type) {
	case string:
		return envPlaceholder.ReplaceAllStringFunc(value, func(match string) string {
			name := match[2 : len(match)-1]
			if replacement, ok := os.LookupEnv(name); ok {
				return replacement
			}
			missing[name] = struct{}{}
			return match
		})
	case map[string]any:
		for key, child := range value {
			value[key] = expandEnv(child, missing)
		}
	case map[any]any:
		converted := make(map[string]any, len(value))
		for key, child := range value {
			converted[fmt.Sprint(key)] = expandEnv(child, missing)
		}
		return converted
	case []any:
		for i, child := range value {
			value[i] = expandEnv(child, missing)
		}
	}
	return value
}
