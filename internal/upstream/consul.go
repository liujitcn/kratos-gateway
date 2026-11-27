package upstream

import (
	"context"
	"net"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v3/registry"
	consulapi "github.com/hashicorp/consul/api"
	"golang.org/x/time/rate"
)

// ConsulDiscovery 使用 Consul 健康查询实现服务发现。
type ConsulDiscovery struct {
	health *consulapi.Health
}

// NewConsulDiscovery 创建支持完整健康快照的服务发现客户端。
func NewConsulDiscovery(client *consulapi.Client) *ConsulDiscovery {
	return &ConsulDiscovery{health: client.Health()}
}

// GetService 查询指定服务当前的健康实例，允许返回空列表。
func (d *ConsulDiscovery) GetService(ctx context.Context, name string) ([]*registry.ServiceInstance, error) {
	instances, _, err := d.query(ctx, name, 0)
	return instances, err
}

// Watch 创建独立的可取消订阅，由 Next 读取首个快照并监听后续变化。
func (d *ConsulDiscovery) Watch(ctx context.Context, name string) (registry.Watcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	return &consulWatcher{
		discovery: d,
		name:      name,
		ctx:       ctx,
		cancel:    cancel,
		limiter:   rate.NewLimiter(rate.Every(time.Second), 2),
	}, nil
}

// query 执行一次健康查询，为长轮询预留 Consul 的等待时间和抖动。
func (d *ConsulDiscovery) query(ctx context.Context, name string, index uint64) ([]*registry.ServiceInstance, uint64, error) {
	opts := &consulapi.QueryOptions{WaitIndex: index}
	timeout := 10 * time.Second
	if index != 0 {
		opts.WaitTime = 55 * time.Second
		timeout = 65 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	entries, meta, err := d.health.Service(name, "", true, opts.WithContext(ctx))
	if err != nil {
		return nil, 0, err
	}
	instances := make([]*registry.ServiceInstance, 0, len(entries))
	for _, entry := range entries {
		service := entry.Service
		var version string
		for _, tag := range service.Tags {
			if v, ok := strings.CutPrefix(tag, "version="); ok {
				version = v
			}
		}
		endpoints := make([]string, 0, len(service.TaggedAddresses))
		for tag, addr := range service.TaggedAddresses {
			switch tag {
			case "lan_ipv4", "wan_ipv4", "lan_ipv6", "wan_ipv6":
				continue
			}
			endpoints = append(endpoints, addr.Address)
		}
		if len(endpoints) == 0 && service.Address != "" && service.Port != 0 {
			endpoints = append(endpoints, "http://"+net.JoinHostPort(service.Address, strconv.Itoa(service.Port)))
		}
		slices.Sort(endpoints)
		instances = append(instances, &registry.ServiceInstance{
			ID:        service.ID,
			Name:      service.Service,
			Version:   version,
			Metadata:  service.Meta,
			Endpoints: endpoints,
		})
	}
	return instances, meta.LastIndex, nil
}

// consulWatcher 维护一个健康订阅的阻塞查询索引和最近一次快照。
type consulWatcher struct {
	discovery *ConsulDiscovery
	name      string
	ctx       context.Context
	cancel    context.CancelFunc
	limiter   *rate.Limiter
	index     uint64
	last      []*registry.ServiceInstance
}

// Next 发布变化后的成功快照，查询失败时让上游按退避策略重试。
func (w *consulWatcher) Next() ([]*registry.ServiceInstance, error) {
	for {
		err := w.limiter.Wait(w.ctx)
		if err != nil {
			return nil, err
		}
		var instances []*registry.ServiceInstance
		var index uint64
		instances, index, err = w.discovery.query(w.ctx, w.name, w.index)
		if err != nil {
			w.index = 0
			return nil, err
		}
		// 快照恢复等操作会让 Consul 索引回退，下一轮从零重建阻塞查询。
		if index < w.index {
			w.index = 0
		} else {
			w.index = max(index, 1)
		}
		if !reflect.DeepEqual(instances, w.last) {
			w.last = instances
			return instances, nil
		}
	}
}

// Stop 取消订阅并中止在途查询。
func (w *consulWatcher) Stop() error {
	w.cancel()
	return nil
}
