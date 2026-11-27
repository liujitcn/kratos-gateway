// Package testutil 提供测试用的服务发现与监视器桩实现。
package testutil

import (
	"context"
	"sync"

	"github.com/go-kratos/kratos/v3/registry"
)

// FakeWatcher 可推送实例列表的假监视器：Next 阻塞等待推送或停止。
type FakeWatcher struct {
	mu     sync.Mutex
	ch     chan []*registry.ServiceInstance
	done   chan struct{}
	closed bool
}

// NewFakeWatcher 创建假监视器。
func NewFakeWatcher() *FakeWatcher {
	return &FakeWatcher{
		ch:   make(chan []*registry.ServiceInstance, 16),
		done: make(chan struct{}),
	}
}

// Push 推送新实例列表（触发 Next 返回）。
func (w *FakeWatcher) Push(ins []*registry.ServiceInstance) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	w.ch <- ins
}

// Next 实现 registry.Watcher：阻塞等待推送；停止后返回 context.Canceled。
func (w *FakeWatcher) Next() ([]*registry.ServiceInstance, error) {
	select {
	case ins := <-w.ch:
		return ins, nil
	case <-w.done:
		return nil, context.Canceled
	}
}

// Stop 实现 registry.Watcher。
func (w *FakeWatcher) Stop() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	close(w.done)
	return nil
}

// FakeDiscovery 假注册中心：按服务名维护监视器列表，支持向全部订阅者推送实例。
type FakeDiscovery struct {
	mu       sync.Mutex
	watchers map[string][]*FakeWatcher
}

// NewFakeDiscovery 创建假注册中心。
func NewFakeDiscovery() *FakeDiscovery {
	return &FakeDiscovery{watchers: make(map[string][]*FakeWatcher)}
}

// GetService 实现 registry.Discovery（桩实现，测试走 Watch 推送路径）。
func (d *FakeDiscovery) GetService(_ context.Context, _ string) ([]*registry.ServiceInstance, error) {
	return nil, nil
}

// Watch 实现 registry.Discovery：为服务名创建新监视器。
func (d *FakeDiscovery) Watch(_ context.Context, name string) (registry.Watcher, error) {
	w := NewFakeWatcher()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.watchers[name] = append(d.watchers[name], w)
	return w, nil
}

// Push 向指定服务的全部监视者推送实例列表。
func (d *FakeDiscovery) Push(name string, ins []*registry.ServiceInstance) {
	d.mu.Lock()
	watchers := append([]*FakeWatcher(nil), d.watchers[name]...)
	d.mu.Unlock()
	for _, w := range watchers {
		w.Push(ins)
	}
}
