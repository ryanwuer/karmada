# kubectl -w 监听在 K8s2 故障后断开的代码实现分析

## 问题描述

当通过 `kubectl -w` 对 search 组件开启监听后，在 K8s2 集群故障时，watch 连接会断开。本文档详细分析了导致这一行为的代码实现。

## 完整调用链路

```
kubectl -w
    ↓
Search API Server (proxy)
    ↓
MultiClusterCache.Watch() - pkg/search/proxy/store/multi_cluster_cache.go:330
    ↓
resourceCache.Watch() (genericregistry.Store) - pkg/search/proxy/store/resource_cache.go
    ↓
Cacher.Watch() - vendor/k8s.io/apiserver/pkg/storage/cacher/cacher.go:516
    ↓
底层 store.Watch() - pkg/search/proxy/store/store.go:145
    ↓
dynamic.Client.Watch() (to member cluster K8s2)
    ↓
[K8s2 网络故障，连接断开]
    ↓
reflector.ListAndWatch() 返回错误 - vendor/k8s.io/client-go/tools/cache/reflector.go:348
    ↓
Cacher.startCaching() 检测到错误 - vendor/k8s.io/apiserver/pkg/storage/cacher/cacher.go:477
    ↓
Cacher.terminateAllWatchers() - vendor/k8s.io/apiserver/pkg/storage/cacher/cacher.go:1310
    ↓
客户端 watch 连接断开
```

## 关键代码分析

### 1. Cacher 的初始化和错误处理逻辑

**文件:** `vendor/k8s.io/apiserver/pkg/storage/cacher/cacher.go`

```go
// 行 436-447: Cacher 启动时的 goroutine
cacher.stopWg.Add(1)
go func() {
    defer cacher.stopWg.Done()
    defer cacher.terminateAllWatchers()  // ⚠️ 退出时终止所有 watchers
    wait.Until(
        func() {
            if !cacher.isStopped() {
                cacher.startCaching(stopCh)
            }
        }, time.Second, stopCh,
    )
}()
```

**关键点:**
- Cacher 使用 `wait.Until` 循环调用 `startCaching`
- 每次循环间隔 1 秒
- 当 goroutine 退出时，会调用 `terminateAllWatchers()` 终止所有客户端连接

### 2. startCaching 方法 - 连接失败的检测点

**文件:** `vendor/k8s.io/apiserver/pkg/storage/cacher/cacher.go:452-480`

```go
func (c *Cacher) startCaching(stopChannel <-chan struct{}) {
    // 设置成功标志的回调
    successfulList := false
    c.watchCache.SetOnReplace(func() {
        successfulList = true
        c.ready.set(true)
        klog.V(1).Infof("cacher (%v): initialized", c.groupResource.String())
    })
    
    defer func() {
        if successfulList {
            c.ready.set(false)  // ⚠️ 标记 cache 不可用
        }
    }()

    c.terminateAllWatchers()  // ⚠️ 每次重启前，先终止所有现有 watchers
    
    // 启动 reflector 的 ListAndWatch
    if err := c.reflector.ListAndWatch(stopChannel); err != nil {
        klog.Errorf("cacher (%v): unexpected ListAndWatch error: %v; reinitializing...", 
                    c.groupResource.String(), err)
    }
    // ⚠️ 函数返回意味着连接断开，将触发 wait.Until 在 1 秒后重试
}
```

**关键点:**
1. **每次 startCaching 开始时都会调用 `terminateAllWatchers()`**
   - 这会断开所有现有的客户端 watch 连接
   - 包括 `kubectl -w` 建立的连接

2. **reflector.ListAndWatch() 阻塞直到连接失败**
   - 当 K8s2 网络故障时，底层的 dynamic client watch 会失败
   - reflector 检测到错误并返回

3. **错误处理:**
   - 记录错误日志
   - 函数返回，触发 `wait.Until` 在 1 秒后重试
   - 重试时会再次调用 `terminateAllWatchers()`

### 3. terminateAllWatchers 的实现

**文件:** `vendor/k8s.io/apiserver/pkg/storage/cacher/cacher.go:1310-1314`

```go
func (c *Cacher) terminateAllWatchers() {
    c.Lock()
    defer c.Unlock()
    c.watchers.terminateAll(c.groupResource, c.stopWatcherLocked)
}
```

**文件:** `vendor/k8s.io/apiserver/pkg/storage/cacher/cacher.go:165-181`

```go
func (i *indexedWatchers) terminateAll(groupResource schema.GroupResource, done func(*cacheWatcher)) {
    if len(i.allWatchers) > 0 || len(i.valueWatchers) > 0 {
        klog.Warningf("Terminating all watchers from cacher %v", groupResource)
    }
    
    // 终止所有 namespace 范围的 watchers
    for _, watchers := range i.allWatchers {
        watchers.terminateAll(done)
    }
    
    // 终止所有基于字段值的 watchers
    for _, watchers := range i.valueWatchers {
        watchers.terminateAll(done)
    }
    
    // 清空所有 watchers 映射
    i.allWatchers = map[namespacedName]watchersMap{}
    i.valueWatchers = map[string]watchersMap{}
}
```

### 4. Reflector 的 watch 实现 - 超时机制

**文件:** `vendor/k8s.io/client-go/tools/cache/reflector.go:418-450`

```go
func (r *Reflector) watch(w watch.Interface, stopCh <-chan struct{}, resyncerrc chan error) error {
    for {
        select {
        case <-stopCh:
            if w != nil {
                w.Stop()
            }
            return nil
        default:
        }

        if w == nil {
            // ⚠️ 计算随机超时时间
            timeoutSeconds := int64(r.minWatchTimeout.Seconds() * (rand.Float64() + 1.0))
            options := metav1.ListOptions{
                ResourceVersion: r.LastSyncResourceVersion(),
                TimeoutSeconds: &timeoutSeconds,  // ⚠️ watch 超时设置
                AllowWatchBookmarks: true,
            }
            
            // 创建新的 watch 连接
            w, err = r.listerWatcher.Watch(options)
            if err != nil {
                // 处理错误...
                return err
            }
        }
        
        // 从 watch 接收事件...
    }
}
```

**关键点:**
- `minWatchTimeout` 默认是 5 分钟
- 实际超时时间是随机的：`minWatchTimeout * (1.0 到 2.0)` = **5-10 分钟**
- 这就是为什么客户端在 5-10 分钟后才会重连

### 5. 底层 store.Watch() - 直接连接成员集群

**文件:** `pkg/search/proxy/store/store.go:144-172`

```go
func (s *store) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
    namespace, _ := s.splitKey(key)
    
    reqNS, objFilter, shortCircuit := filterNS(s.multiNS, namespace)
    if shortCircuit {
        return watch.NewEmptyWatch(), nil
    }
    
    // 获取到成员集群的 dynamic client
    client, err := s.client(reqNS)
    if err != nil {
        return nil, err  // ⚠️ 连接失败会返回错误
    }
    
    options := convertToMetaV1ListOptions(opts)
    
    // ⚠️ 直接调用成员集群的 Watch API
    watcher, err := client.Watch(ctx, options)
    if err != nil {
        return nil, err  // ⚠️ Watch 失败返回错误
    }
    
    if objFilter != nil {
        watcher = watch.Filter(watcher, func(in watch.Event) (watch.Event, bool) {
            return in, objFilter(in.Object)
        })
    }
    return watcher, nil
}
```

**关键点:**
1. `store.Watch()` 直接连接到成员集群的 apiserver
2. 当 K8s2 网络故障时，`client.Watch()` 会返回错误或连接超时
3. 这个错误会向上传播到 reflector，最终触发 cacher 的重启逻辑

## 故障场景时序图

```
时间线              K8s2 状态         Cacher 状态              客户端 (kubectl -w)
────────────────────────────────────────────────────────────────────────────────
T0   正常运行        Ready           ListAndWatch 正常         接收事件流
     │                               │
T1   网络故障        NotReady        │                         │
     │                               ↓
T2   网络包丢失      NotReady        Watch 连接超时/失败       │
     │                               ↓
T3   检测到错误      NotReady        ListAndWatch 返回错误     │
     │                               ↓
T4   触发重启        NotReady        startCaching() 退出       │
     │                               ↓
T5   终止连接        NotReady        terminateAllWatchers()    连接断开 ❌
     │                               ↓
T6   等待重试        NotReady        wait.Until (1秒后)        │
     │                               ↓
T7   重试失败        NotReady        ListAndWatch 再次失败     │
     │                               ↓
     ├─ 循环 ─────────────────────── 每秒重试 ─────────────────┤
     │
T8   故障恢复        Ready           │                         │
     │                               ↓
T9   连接成功        Ready           ListAndWatch 成功         │
     │                               ↓
T10  缓存同步        Ready           ready.set(true)           │
     │                               ↓
T11  等待客户端      Ready           接受新 Watch 请求         等待超时 (5-10分钟)
     │                               │                         │
T12  超时重连        Ready           │                         ← 重建 Watch
     │                               ↓
T13  新连接建立      Ready           创建新 cacheWatcher       接收事件流 ✅
```

## 为什么 kubectl -w 断开连接？

### 根本原因

**Kubernetes Cacher 的设计原则:**
- Cacher 保证缓存的一致性
- 当底层存储连接失败时，缓存状态不可靠
- **为了避免返回陈旧或不一致的数据，必须终止所有 watch 连接**

### 代码证据

1. **强制终止策略** (`cacher.go:472`):
   ```go
   c.terminateAllWatchers()  // 每次重启前都终止
   ```

2. **ready 状态管理** (`cacher.go:466-470`):
   ```go
   defer func() {
       if successfulList {
           c.ready.set(false)  // 标记为不可用
       }
   }()
   ```

3. **警告日志** (`cacher.go:171`):
   ```go
   klog.Warningf("Terminating all watchers from cacher %v", groupResource)
   ```

## 相关日志示例

当 K8s2 故障时，你会看到类似的日志：

```bash
# 1. Reflector 检测到连接失败
E1127 10:15:23.123456 reflector.go:147] pkg/mod/k8s.io/client-go@v0.xx.x/tools/cache/reflector.go:229: 
Failed to watch *v1.Pod: failed to list *v1.Pod: Get "https://k8s2-apiserver:6443/api/v1/pods?...": 
dial tcp: i/o timeout

# 2. Cacher 记录 ListAndWatch 错误
E1127 10:15:23.234567 cacher.go:478] cacher (core/v1/Pod): unexpected ListAndWatch error: 
Get "https://k8s2-apiserver:6443/api/v1/pods?...": dial tcp: i/o timeout; reinitializing...

# 3. 终止所有 watchers
W1127 10:15:23.345678 cacher.go:171] Terminating all watchers from cacher core/v1/Pod

# 4. 1秒后重试
I1127 10:15:24.456789 reflector.go:349] Listing and watching *v1.Pod from cacher (core/v1/Pod)

# 5. 如果集群仍然不可用，继续失败
E1127 10:15:24.567890 cacher.go:478] cacher (core/v1/Pod): unexpected ListAndWatch error: 
Get "https://k8s2-apiserver:6443/api/v1/pods?...": dial tcp: i/o timeout; reinitializing...

# ... 循环直到集群恢复 ...

# 6. 集群恢复后成功
I1127 10:20:45.123456 cacher.go:463] cacher (core/v1/Pod): initialized
```

## 与 Issue 的关系

这个分析解释了 GitHub issue 中观察到的行为：

1. **为什么 kubectl -w 断开:**
   - K8s2 故障导致 reflector 的 watch 连接失败
   - Cacher 检测到错误并调用 `terminateAllWatchers()`
   - 所有客户端连接被强制断开

2. **为什么需要 5-10 分钟才能看到更新:**
   - 客户端的 watch 已经断开
   - client-go 的 informer 有随机的重连超时 (5-10 分钟)
   - 只有重连后，才会创建新的 watch，包含恢复的 K8s2

3. **为什么多副本会看到不一致的数据:**
   - 每个副本的 watch 超时是独立随机的
   - 不同副本在不同时刻重连
   - 导致有的副本看到 K8s2 的数据，有的看不到

## 解决方案的启示

基于这个分析，我们可以看到几个改进方向：

### 1. 主动通知机制 (推荐)

当 `MultiClusterCache.UpdateCache()` 检测到新集群加入时：
- 不要等待客户端自然超时
- 主动关闭现有的 watch 连接
- 客户端会立即重连，获取包含新集群的视图

### 2. 独立的集群 Watch

为每个集群维护独立的 watch 连接：
- K8s1 的 watch 不受 K8s2 故障影响
- K8s2 恢复后，只需重建 K8s2 的 watch
- 现有的 watchMux 可以动态添加新的 source

### 3. Watch 状态同步

在 search proxy 层面：
- 跟踪每个 watch 连接知道哪些集群
- 当集群拓扑变化时，发送特殊事件
- 客户端根据事件决定是否重连

## 代码修改点

如果要实现改进，需要修改以下文件：

1. **pkg/search/proxy/store/multi_cluster_cache.go**
   - 跟踪活跃的 watch 连接
   - 在 `UpdateCache()` 中检测集群变化
   - 通知或终止受影响的 watch

2. **pkg/search/proxy/store/util.go**
   - 增强 `watchMux` 支持动态添加 source
   - 实现 `AddSourceDynamic()` 方法

3. **pkg/search/proxy/controller.go**
   - 在 reconcile 逻辑中检测集群拓扑变化
   - 触发 watch 连接更新

## 参考资料

- Kubernetes Cacher 设计: `vendor/k8s.io/apiserver/pkg/storage/cacher/cacher.go`
- Reflector 实现: `vendor/k8s.io/client-go/tools/cache/reflector.go`
- Watch 超时配置: `vendor/k8s.io/client-go/tools/cache/shared_informer.go`

