# Search component cannot immediately reflect resources from recovered clusters in existing watch connections

## Background

We encountered this issue during chaos engineering tests with our deployment platform that uses Karmada's search component to aggregate Pod views from multiple Kubernetes clusters.

## Environment

- Two Kubernetes clusters: K8s1 (healthy) and K8s2 (subject to fault injection)
- Internal deployment platform uses informer to watch aggregated Pod resources through search component

## Steps to Reproduce

1. **Initial State**: Both K8s1 and K8s2 are healthy and registered with Karmada. The deployment platform has an active watch connection to search component showing aggregated Pods from both clusters.

2. **Fault Injection**: Inject network fault into K8s2 (drop all network packets), causing K8s2 to become `NotReady`.

3. **Search Component Behavior**: Search component stops the informer for K8s2 cluster (as designed in `controller.go:262-286`):
   ```go
   func (c *Controller) clusterAbleToCache(cluster string) (cls *clusterv1alpha1.Cluster, able bool, err error) {
       if !util.IsClusterReady(&cls.Status) {
           klog.Warningf("cluster %s is notReady try to stop this cluster informer", cluster)
           c.InformerManager.Stop(cluster)
           return
       }
   }
   ```

4. **Fault Recovery**: Remove network fault from K8s2. K8s2 becomes `Ready` again.

5. **Search Component Recovery**: Search component restarts the informer for K8s2 and begins caching resources.

6. **Resource Changes**: Create new Pods or modify existing Pods in K8s2.

7. **Issue Observed**: The deployment platform's existing watch connection **cannot see the new resources from K8s2**. The Pod list remains in the pre-fault state.

8. **Delayed Resolution**: After 5-10 minutes, the deployment platform finally sees the updated Pod list from K8s2.

## Root Cause Analysis

### Current Watch Implementation

The search component's proxy uses a static `watchMux` in `MultiClusterCache.Watch()`:

```go
// pkg/search/proxy/store/multi_cluster_cache.go:330-371
func (c *MultiClusterCache) Watch(ctx context.Context, gvr schema.GroupVersionResource, options *metainternalversion.ListOptions) (watch.Interface, error) {
    mux := newWatchMux()
    clusters := c.getClusterNames()  // ⚠️ Snapshot of clusters at watch creation time
    for i := range clusters {
        cluster := clusters[i]
        cache := c.cacheForClusterResource(cluster, gvr)
        if cache == nil {
            continue
        }
        w, err := cache.Watch(ctx, options)
        mux.AddSource(w, func(e watch.Event) {
            setObjectResourceVersionFunc(cluster, e.Object)
            addCacheSourceAnnotation(e.Object, cluster)
        })
    }
    mux.Start()  // ⚠️ Sources are fixed after Start()
    return mux, nil
}
```

**The Problem:**
1. When a watch connection is established, `watchMux` only includes clusters that exist at that moment
2. When K8s2 recovers and is added back to the cache via `UpdateCache()`, existing watch connections are **not notified**
3. The `watchMux.sources` array is immutable after `Start()` is called
4. New resources from K8s2 are only visible after the client's informer reconnects

### Why 5-10 Minutes Delay?

The delay is due to client-go's watch timeout mechanism:
- `MinWatchTimeout` is hardcoded to 5 minutes in client-go
- Actual timeout is randomized between `minWatchTimeout` and `2 * minWatchTimeout` (5-10 minutes)
- Only after watch timeout triggers reconnection, the new `Watch()` call includes K8s2

Reference: [client-go watch timeout](https://github.com/kubernetes/client-go/blob/master/tools/watch/retrywatcher.go)

## Expected Behavior

When a previously unavailable cluster (K8s2) recovers and its resources are cached:
1. Existing watch connections should be notified about the cluster addition
2. Resources from the recovered cluster should be sent as `ADDED` events
3. Deployment platform should see K8s2 resources immediately, not after 5-10 minutes

## Current Workaround

None available except waiting for watch reconnection.

## Impact

- **Service Visibility**: During the 5-10 minute window, the deployment platform has incomplete resource views
- **Operational Risk**: Operators may make incorrect decisions based on outdated information
- **Poor User Experience**: After cluster recovery, users expect immediate visibility into all resources
- **Inconsistent Views Across Replicas**: When the deployment platform has multiple replicas, each replica's watch connection times out at different random times (due to the randomized 5-10 minute timeout). This causes different replicas to see the updated Pod list at different moments, resulting in inconsistent responses. Users may observe Pod lists changing back and forth between requests as their requests are load-balanced across different replicas - some showing the old state (pre-recovery) and others showing the new state (post-recovery). This inconsistency severely impacts user experience and makes it difficult to trust the platform's data.

## Proposed Solutions

### Option 1: Dynamic Watch Sources (Recommended)

Modify `watchMux` to support dynamic source addition:

```go
// Add method to watchMux
func (w *watchMux) AddSourceDynamic(watcher watch.Interface, decorator func(watch.Event)) {
    w.lock.Lock()
    defer w.lock.Unlock()
    
    source := decoratedWatcher{
        watcher:   watcher,
        decorator: decorator,
    }
    w.sources = append(w.sources, source)
    
    // Start watching the new source
    wg := sync.WaitGroup{}
    wg.Add(1)
    go func() {
        defer wg.Done()
        w.startWatchSource(source.watcher, source.decorator)
    }()
}
```

When `MultiClusterCache.UpdateCache()` adds a new cluster:
1. Notify all active watch connections
2. Add new cluster's watcher to existing `watchMux` instances
3. Send existing resources from new cluster as `ADDED` events

**Implementation Steps:**
1. Store active watch connections in `MultiClusterCache`
2. When `UpdateCache()` detects new cluster:
   - Create watcher for the new cluster
   - Call `AddSourceDynamic()` on all active watch connections
3. Handle resource version tracking for the new cluster

**Challenges:**
- Resource version management becomes complex
- Need to track which resources client has already seen
- Potential race conditions between cache updates and watch events

### Option 2: Proactive Watch Reconnection

When a new cluster is added to cache:
1. Send a synthetic `ERROR` or `BOOKMARK` event to existing watch connections
2. Force clients to reconnect, picking up the new cluster
3. Client-go's informer will automatically handle reconnection

**Pros:**
- Simpler implementation
- Leverages existing reconnection logic
- No resource version tracking issues

**Cons:**
- Still requires client reconnection (but immediate, not 5-10 minutes)
- May cause brief disruption to all watch connections

### Option 3: Periodic Full Reconciliation

Add a configuration option for periodic full list operations to detect missing resources:

```go
type SearchConfig struct {
    // ReconcileInterval defines how often to perform full list to detect missing resources
    // Default: 0 (disabled)
    ReconcileInterval time.Duration
}
```

**Pros:**
- Simple to implement
- No changes to watch mechanism

**Cons:**
- Increased API server load
- Delayed discovery (based on interval)
- Not a real-time solution

### Option 4: Watch Cache Invalidation Event

Introduce a new event type `CACHE_INVALIDATED` to signal clients that cache state has changed:

```go
// Send to all active watches when cluster topology changes
event := watch.Event{
    Type: watch.Modified, // or a custom type
    Object: &metav1.Status{
        Status:  "CacheInvalidated",
        Message: "Cluster topology changed, recommend reconnection",
        Reason:  "ClusterRecovered",
        Code:    299, // Miscellaneous warning
    },
}
```

## Questions for Maintainers

1. Is this behavior considered a bug or limitation by design?
2. Would the community accept a PR implementing dynamic watch sources (Option 1)?
3. Are there concerns about resource version consistency when adding dynamic sources?
4. Should we add a configuration option to control this behavior?
5. Which proposed solution aligns best with Karmada's architecture and goals?

## Additional Context

This issue is particularly critical for:
- Chaos engineering and fault recovery scenarios
- Multi-cluster deployment platforms
- Real-time monitoring and alerting systems
- Any application requiring immediate visibility into cluster state changes
- High-availability setups where cluster failures and recoveries are expected

### Similar Issues in Kubernetes Ecosystem

This is a common challenge in multi-cluster aggregation systems. Similar issues have been discussed in:
- Federation v2 / KubeFed
- Virtual Kubelet scenarios
- Multi-cluster service mesh implementations

## Related Code References

- `pkg/search/controller.go:262-286` - Cluster readiness check and informer stop logic
- `pkg/search/controller.go:361-429` - doCacheCluster logic and informer creation
- `pkg/search/proxy/store/multi_cluster_cache.go:97-133` - UpdateCache implementation
- `pkg/search/proxy/store/multi_cluster_cache.go:330-371` - Watch implementation
- `pkg/search/proxy/store/util.go:190-279` - watchMux implementation and event multiplexing
- `pkg/search/proxy/controller.go:189-236` - Proxy controller reconciliation

## Test Scenario

To help validate any fix, here's a test scenario:

```go
// Pseudo-test code
func TestClusterRecoveryWatchVisibility(t *testing.T) {
    // 1. Setup: Two clusters with resources
    cluster1 := setupCluster("cluster1", pods...)
    cluster2 := setupCluster("cluster2", pods...)
    
    // 2. Establish watch connection
    watcher := searchClient.Watch(ctx, "pods", watchOptions)
    
    // 3. Mark cluster2 as NotReady
    markClusterNotReady("cluster2")
    waitForInformerStop("cluster2")
    
    // 4. Mark cluster2 as Ready
    markClusterReady("cluster2")
    waitForInformerStart("cluster2")
    
    // 5. Create new pod in cluster2
    pod := createPod("cluster2", "new-pod")
    
    // 6. Verify: Watch should receive ADDED event for new pod
    // WITHOUT waiting for watch timeout
    event := waitForEvent(watcher, timeout: 30*time.Second)
    assert.Equal(t, watch.Added, event.Type)
    assert.Equal(t, "new-pod", event.Object.Name)
    assert.Equal(t, "cluster2", event.Object.Annotations[cacheSourceAnnotation])
}
```

---

**Environment Details:**
- Karmada Version: [Please specify - check with `karmadactl version`]
- Kubernetes Version: [Please specify - check with `kubectl version`]
- Client-go Version: [Please specify - check go.mod]
- Deployment Mode: [e.g., host cluster, standalone]

**Logs and Diagnostics:**

When the issue occurs, relevant log messages include:

```
# When cluster becomes NotReady
I1127 10:15:23.123456 controller.go:280] cluster cluster2 is notReady try to stop this cluster informer

# When cluster recovers
I1127 10:20:45.123456 controller.go:395] Try to build informer manager for cluster cluster2
I1127 10:20:45.234567 controller.go:421] Add informer for cluster2, core/v1, Resource=pods
I1127 10:20:45.345678 controller.go:424] Start informer for cluster2
I1127 10:20:46.456789 controller.go:427] Start informer for cluster2 done

# In proxy cache
I1127 10:20:46.567890 multi_cluster_cache.go:122] Add cache for cluster cluster2
I1127 10:20:46.678901 cluster_cache.go:87] Add cache for cluster2 core/v1, Resource=pods
```

However, existing watch connections show no events for new resources in cluster2 until timeout.

---

**Discussion Points:**

1. **Backward Compatibility**: Any solution should maintain compatibility with existing clients
2. **Performance Impact**: Adding dynamic sources should not significantly impact watch performance
3. **Resource Version Semantics**: Need to clearly define resource version behavior when clusters are added/removed dynamically
4. **API Contracts**: Should we document this behavior as a known limitation, or commit to fixing it?

## Volunteers

I'm willing to work on a PR for this issue if the community agrees on the approach. Please provide feedback on:
- Preferred solution (Option 1, 2, 3, 4, or alternative)
- Implementation guidance
- Testing requirements
- Documentation updates needed

