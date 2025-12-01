/*
Copyright 2024 The Karmada Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package store

import (
	"context"
	"sync"
	"testing"
	"time"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	fakedynamic "k8s.io/client-go/dynamic/fake"
)

// TestWatchInvalidationOnClusterAdded tests that existing watch connections
// receive invalidation events when a new cluster is added
func TestWatchInvalidationOnClusterAdded(t *testing.T) {
	// Create cache with fake clients
	cluster1Client := fakedynamic.NewSimpleDynamicClient(scheme,
		newUnstructuredObject(podGVK, "pod1", withNamespace("ns1")),
	)
	cluster2Client := fakedynamic.NewSimpleDynamicClient(scheme,
		newUnstructuredObject(podGVK, "pod2", withNamespace("ns1")),
	)
	
	var clientLock sync.Mutex
	clients := map[string]dynamic.Interface{
		"cluster1": cluster1Client,
	}
	
	newClientFunc := func(cluster string) (dynamic.Interface, error) {
		clientLock.Lock()
		defer clientLock.Unlock()
		if client, ok := clients[cluster]; ok {
			return client, nil
		}
		return fakedynamic.NewSimpleDynamicClient(scheme), nil
	}
	
	cache := NewMultiClusterCache(newClientFunc, restMapper)
	defer cache.Stop()
	
	cluster1 := newCluster("cluster1")
	resourcesByCluster := map[string]map[schema.GroupVersionResource]*MultiNamespace{
		cluster1.Name: resourceSet(podGVR),
	}
	registeredResources := map[schema.GroupVersionResource]struct{}{
		podGVR: {},
	}

	// Initial cache update with cluster1
	err := cache.UpdateCache(resourcesByCluster, registeredResources)
	if err != nil {
		t.Fatalf("Failed to update cache: %v", err)
	}

	// Wait for cache to be ready
	time.Sleep(100 * time.Millisecond)

	// Establish watch connection
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	watcher, err := cache.Watch(ctx, podGVR, &metainternalversion.ListOptions{
		ResourceVersion: "0",
	})
	if err != nil {
		t.Fatalf("Failed to create watch: %v", err)
	}
	defer watcher.Stop()

	// Verify initial events are received
	select {
	case event := <-watcher.ResultChan():
		if event.Type != watch.Added {
			t.Errorf("Expected Added event, got %v", event.Type)
		}
		t.Logf("Received initial event: %v", event.Type)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for initial watch event")
	}

	// Add cluster2 to the client map
	clientLock.Lock()
	clients["cluster2"] = cluster2Client
	clientLock.Unlock()

	// Add cluster2 to cache
	cluster2 := newCluster("cluster2")
	resourcesByCluster[cluster2.Name] = resourceSet(podGVR)

	// Update cache with new cluster
	err = cache.UpdateCache(resourcesByCluster, registeredResources)
	if err != nil {
		t.Fatalf("Failed to update cache with new cluster: %v", err)
	}

	// Verify that we receive an ERROR event indicating cache invalidation
	select {
	case event := <-watcher.ResultChan():
		if event.Type != watch.Error {
			t.Errorf("Expected Error event for cache invalidation, got %v", event.Type)
		}
		
		// Verify the status contains appropriate message
		status, ok := event.Object.(*metav1.Status)
		if !ok {
			t.Errorf("Expected Status object in error event, got %T", event.Object)
		} else {
			if status.Reason != metav1.StatusReasonExpired {
				t.Errorf("Expected reason Expired, got %v", status.Reason)
			}
			if status.Code != 410 {
				t.Errorf("Expected status code 410, got %d", status.Code)
			}
			t.Logf("Received invalidation message: %s", status.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for cache invalidation event after cluster addition")
	}
}

// TestNoInvalidationOnCacheUpdateWithoutTopologyChange tests that watch connections
// are NOT invalidated when cache is updated without topology changes
func TestNoInvalidationOnCacheUpdateWithoutTopologyChange(t *testing.T) {
	cluster1Client := fakedynamic.NewSimpleDynamicClient(scheme,
		newUnstructuredObject(podGVK, "pod1", withNamespace("ns1")),
	)
	
	newClientFunc := func(cluster string) (dynamic.Interface, error) {
		if cluster == "cluster1" {
			return cluster1Client, nil
		}
		return fakedynamic.NewSimpleDynamicClient(scheme), nil
	}
	
	cache := NewMultiClusterCache(newClientFunc, restMapper)
	defer cache.Stop()

	cluster1 := newCluster("cluster1")
	resourcesByCluster := map[string]map[schema.GroupVersionResource]*MultiNamespace{
		cluster1.Name: resourceSet(podGVR),
	}
	registeredResources := map[schema.GroupVersionResource]struct{}{
		podGVR: {},
	}

	// Initial cache update
	err := cache.UpdateCache(resourcesByCluster, registeredResources)
	if err != nil {
		t.Fatalf("Failed to update cache: %v", err)
	}

	// Wait for cache to be ready
	time.Sleep(100 * time.Millisecond)

	// Establish watch connection
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	watcher, err := cache.Watch(ctx, podGVR, &metainternalversion.ListOptions{
		ResourceVersion: "0",
	})
	if err != nil {
		t.Fatalf("Failed to create watch: %v", err)
	}
	defer watcher.Stop()

	// Consume initial events
	select {
	case <-watcher.ResultChan():
		t.Log("Received initial event")
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for initial watch event")
	}

	// Update cache again with same cluster (no topology change)
	err = cache.UpdateCache(resourcesByCluster, registeredResources)
	if err != nil {
		t.Fatalf("Failed to update cache: %v", err)
	}

	// Verify that we do NOT receive an invalidation event
	select {
	case event := <-watcher.ResultChan():
		if event.Type == watch.Error {
			t.Errorf("Unexpected error event when topology didn't change: %v", event)
		}
	case <-time.After(500 * time.Millisecond):
		// Expected: no event within timeout means no invalidation
		t.Log("Correctly did not receive invalidation event when topology unchanged")
	}
}

// TestWatchDisconnectionOnClusterRemoved tests that watch connections are automatically
// disconnected when a cluster is removed (via cacher.Stop() mechanism, not our invalidation)
func TestWatchDisconnectionOnClusterRemoved(t *testing.T) {
	t.Skip("Cluster removal is handled by cacher.Stop() which terminates watches automatically. " +
		"We don't need to test our invalidation mechanism for removal, only for addition.")
	
	// Note: When a cluster is removed from cache:
	// 1. c.cache[clusterName].stop() is called
	// 2. This triggers cacher.Stop()
	// 3. cacher.terminateAllWatchers() disconnects all watches
	// 4. Clients receive disconnection signal automatically
	//
	// Our invalidateAllWatches() is only needed for cluster ADDITION,
	// not removal, because removal already has a built-in mechanism.
}

// TestWatchInvalidationOnClusterRecovery tests that watch connections are invalidated
// when a cluster recovers from NotReady state (the real-world chaos engineering scenario)
func TestWatchInvalidationOnClusterRecovery(t *testing.T) {
	cluster1Client := fakedynamic.NewSimpleDynamicClient(scheme,
		newUnstructuredObject(podGVK, "pod1", withNamespace("ns1")),
	)
	cluster2Client := fakedynamic.NewSimpleDynamicClient(scheme,
		newUnstructuredObject(podGVK, "pod2", withNamespace("ns1")),
	)
	
	var clientLock sync.Mutex
	clients := map[string]dynamic.Interface{
		"cluster1": cluster1Client,
		"cluster2": cluster2Client,
	}
	
	newClientFunc := func(cluster string) (dynamic.Interface, error) {
		clientLock.Lock()
		defer clientLock.Unlock()
		if client, ok := clients[cluster]; ok {
			return client, nil
		}
		return fakedynamic.NewSimpleDynamicClient(scheme), nil
	}
	
	cache := NewMultiClusterCache(newClientFunc, restMapper)
	defer cache.Stop()

	cluster1 := newCluster("cluster1")
	cluster2 := newCluster("cluster2")
	
	// Initial state: both clusters are healthy
	resourcesByCluster := map[string]map[schema.GroupVersionResource]*MultiNamespace{
		cluster1.Name: resourceSet(podGVR),
		cluster2.Name: resourceSet(podGVR),
	}
	registeredResources := map[schema.GroupVersionResource]struct{}{
		podGVR: {},
	}

	err := cache.UpdateCache(resourcesByCluster, registeredResources)
	if err != nil {
		t.Fatalf("Failed to update cache: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Establish watch connection
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	watcher, err := cache.Watch(ctx, podGVR, &metainternalversion.ListOptions{
		ResourceVersion: "0",
	})
	if err != nil {
		t.Fatalf("Failed to create watch: %v", err)
	}
	defer watcher.Stop()

	// Consume initial events from both clusters
	timeout := time.After(2 * time.Second)
	eventCount := 0
	for eventCount < 2 {
		select {
		case event := <-watcher.ResultChan():
			if event.Type == watch.Added {
				eventCount++
				t.Logf("Received initial event %d", eventCount)
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for initial events, got %d", eventCount)
		}
	}

	// Simulate cluster2 becoming NotReady: remove it from cache
	delete(resourcesByCluster, cluster2.Name)
	err = cache.UpdateCache(resourcesByCluster, registeredResources)
	if err != nil {
		t.Fatalf("Failed to update cache when cluster2 becomes unhealthy: %v", err)
	}

	// Client's watch should receive invalidation event
	select {
	case event := <-watcher.ResultChan():
		if event.Type != watch.Error {
			t.Errorf("Expected Error event when cluster removed, got %v", event.Type)
		}
		t.Log("Received invalidation event when cluster2 became unhealthy")
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for invalidation event when cluster2 became unhealthy")
	}

	// Client would normally reconnect here, establishing a new watch
	watcher2, err := cache.Watch(ctx, podGVR, &metainternalversion.ListOptions{
		ResourceVersion: "0",
	})
	if err != nil {
		t.Fatalf("Failed to create new watch after cluster failure: %v", err)
	}
	defer watcher2.Stop()

	// Consume initial event (only from cluster1 now)
	select {
	case event := <-watcher2.ResultChan():
		if event.Type != watch.Added {
			t.Errorf("Expected Added event, got %v", event.Type)
		}
		t.Log("New watch established, receiving events from remaining cluster1")
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for event on new watch")
	}

	// Simulate cluster2 recovery: add it back to cache
	resourcesByCluster[cluster2.Name] = resourceSet(podGVR)
	err = cache.UpdateCache(resourcesByCluster, registeredResources)
	if err != nil {
		t.Fatalf("Failed to update cache when cluster2 recovers: %v", err)
	}

	// Client's watch should receive invalidation event again
	select {
	case event := <-watcher2.ResultChan():
		if event.Type != watch.Error {
			t.Errorf("Expected Error event when cluster recovered, got %v", event.Type)
		}
		
		status, ok := event.Object.(*metav1.Status)
		if !ok {
			t.Errorf("Expected Status object, got %T", event.Object)
		} else {
			if status.Code != 410 {
				t.Errorf("Expected status code 410, got %d", status.Code)
			}
			t.Logf("✅ Received invalidation event when cluster2 recovered: %s", status.Message)
			t.Log("✅ Client will immediately reconnect and see cluster2's resources!")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("❌ Timeout waiting for invalidation event when cluster2 recovered - this means the fix didn't work!")
	}
}

// TestMultipleWatchesInvalidated tests that multiple active watches are all invalidated
func TestMultipleWatchesInvalidated(t *testing.T) {
	cluster1Client := fakedynamic.NewSimpleDynamicClient(scheme,
		newUnstructuredObject(podGVK, "pod1", withNamespace("ns1")),
	)
	cluster2Client := fakedynamic.NewSimpleDynamicClient(scheme,
		newUnstructuredObject(podGVK, "pod2", withNamespace("ns1")),
	)
	
	var clientLock sync.Mutex
	clients := map[string]dynamic.Interface{
		"cluster1": cluster1Client,
	}
	
	newClientFunc := func(cluster string) (dynamic.Interface, error) {
		clientLock.Lock()
		defer clientLock.Unlock()
		if client, ok := clients[cluster]; ok {
			return client, nil
		}
		return fakedynamic.NewSimpleDynamicClient(scheme), nil
	}
	
	cache := NewMultiClusterCache(newClientFunc, restMapper)
	defer cache.Stop()

	cluster1 := newCluster("cluster1")
	resourcesByCluster := map[string]map[schema.GroupVersionResource]*MultiNamespace{
		cluster1.Name: resourceSet(podGVR),
	}
	registeredResources := map[schema.GroupVersionResource]struct{}{
		podGVR: {},
	}

	// Initial cache update
	err := cache.UpdateCache(resourcesByCluster, registeredResources)
	if err != nil {
		t.Fatalf("Failed to update cache: %v", err)
	}

	// Wait for cache to be ready
	time.Sleep(100 * time.Millisecond)

	// Establish multiple watch connections
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	numWatches := 3
	watchers := make([]watch.Interface, numWatches)
	for i := 0; i < numWatches; i++ {
		w, err := cache.Watch(ctx, podGVR, &metainternalversion.ListOptions{
			ResourceVersion: "0",
		})
		if err != nil {
			t.Fatalf("Failed to create watch %d: %v", i, err)
		}
		defer w.Stop()
		watchers[i] = w

		// Consume initial event
		select {
		case <-w.ResultChan():
			t.Logf("Watch %d: received initial event", i)
		case <-time.After(2 * time.Second):
			t.Fatalf("Timeout waiting for initial event on watch %d", i)
		}
	}

	// Add cluster2 to the client map
	clientLock.Lock()
	clients["cluster2"] = cluster2Client
	clientLock.Unlock()

	// Add new cluster
	cluster2 := newCluster("cluster2")
	resourcesByCluster[cluster2.Name] = resourceSet(podGVR)

	err = cache.UpdateCache(resourcesByCluster, registeredResources)
	if err != nil {
		t.Fatalf("Failed to update cache with new cluster: %v", err)
	}

	// Verify that all watches receive invalidation events
	for i, watcher := range watchers {
		select {
		case event := <-watcher.ResultChan():
			if event.Type != watch.Error {
				t.Errorf("Watch %d: expected Error event, got %v", i, event.Type)
			} else {
				t.Logf("Watch %d: correctly received invalidation event", i)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("Watch %d: timeout waiting for invalidation event", i)
		}
	}
}
