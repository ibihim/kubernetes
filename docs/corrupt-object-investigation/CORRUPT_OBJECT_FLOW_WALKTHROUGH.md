# Complete Code Walkthrough: Corrupt Object Deletion → Client Connection Close

This document traces the complete code path from when a corrupt object is deleted in etcd
until the client's HTTP connection is closed. Each step includes file paths, line numbers,
and relevant code snippets.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                                   kube-apiserver                                      │
│                                                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐ │
│  │ LAYER 1: etcd3 Storage Layer                                                     │ │
│  │                                                                                   │ │
│  │  etcd ──▶ watchChan.transform() ──▶ prepareObjs() fails ──▶ sendError()         │ │
│  │                                            │                       │              │ │
│  │                                            ▼                       ▼              │ │
│  │                              corruptObjectDeletedError    watch.Error event       │ │
│  │                                                                    │              │ │
│  └────────────────────────────────────────────────────────────────────┼──────────────┘ │
│                                                                       │               │
│  ┌────────────────────────────────────────────────────────────────────┼──────────────┐ │
│  │ LAYER 2: Reflector (client-go, used by Cacher)                     │              │ │
│  │                                                                    ▼              │ │
│  │  handleAnyWatch() ◀── receives watch.Error ──▶ returns error                     │ │
│  │         │                                                                         │ │
│  │         ▼                                                                         │ │
│  │  watch() ──▶ error handling ──▶ returns nil (logs warning)                       │ │
│  │         │                                                                         │ │
│  │         ▼                                                                         │ │
│  │  ListAndWatch() returns nil                                                       │ │
│  │                                                                                   │ │
│  └───────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                       │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│  │ LAYER 3: Cacher  ⚠️  BYPASSES RunWithContext - calls ListAndWatch DIRECTLY!       │ │
│  │                                                                                   │ │
│  │  startCaching() returns ──▶ wait.Until waits 1 second (FIXED, no backoff!)       │ │
│  │         │                                                                         │ │
│  │         ▼                                                                         │ │
│  │  startCaching() called again                                                      │ │
│  │         │                                                                         │ │
│  │         ▼                                                                         │ │
│  │  terminateAllWatchers() ──▶ stopLocked() ──▶ close(input)                        │ │
│  │                                                     │                             │ │
│  └─────────────────────────────────────────────────────┼─────────────────────────────┘ │
│                                                        │                              │
│  ┌─────────────────────────────────────────────────────┼─────────────────────────────┐ │
│  │ LAYER 4: cacheWatcher (per client)                  │                             │ │
│  │                                                     ▼                             │ │
│  │  process() ◀── sees input closed ──▶ returns                                     │ │
│  │         │                                                                         │ │
│  │         ▼                                                                         │ │
│  │  processInterval() ──▶ defer close(c.result)                                     │ │
│  │                                        │                                          │ │
│  └────────────────────────────────────────┼──────────────────────────────────────────┘ │
│                                           │                                           │
│  ┌────────────────────────────────────────┼──────────────────────────────────────────┐ │
│  │ LAYER 5: HTTP Handler                  │                                          │ │
│  │                                        ▼                                          │ │
│  │  WatchServer.HandleHTTP() ◀── sees result closed ──▶ returns                     │ │
│  │         │                                                                         │ │
│  │         ▼                                                                         │ │
│  │  HTTP Response ends ──▶ Connection closes                                        │ │
│  │                                                                                   │ │
│  └───────────────────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────────────────┘
                                           │
                                           ▼
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                                      CLIENT                                           │
│                                                                                       │
│  1. Client's Reflector sees channel close (NOT watch.Error)                          │
│         │                                                                             │
│         ▼                                                                             │
│  2. Client tries to RESUME from lastSyncResourceVersion (NOT fresh list!)            │
│         │                                                                             │
│         ▼                                                                             │
│  3. Server returns "too old resource version" error                                  │
│         │                                                                             │
│         ▼                                                                             │
│  4. Client's watch() returns nil ──▶ ListAndWatchWithContext() returns               │
│         │                                                                             │
│         ▼                                                                             │
│  5. RunWithContext() retries ──▶ Fresh LIST ──▶ CLIENT CACHE REBUILT                 │
│                                                                                       │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

---

## ⚠️ Critical Architectural Detail: Cacher Bypasses RunWithContext

Understanding **how** the Cacher uses the Reflector is essential to understanding why all
client watchers are terminated when a corrupt object error occurs.

### Normal Reflector Usage (e.g., client-side informers)

Typically, a Reflector is started using `RunWithContext()`:

**File:** `staging/src/k8s.io/client-go/tools/cache/reflector.go:354-363`

```go
func (r *Reflector) RunWithContext(ctx context.Context) {
    logger := klog.FromContext(ctx)
    logger.V(3).Info("Starting reflector", ...)
    wait.BackoffUntil(func() {
        if err := r.ListAndWatchWithContext(ctx); err != nil {
            r.watchErrorHandler(ctx, r, err)  // ← Custom error handler called!
        }
    }, r.backoffManager, true, ctx.Done())    // ← Exponential backoff (800ms → 30s)
    logger.V(3).Info("Stopping reflector", ...)
}
```

Key characteristics:
- Uses **exponential backoff** via `wait.BackoffUntil` (800ms initial, up to 30s max)
- Invokes **`watchErrorHandler`** when errors occur (allows custom error handling)
- Designed for resilient, long-running client watches

### Cacher's Direct ListAndWatch Usage (DIFFERENT!)

The Cacher **does NOT use `RunWithContext()`**. Instead, it directly calls `ListAndWatch()`
and manages its own retry loop:

**File:** `staging/src/k8s.io/apiserver/pkg/storage/cacher/cacher.go:469-500`

```go
// The Cacher's goroutine setup:
go func() {
    defer cacher.stopWg.Done()
    defer cacher.terminateAllWatchers()
    wait.Until(                                    // ← Simple retry, NOT BackoffUntil!
        func() {
            if !cacher.isStopped() {
                cacher.startCaching(stopCh)
            }
        }, time.Second, stopCh,                    // ← FIXED 1-second delay, NO backoff!
    )
}()

func (c *Cacher) startCaching(stopChannel <-chan struct{}) {
    // ...
    c.terminateAllWatchers()                       // ← ALL clients terminated FIRST!
    err = c.reflector.ListAndWatch(stopChannel)    // ← Direct call, NOT RunWithContext!
    if err != nil {
        klog.Errorf("cacher (%v): unexpected ListAndWatch error: %v; reinitializing...",
            c.groupResource.String(), err)         // ← Just logs, no watchErrorHandler!
    }
}
```

### Comparison Table

| Aspect | `RunWithContext` (normal clients) | Cacher's Direct `ListAndWatch` |
|--------|-----------------------------------|--------------------------------|
| **Retry mechanism** | `wait.BackoffUntil` | `wait.Until` |
| **Backoff strategy** | Exponential (800ms → 30s) | **Fixed 1 second** |
| **Error handler** | `watchErrorHandler` called | **Not called** (just logs) |
| **On restart** | Continues watching | **`terminateAllWatchers()` first!** |

### Why This Matters

Because the Cacher bypasses `RunWithContext()`:

1. **No exponential backoff** — Retries happen every 1 second regardless of error type
2. **No custom error handling** — The `watchErrorHandler` is never invoked
3. **`terminateAllWatchers()` on EVERY restart** — All connected clients are disconnected
   before re-listing, even for transient errors

This design choice means that ANY error that causes `ListAndWatch()` to return will
result in ALL client watchers being terminated ~1 second later.

---

## Step-by-Step Code Walkthrough

---

### STEP 1: etcd3 Watcher Detects Corrupt Object Deletion

When a DELETE event arrives from etcd for a corrupt object, the watcher tries to
decode/transform the object data but fails.

**File:** `staging/src/k8s.io/apiserver/pkg/storage/etcd3/watcher.go:570-576`

```go
// transform transforms an event into a result for user if not filtered.
func (wc *watchChan) transform(e *event) (res *watch.Event, err error) {
    curObj, oldObj, err := wc.prepareObjs(e)  // ← Fails for corrupt object
    if err != nil {
        klog.Errorf("failed to prepare current and previous objects: %v", err)
        return nil, err  // ← Error returned here
    }
    // ...
}
```

**File:** `staging/src/k8s.io/apiserver/pkg/storage/etcd3/watcher.go:720-731`

```go
func (wc *watchChan) prepareObjs(e *event) (curObj runtime.Object, oldObj runtime.Object, err error) {
    // ...
    if len(e.prevValue) > 0 && (e.isDeleted || !wc.acceptAll()) {
        data, _, err := wc.watcher.transformer.TransformFromStorage(wc.ctx, e.prevValue, authenticatedDataString(e.key))
        if err != nil {
            return nil, nil, wc.watcher.transformIfCorruptObjectError(e, err)  // ← Wraps error
        }
        oldObj, err = decodeObj(wc.watcher.codec, wc.watcher.versioner, data, e.rev)
        if err != nil {
            return nil, nil, wc.watcher.transformIfCorruptObjectError(e, err)  // ← Wraps error
        }
    }
    return curObj, oldObj, nil
}
```

**File:** `staging/src/k8s.io/apiserver/pkg/storage/etcd3/watcher.go:745-756`

```go
func (w *watcher) transformIfCorruptObjectError(e *event, err error) error {
    var corruptObjErr *corruptObjectError
    if !e.isDeleted || !errors.As(err, &corruptObjErr) {
        return err
    }

    // if we are here it means we received a DELETED event but the object
    // associated with it is corrupt because we failed to transform or
    // decode the data associated with the object.
    // wrap the original error so we can send a proper watch Error event.
    return &corruptObjectDeletedError{err: corruptObjErr}  // ← Special error type
}
```

---

### STEP 2: Error is Processed and Sent as watch.Error Event

The concurrent event processing goroutine receives the error and calls `sendError()`.

**File:** `staging/src/k8s.io/apiserver/pkg/storage/etcd3/watcher.go:531-556`

```go
func (p *concurrentOrderedEventProcessing) collectEventProcessing(ctx context.Context) {
    var processingResponse chan *processingResult
    var r *processingResult
    for {
        select {
        case <-ctx.Done():
            return
        case processingResponse = <-p.processingQueue:
        }
        select {
        case <-ctx.Done():
            return
        case r = <-processingResponse:
        }
        if r.err != nil {
            p.wc.sendError(r.err)  // ← Error sent here!
            return
        }
        // ...
    }
}
```

**File:** `staging/src/k8s.io/apiserver/pkg/storage/etcd3/watcher.go:653-671`

```go
// sendError synchronously puts an error event into resultChan and
// trigger cancelling all goroutines.
func (wc *watchChan) sendError(err error) {
    // We use wc.ctx to reap all goroutines. Under whatever condition, we should stop them all.
    // It's fine to double cancel.
    defer wc.cancel()

    if isCancelError(err) {
        return
    }
    errResult := transformErrorToEvent(err)  // ← Converts to watch.Error event
    if errResult != nil {
        // error result is guaranteed to be received by user before closing ResultChan.
        select {
        case wc.resultChan <- *errResult:  // ← Sent to result channel!
        case <-wc.ctx.Done():
        }
    }
}
```

**File:** `staging/src/k8s.io/apiserver/pkg/storage/etcd3/watcher.go:641-651`

```go
func transformErrorToEvent(err error) *watch.Event {
    err = interpretWatchError(err)  // ← Converts to StatusError
    if _, ok := err.(apierrors.APIStatus); !ok {
        err = apierrors.NewInternalError(err)
    }
    status := err.(apierrors.APIStatus).Status()
    return &watch.Event{
        Type:   watch.Error,      // ← watch.Error type!
        Object: &status,
    }
}
```

**File:** `staging/src/k8s.io/apiserver/pkg/storage/etcd3/errors.go:31-47`

```go
func interpretWatchError(err error) error {
    switch {
    case err == etcdrpc.ErrCompacted:
        return errors.NewResourceExpired("The resourceVersion for the provided watch is too old.")
    }

    var corruptobjDeletedErr *corruptObjectDeletedError
    if goerrors.As(err, &corruptobjDeletedErr) {
        return &errors.StatusError{
            ErrStatus: metav1.Status{
                Status:  metav1.StatusFailure,
                Code:    http.StatusInternalServerError,           // ← 500
                Reason:  metav1.StatusReasonStoreReadError,        // ← Special reason
                Message: corruptobjDeletedErr.Error(),
            },
        }
    }

    return err
}
```

> **Key Point:** The error has `StatusReasonStoreReadError`, NOT `StatusReasonInternalError`.
> This is important for Step 4.

---

### STEP 3: Backend Reflector Receives watch.Error Event

The Reflector (from client-go) is watching the etcd3 watcher's result channel.
When it receives a `watch.Error` event, it returns an error.

**File:** `staging/src/k8s.io/client-go/tools/cache/reflector.go:860-898`

```go
func handleAnyWatch(
    ctx context.Context,
    start time.Time,
    w watch.Interface,
    store ReflectorStore,
    // ... other params
) (bool, error) {
    watchListBookmarkReceived := false
    // ...
    stopWatcher := true
    defer func() {
        if stopWatcher {
            w.Stop()
        }
    }()

loop:
    for {
        select {
        case <-ctx.Done():
            return watchListBookmarkReceived, errorStopRequested
        case err := <-errCh:
            return watchListBookmarkReceived, err
        case event, ok := <-w.ResultChan():  // ← Reads from etcd3 watcher
            if !ok {
                break loop
            }
            if event.Type == watch.Error {
                return watchListBookmarkReceived, apierrors.FromObject(event.Object)  // ← Returns error!
            }
            // ... handle other event types (Added, Modified, Deleted, Bookmark)
        }
    }
    // ...
}
```

> **Key Point:** The Reflector returns the error immediately when it sees `watch.Error`.
> It does NOT call `store.Add/Update/Delete` for error events.

---

### STEP 4: Reflector's watch() Handles Error and Returns nil

The `watch()` function receives the error from `handleAnyWatch` (via `handleWatch`)
and processes it through error handling logic.

**File:** `staging/src/k8s.io/client-go/tools/cache/reflector.go:538-570`

```go
func (r *Reflector) watch(ctx context.Context, w watch.Interface, resyncerrc chan error) error {
    // ...
    for {
        // ... watch setup ...

        err = handleWatch(ctx, start, w, r.store, r.expectedType, r.expectedGVK, r.name,
            r.typeDescription, r.setLastSyncResourceVersion, r.clock, resyncerrc)
        // handleWatch always stops the watcher. So we don't need to here.
        // Just set it to nil to trigger a retry on the next loop.
        w = nil
        retry.After(err)
        if err != nil {
            if !errors.Is(err, errorStopRequested) {
                switch {
                case isExpiredError(err):
                    // ... handle expired ...
                    logger.V(4).Info("Watch closed", ...)
                case apierrors.IsTooManyRequests(err):
                    // ... handle 429 with backoff ...
                    logger.V(2).Info("Watch returned 429 - backing off", ...)
                    continue
                case apierrors.IsInternalError(err) && retry.ShouldRetry():
                    // ← NOT taken! StatusReasonStoreReadError is a known reason
                    logger.V(2).Info("Retrying watch after internal error", ...)
                    continue
                default:
                    // ← THIS branch is taken!
                    logger.Info("Warning: watch ended with error", "reflector", r.name,
                        "type", r.typeDescription, "err", err)
                }
            }
            return nil  // ← Returns nil, NOT the error!
        }
    }
}
```

> **Key Point:** `apierrors.IsInternalError(err)` returns `false` for `StatusReasonStoreReadError`
> because it's in the `knownReasons` map. So the error falls through to `default` case,
> gets logged as a warning, and the function returns `nil`.

**File:** `staging/src/k8s.io/apimachinery/pkg/api/errors/errors.go:711-720`

```go
func IsInternalError(err error) bool {
    reason, code := reasonAndCodeForError(err)
    if reason == metav1.StatusReasonInternalError {
        return true
    }
    // StatusReasonStoreReadError IS in knownReasons (line 57)
    // So ok == true, and this returns false
    if _, ok := knownReasons[reason]; !ok && code == http.StatusInternalServerError {
        return true
    }
    return false  // ← Returns false for StatusReasonStoreReadError
}
```

---

### STEP 5: ListAndWatch() Returns → startCaching() Completes

Since `watch()` returns `nil`, `watchWithResync()` returns `nil`,
`ListAndWatchWithContext()` returns `nil`, and finally `ListAndWatch()` returns `nil`.

> **⚠️ Important:** Remember that the Cacher calls `ListAndWatch()` directly, NOT via
> `RunWithContext()`. This means no `watchErrorHandler` is invoked, and no exponential
> backoff is applied. See the [Critical Architectural Detail](#️-critical-architectural-detail-cacher-bypasses-runwithcontext) section above.

**File:** `staging/src/k8s.io/client-go/tools/cache/reflector.go:391-434`

```go
func (r *Reflector) ListAndWatch(stopCh <-chan struct{}) error {
    return r.ListAndWatchWithContext(wait.ContextForChannel(stopCh))
}

func (r *Reflector) ListAndWatchWithContext(ctx context.Context) error {
    // ... list logic ...

    logger.V(2).Info("Caches populated", "type", r.typeDescription, "reflector", r.name)
    return r.watchWithResync(ctx, w)  // ← Returns nil
}
```

**File:** `staging/src/k8s.io/apiserver/pkg/storage/cacher/cacher.go:484-500`

```go
func (c *Cacher) startCaching(stopChannel <-chan struct{}) {
    c.watchCache.SetOnReplace(func() {
        c.ready.setReady()
        klog.V(1).InfoS("cacher initialized", "group", c.groupResource.Group,
            "resource", c.groupResource.Resource)
        metrics.WatchCacheInitializations.WithLabelValues(c.groupResource.Group,
            c.groupResource.Resource).Inc()
    })
    var err error
    defer func() {
        c.ready.setError(err)
    }()

    c.terminateAllWatchers()
    err = c.reflector.ListAndWatch(stopChannel)  // ← Returns nil (no error logged)
    if err != nil {
        klog.Errorf("cacher (%v): unexpected ListAndWatch error: %v; reinitializing...",
            c.groupResource.String(), err)
    }
}
// ← Function returns, err == nil
```

---

### STEP 6: wait.Until Waits 1 Second, Then Calls startCaching() Again

The goroutine running `startCaching()` is managed by `wait.Until` (NOT `wait.BackoffUntil`!),
which retries after a **fixed** 1 second delay — regardless of error type or frequency.

**File:** `staging/src/k8s.io/apiserver/pkg/storage/cacher/cacher.go:469-480`

```go
cacher.stopWg.Add(1)
go func() {
    defer cacher.stopWg.Done()
    defer cacher.terminateAllWatchers()
    wait.Until(
        func() {
            if !cacher.isStopped() {
                cacher.startCaching(stopCh)  // ← Called again after delay
            }
        }, time.Second, stopCh,  // ← 1 SECOND delay between calls
    )
}()
```

> **Key Point:** After the first `startCaching()` returns, `wait.Until` waits 1 second
> before calling `startCaching()` again.

---

### STEP 7: terminateAllWatchers() Called BEFORE Re-LIST

When `startCaching()` is called again, `terminateAllWatchers()` is called first,
BEFORE the fresh LIST operation.

**File:** `staging/src/k8s.io/apiserver/pkg/storage/cacher/cacher.go:484-496`

```go
func (c *Cacher) startCaching(stopChannel <-chan struct{}) {
    // ...
    c.terminateAllWatchers()  // ← ALL clients terminated FIRST!
    err = c.reflector.ListAndWatch(stopChannel)  // ← Re-LIST happens AFTER
    // ...
}
```

---

### STEP 8: terminateAllWatchers() Iterates All Watchers

**File:** `staging/src/k8s.io/apiserver/pkg/storage/cacher/cacher.go:1134-1138`

```go
func (c *Cacher) terminateAllWatchers() {
    c.Lock()
    defer c.Unlock()
    c.watchers.terminateAll(c.groupResource, c.stopWatcherLocked)  // ← Passes callback
}
```

**File:** `staging/src/k8s.io/apiserver/pkg/storage/cacher/cacher.go:176-192`

```go
func (i *indexedWatchers) terminateAll(groupResource schema.GroupResource, done func(*cacheWatcher)) {
    // note that we don't have to call setDrainInputBufferLocked method on the watchers
    // because we take advantage of the default value - stop immediately
    if len(i.allWatchers) > 0 || len(i.valueWatchers) > 0 {
        klog.Warningf("Terminating all watchers from cacher %v", groupResource)  // ← Logged!
    }
    for _, watchers := range i.allWatchers {
        watchers.terminateAll(done)  // ← Iterates and calls done()
    }
    for _, watchers := range i.valueWatchers {
        watchers.terminateAll(done)  // ← Iterates and calls done()
    }
    i.allWatchers = map[namespacedName]watchersMap{}
    i.valueWatchers = map[string]watchersMap{}
}
```

**File:** `staging/src/k8s.io/apiserver/pkg/storage/cacher/cacher.go:134-139`

```go
func (wm watchersMap) terminateAll(done func(*cacheWatcher)) {
    for key, watcher := range wm {
        delete(wm, key)
        done(watcher)  // ← Calls stopWatcherLocked(watcher)
    }
}
```

---

### STEP 9: stopWatcherLocked() Calls watcher.stopLocked()

**File:** `staging/src/k8s.io/apiserver/pkg/storage/cacher/cacher.go:1140-1146`

```go
func (c *Cacher) stopWatcherLocked(watcher *cacheWatcher) {
    if c.dispatching {
        c.watchersToStop = append(c.watchersToStop, watcher)  // Defer if dispatching
    } else {
        watcher.stopLocked()  // ← Direct call
    }
}
```

---

### STEP 10: cacheWatcher.stopLocked() Closes the Channels

**File:** `staging/src/k8s.io/apiserver/pkg/storage/cacher/cache_watcher.go:53-88`

```go
// cacheWatcher implements watch.Interface
// this is not thread-safe
type cacheWatcher struct {
    input     chan *watchCacheEvent  // ← Receives events from Cacher
    result    chan watch.Event       // ← Sends events to HTTP handler
    done      chan struct{}          // ← Signals shutdown
    // ...
}
```

**File:** `staging/src/k8s.io/apiserver/pkg/storage/cacher/cache_watcher.go:126-145`

```go
// we rely on the fact that stopLocked is actually protected by Cacher.Lock()
func (c *cacheWatcher) stopLocked() {
    if !c.stopped {
        c.stopped = true
        // stop without draining the input channel was requested.
        if !c.drainInputBuffer {
            close(c.done)   // ← Closes done channel
        }
        close(c.input)      // ← CLOSES INPUT CHANNEL!
    }

    // Even if the watcher was already stopped, if it previously was
    // using draining mode and it's not using it now we need to
    // close the done channel now.
    if !c.drainInputBuffer && !c.isDoneChannelClosedLocked() {
        close(c.done)
    }
}
```

> **Key Point:** `stopLocked()` closes `input` and `done` channels, but NOT `result`.
> The `result` channel is closed by the processing goroutine (Step 12).

---

### STEP 11: process() Goroutine Sees Closed input Channel

The `process()` function runs in a separate goroutine, reading from the `input` channel.
When the channel is closed, the receive returns `ok = false`.

**File:** `staging/src/k8s.io/apiserver/pkg/storage/cacher/cache_watcher.go:520-545`

```go
func (c *cacheWatcher) process(ctx context.Context, resourceVersion uint64) {
    // At this point we already start processing incoming watch events.
    utilflowcontrol.WatchInitialized(ctx)

    for {
        select {
        case event, ok := <-c.input:
            if !ok {
                return  // ← INPUT CHANNEL CLOSED! Goroutine returns
            }
            // ... event processing ...
        case <-ctx.Done():
            return
        }
    }
}
```

---

### STEP 12: processInterval() Closes result Channel via defer

When `process()` returns, control returns to `processInterval()`, which has a
`defer close(c.result)` that now executes.

**File:** `staging/src/k8s.io/apiserver/pkg/storage/cacher/cache_watcher.go:436-518`

```go
func (c *cacheWatcher) processInterval(ctx context.Context, cacheInterval *watchCacheInterval,
    resourceVersion uint64) {
    defer utilruntime.HandleCrash()
    defer close(c.result)  // ← RESULT CHANNEL CLOSED when function returns!
    defer c.Stop()

    // ... initial events processing ...

    // send bookmark after sending all events in cacheInterval for watchlist request
    if cacheInterval.initialEventsEndBookmark != nil {
        c.sendWatchCacheEvent(cacheInterval.initialEventsEndBookmark)
    }
    c.process(ctx, resourceVersion)  // ← When this returns, defers execute
}
// ← defer close(c.result) executes here
```

---

### STEP 13: WatchServer.HandleHTTP() Sees Closed result Channel

The HTTP handler reads from `cacheWatcher.result` (via `ResultChan()`).
When the channel is closed, the receive returns `ok = false`.

**File:** `staging/src/k8s.io/apiserver/pkg/storage/cacher/cache_watcher.go:115-118`

```go
// Implements watch.Interface.
func (c *cacheWatcher) ResultChan() <-chan watch.Event {
    return c.result  // ← This is what HTTP handler reads
}
```

**File:** `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/watch.go:207-283`

```go
// HandleHTTP serves a series of encoded events via HTTP with Transfer-Encoding: chunked.
func (s *WatchServer) HandleHTTP(w http.ResponseWriter, req *http.Request) {
    // ... setup ...

    // begin the stream
    w.Header().Set("Content-Type", s.MediaType)
    w.Header().Set("Transfer-Encoding", "chunked")
    w.WriteHeader(http.StatusOK)
    flusher.Flush()

    gvr := s.Scope.Resource
    watchEncoder := newWatchEncoder(req.Context(), gvr, s.EmbeddedEncoder, s.Encoder, framer)
    ch := s.Watching.ResultChan()  // ← This is cacheWatcher.result
    done := req.Context().Done()

    for {
        select {
        case <-s.ServerShuttingDownCh:
            return
        case <-done:
            return
        case <-timeoutCh:
            return
        case event, ok := <-ch:
            if !ok {
                // End of results.
                return  // ← CHANNEL CLOSED! Handler returns
            }
            metrics.WatchEvents.WithContext(req.Context()).WithLabelValues(
                gvr.Group, gvr.Version, gvr.Resource).Inc()
            // ...
            if err := watchEncoder.Encode(event); err != nil {
                utilruntime.HandleError(err)
                return
            }
            // ...
        }
    }
}
// ← HTTP handler returns, response ends, connection closes
```

---

### STEP 14: HTTP Response Ends → Client Connection Closes

When `HandleHTTP()` returns:
1. The HTTP response body completes (chunked transfer encoding ends)
2. The TCP connection is closed or returned to the pool
3. The client sees the connection close (EOF on read)

**From the client's perspective:**
- The client's watch `ResultChan()` returns `ok = false`
- **NO `watch.Error` event is received** — just channel closure
- The client's Reflector detects this and restarts `ListAndWatch()`

---

## Complete Sequence Diagram

```
Time ─────────────────────────────────────────────────────────────────────────────────▶

┌─────────────────────────────────────────────────────────────────────────────────────┐
│ TRIGGER: Corrupt object deleted in etcd                                             │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ etcd3/watcher.go                                                                     │
│                                                                                      │
│   transform() ──▶ prepareObjs() fails ──▶ corruptObjectDeletedError                 │
│        │                                                                             │
│        ▼                                                                             │
│   collectEventProcessing() ──▶ sendError()                                          │
│        │                                                                             │
│        ▼                                                                             │
│   transformErrorToEvent() ──▶ watch.Error{StatusReasonStoreReadError}               │
│        │                                                                             │
│        ▼                                                                             │
│   wc.resultChan <- errResult                                                        │
│                                                                                      │
└──────────────────────────────────────────┬──────────────────────────────────────────┘
                                           │
                                           ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ client-go/tools/cache/reflector.go                                                   │
│                                                                                      │
│   handleAnyWatch()                                                                   │
│        │ case event, ok := <-w.ResultChan():                                        │
│        │     if event.Type == watch.Error:                                          │
│        │         return apierrors.FromObject(event.Object)  ◀── returns error       │
│        │                                                                             │
│        ▼                                                                             │
│   watch()                                                                            │
│        │ err = handleWatch(...)                                                     │
│        │ switch:                                                                     │
│        │     case apierrors.IsInternalError(err):  ◀── returns FALSE               │
│        │     default:                                                               │
│        │         logger.Info("Warning: watch ended with error", ...)               │
│        │ return nil  ◀── returns NIL, not the error!                               │
│        │                                                                             │
│        ▼                                                                             │
│   ListAndWatch() returns nil                                                        │
│                                                                                      │
└──────────────────────────────────────────┬──────────────────────────────────────────┘
                                           │
                                           ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ apiserver/pkg/storage/cacher/cacher.go                                               │
│                                                                                      │
│   startCaching()                                                                     │
│        │ err = c.reflector.ListAndWatch(stopChannel)  ◀── returns nil              │
│        │ (no error logged because err == nil)                                       │
│        │                                                                             │
│        ▼                                                                             │
│   startCaching() returns                                                            │
│        │                                                                             │
│        ▼                                                                             │
│   wait.Until ──▶ waits 1 SECOND                                                     │
│        │                                                                             │
│        ▼                                                                             │
│   startCaching() called again                                                        │
│        │                                                                             │
│        ▼                                                                             │
│   terminateAllWatchers()  ◀── CALLED BEFORE re-LIST!                               │
│        │                                                                             │
│        ▼                                                                             │
│   indexedWatchers.terminateAll()                                                    │
│        │ klog.Warningf("Terminating all watchers from cacher %v", ...)             │
│        │ for each watcher: done(watcher)                                            │
│        │                                                                             │
│        ▼                                                                             │
│   stopWatcherLocked(watcher)                                                        │
│        │                                                                             │
│        ▼                                                                             │
│   watcher.stopLocked()                                                              │
│        │ close(c.done)                                                              │
│        │ close(c.input)  ◀── INPUT CHANNEL CLOSED                                  │
│                                                                                      │
└──────────────────────────────────────────┬──────────────────────────────────────────┘
                                           │
                                           │ (in separate goroutine per cacheWatcher)
                                           ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ apiserver/pkg/storage/cacher/cache_watcher.go                                        │
│                                                                                      │
│   process()                                                                          │
│        │ for {                                                                       │
│        │     select {                                                               │
│        │     case event, ok := <-c.input:                                          │
│        │         if !ok {                                                           │
│        │             return  ◀── INPUT CLOSED, returns                             │
│        │         }                                                                  │
│        │     }                                                                      │
│        │ }                                                                          │
│        │                                                                             │
│        ▼                                                                             │
│   processInterval() ──▶ defer close(c.result)  ◀── RESULT CHANNEL CLOSED          │
│                                                                                      │
└──────────────────────────────────────────┬──────────────────────────────────────────┘
                                           │
                                           │ (in HTTP handler goroutine)
                                           ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ apiserver/pkg/endpoints/handlers/watch.go                                            │
│                                                                                      │
│   WatchServer.HandleHTTP()                                                          │
│        │ ch := s.Watching.ResultChan()                                             │
│        │ for {                                                                      │
│        │     select {                                                               │
│        │     case event, ok := <-ch:                                               │
│        │         if !ok {                                                           │
│        │             // End of results.                                             │
│        │             return  ◀── CHANNEL CLOSED, handler returns                   │
│        │         }                                                                  │
│        │     }                                                                      │
│        │ }                                                                          │
│        │                                                                             │
│        ▼                                                                             │
│   HTTP handler returns ──▶ Response ends ──▶ Connection closes                      │
│                                                                                      │
└──────────────────────────────────────────┬──────────────────────────────────────────┘
                                           │
                                           ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ CLIENT (client-go Reflector using RunWithContext)                                    │
│                                                                                      │
│   handleAnyWatch()                                                                   │
│        │ case event, ok := <-w.ResultChan():                                        │
│        │     if !ok {                                                               │
│        │         break loop  ◀── CHANNEL CLOSED (no error!)                        │
│        │     }                                                                      │
│        │                                                                             │
│        ▼                                                                             │
│   Returns (false, nil)  ◀── Note: error is nil!                                     │
│        │                                                                             │
│        ▼                                                                             │
│   watch() - err is nil, so FOR LOOP CONTINUES (does NOT return!)                    │
│        │                                                                             │
│        ▼                                                                             │
│   Tries to reconnect with ResourceVersion: r.LastSyncResourceVersion()              │
│        │                                                                             │
│        ▼                                                                             │
│   Server returns "too old resource version" error                                   │
│        │                                                                             │
│        ▼                                                                             │
│   isExpiredError(err) = true ──▶ watch() returns nil                               │
│        │                                                                             │
│        ▼                                                                             │
│   ListAndWatchWithContext() returns nil                                             │
│        │                                                                             │
│        ▼                                                                             │
│   RunWithContext() ──▶ BackoffUntil retries ──▶ calls ListAndWatchWithContext()    │
│        │                                                                             │
│        ▼                                                                             │
│   r.list(ctx) ──▶ FRESH LIST ──▶ store.Replace() ──▶ CLIENT CACHE REBUILT          │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Summary Table

| Step | File | Line | What Happens |
|------|------|------|--------------|
| 1 | `etcd3/watcher.go` | 572-576 | `transform()` calls `prepareObjs()` which fails |
| 2 | `etcd3/watcher.go` | 745-756 | Error wrapped as `corruptObjectDeletedError` |
| 3 | `etcd3/watcher.go` | 545-547 | `sendError()` called |
| 4 | `etcd3/watcher.go` | 663-667 | `watch.Error` event sent to `resultChan` |
| 5 | `reflector.go` | 897-898 | `handleAnyWatch()` returns error |
| 6 | `reflector.go` | 560-567 | `watch()` logs warning, returns `nil` |
| 7 | `cacher.go` | 496 | ⚠️ `ListAndWatch()` returns `nil` (direct call, NOT via `RunWithContext`!) |
| 8 | `cacher.go` | 478 | ⚠️ `wait.Until` waits 1 second (fixed delay, NO exponential backoff!) |
| 9 | `cacher.go` | 495 | `terminateAllWatchers()` called |
| 10 | `cacher.go` | 1137 | Iterates all watchers |
| 11 | `cacher.go` | 1144 | `watcher.stopLocked()` called |
| 12 | `cache_watcher.go` | 131-133 | `close(done)` and `close(input)` |
| 13 | `cache_watcher.go` | 531-533 | `process()` sees closed channel, returns |
| 14 | `cache_watcher.go` | 438 | `defer close(c.result)` executes |
| 15 | `watch.go` | 261-264 | HTTP handler sees closed channel, returns |
| 16 | — | — | HTTP response ends, connection closes |
| | | | |
| **Client-Side (using RunWithContext)** | | | |
| 17 | `reflector.go` | 893-895 | Client's `handleAnyWatch()` sees channel close, returns `nil` |
| 18 | `reflector.go` | 510-523 | Client tries to resume from `lastSyncResourceVersion` |
| 19 | `watch_cache.go` | 915-916 | Server returns "too old resource version" error |
| 20 | `reflector.go` | 547-567 | `isExpiredError` → `watch()` returns `nil` |
| 21 | `reflector.go` | 357-361 | `RunWithContext()` retries after backoff |
| 22 | `reflector.go` | 426 | Fresh LIST → `store.Replace()` → **CLIENT CACHE REBUILT** |

> **⚠️ Note:** Steps 7-8 highlight that the Cacher bypasses `RunWithContext()` and manages its own
> retry loop with `wait.Until`. This is why there's no exponential backoff and why
> `terminateAllWatchers()` is called on every restart.
>
> **📝 Note:** Steps 17-22 show the client-side flow. The client doesn't immediately rebuild its
> cache on channel close — it first tries to resume, gets a "too old" error, and only then
> does a fresh LIST.

---

## Key Insights

### 1. The Client Never Receives a `watch.Error` Event

The `watch.Error` event from etcd3 is consumed by the backend Reflector.
Clients only see their watch channel close — they have no idea WHY it closed.

### 2. Error Classification Matters

The `StatusReasonStoreReadError` is in the `knownReasons` map, so `IsInternalError()`
returns `false`. This means the error is NOT retried internally by the Reflector.

### 3. Timing: 1 Second Delay Before Client Termination

After the watch error:
1. `watch()` returns immediately
2. `startCaching()` returns
3. `wait.Until` waits **1 second**
4. `startCaching()` called again
5. `terminateAllWatchers()` terminates clients

### 4. Clients Are Terminated BEFORE Re-LIST

`terminateAllWatchers()` is called at the START of `startCaching()`, before
`reflector.ListAndWatch()`. This means clients are disconnected before new
data is loaded.

### 5. Channel Closure Cascade

```
close(input) ──▶ process() returns ──▶ close(result) ──▶ HTTP handler returns
```

The closure propagates through the goroutine chain via deferred channel closes.

### 6. Cacher's Direct ListAndWatch Usage (Architectural)

The Cacher **bypasses `RunWithContext()`** and calls `ListAndWatch()` directly. This is
a critical architectural choice that explains why ALL watchers are terminated on ANY error:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  NORMAL CLIENT (using RunWithContext)                                                │
│                                                                                      │
│  RunWithContext() {                                                                  │
│      wait.BackoffUntil(func() {                                                     │
│          err := ListAndWatchWithContext()                                           │
│          if err != nil {                                                            │
│              watchErrorHandler(err)  ◀── Custom error handling possible             │
│          }                                                                          │
│      }, backoffManager)             ◀── Exponential backoff (800ms→30s)            │
│  }                                                                                  │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────┐
│  CACHER (bypasses RunWithContext)                                                    │
│                                                                                      │
│  wait.Until(func() {                                                                │
│      startCaching() {                                                               │
│          terminateAllWatchers()     ◀── ALL clients disconnected FIRST!            │
│          err := ListAndWatch()                                                      │
│          if err != nil {                                                            │
│              klog.Errorf(...)       ◀── Just logs, no custom handler               │
│          }                                                                          │
│      }                                                                              │
│  }, 1*time.Second)                  ◀── Fixed 1-second delay, NO backoff           │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

**Consequences of this design:**
- No exponential backoff on repeated failures
- No opportunity for custom error handling via `watchErrorHandler`
- `terminateAllWatchers()` called unconditionally on every `startCaching()` invocation
- All client watchers see channel closure, not the original `watch.Error`

### 7. Client Cache Rebuild After Forced Disconnection

When a client is forcibly disconnected, it **eventually rebuilds its local cache**, but not
immediately. The mechanism involves a "too old" error from the server.

#### The Complete Client-Side Flow

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  PHASE 1: CHANNEL CLOSES                                                             │
│                                                                                      │
│     Client's watch ResultChan returns ok=false                                      │
│     Client's handleAnyWatch() breaks out of loop, returns (false, nil)              │
│     Client's watch() function - err is nil, so FOR LOOP CONTINUES (no return!)      │
│                                          │                                          │
│                                          ▼                                          │
│  PHASE 2: CLIENT TRIES TO RESUME (NOT a fresh list!)                                │
│                                                                                      │
│     watch() creates new watch request with:                                         │
│         ResourceVersion: r.LastSyncResourceVersion()  ← Last known RV               │
│                                          │                                          │
│                                          ▼                                          │
│  PHASE 3: SERVER REJECTS WITH "TOO OLD" ERROR                                       │
│                                                                                      │
│     Cacher's watchCache after Replace():                                            │
│       • Event buffer emptied (startIndex = endIndex)                                │
│       • oldest = listResourceVersion + 1                                            │
│       • If client's RV < oldest-1 → "too old resource version" error                │
│                                          │                                          │
│                                          ▼                                          │
│  PHASE 4: CLIENT'S watch() RETURNS nil                                              │
│                                                                                      │
│     isExpiredError(err) = true → falls through to return nil                        │
│                                          │                                          │
│                                          ▼                                          │
│  PHASE 5: RunWithContext() RETRIES AFTER BACKOFF                                    │
│                                                                                      │
│     wait.BackoffUntil calls ListAndWatchWithContext() again                         │
│                                          │                                          │
│                                          ▼                                          │
│  PHASE 6: FRESH LIST - CLIENT CACHE REBUILT!                                        │
│                                                                                      │
│     ListAndWatchWithContext() starts fresh:                                         │
│       err = r.list(ctx)  ← FULL LIST FROM SERVER                                    │
│       store.Replace(...)  ← CLIENT'S LOCAL CACHE REPLACED                           │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

#### Key Code: Client Tries to Resume (Not Fresh List Initially)

**File:** `staging/src/k8s.io/client-go/tools/cache/reflector.go:510-523`

```go
// Inside the watch() for loop - client tries to RESUME, not fresh list
if w == nil {
    options := metav1.ListOptions{
        ResourceVersion: r.LastSyncResourceVersion(),  // ← Tries to resume from last RV!
        TimeoutSeconds:  &timeoutSeconds,
        AllowWatchBookmarks: true,
    }
    w, err = r.listerWatcher.WatchWithContext(ctx, options)
```

#### Key Code: Server Rejects With "Too Old" Error

**File:** `staging/src/k8s.io/apiserver/pkg/storage/cacher/watch_cache.go:885-916`

```go
// After Replace(), the buffer is empty and oldest = listResourceVersion + 1
case w.listResourceVersion > 0 && !w.removedEventSinceRelist:
    oldest = w.listResourceVersion + 1

// ...

// Client's old RV is rejected
if resourceVersion < oldest-1 {
    return nil, errors.NewResourceExpired(
        fmt.Sprintf("too old resource version: %d (%d)", resourceVersion, oldest-1))
}
```

#### Key Code: Expired Error Triggers Fresh List

**File:** `staging/src/k8s.io/client-go/tools/cache/reflector.go:546-567`

```go
case isExpiredError(err):
    // Don't set LastSyncResourceVersionUnavailable...
    logger.V(4).Info("Watch closed", ...)
    // Falls through to return nil
}
return nil  // ← watch() returns, ListAndWatchWithContext() returns

// Then RunWithContext() calls ListAndWatchWithContext() again:
// ListAndWatchWithContext() → r.list(ctx) → FRESH LIST!
```

#### Why The Server Rejects The Resume Attempt

When `watchCache.Replace()` is called during Cacher re-initialization:

**File:** `staging/src/k8s.io/apiserver/pkg/storage/cacher/watch_cache.go:771-785`

```go
// Replace() empties the event buffer
w.startIndex = w.endIndex              // ← Buffer emptied!
w.removedEventSinceRelist = false
// ...
w.listResourceVersion = version        // ← New list RV (e.g., 150)
w.resourceVersion = version
```

If the client was watching at RV=100 and the new list is at RV=150:
- Client tries to watch from RV=100
- Server computes `oldest = 150 + 1 = 151`
- Check: `100 < 151 - 1` → `100 < 150` → **TRUE**
- Server returns: `"too old resource version: 100 (150)"`

#### Summary: Client Cache Rebuild Sequence

| Step | What Happens | Cache Rebuilt? |
|------|--------------|----------------|
| 1. Channel closes | Client's watch loop continues (err=nil) | ❌ No |
| 2. Resume attempt | Client requests watch from last RV | ❌ No |
| 3. Server rejects | Returns "too old resource version" error | ❌ No |
| 4. watch() returns | `isExpiredError` triggers return nil | ❌ No |
| 5. Backoff | `RunWithContext` waits, then retries | ❌ No |
| 6. **Fresh list** | **New `ListAndWatchWithContext()` does full LIST** | ✅ **YES** |

> **Key Insight:** The client cache rebuild is triggered by the "too old" error from the
> server, NOT by the initial channel closure. The client first tries to resume watching
> from its last known resourceVersion before falling back to a fresh list.
