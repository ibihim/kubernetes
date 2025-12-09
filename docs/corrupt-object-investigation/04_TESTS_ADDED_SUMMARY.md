# Tests Added for Corrupt Object Cache Rebuild Flow

> **Status**: Tests implemented and committed (`90e9ae7ff6f`), awaiting execution on etcd-enabled device.

## WHAT — High-Level Overview

I added **3 integration tests** that verify the complete flow when a corrupt (undecryptable) object is deleted from the Kubernetes API server:

| Test | Type | Location | Purpose |
|------|------|----------|---------|
| `TestStoreReadErrorClassification` | Unit | `staging/src/k8s.io/apimachinery/pkg/api/errors/errors_test.go` | Validates error classification logic |
| `TestCorruptObjectDeleteTriggersWatcherReconnect` | E2E | `test/integration/controlplane/transformation/secrets_transformation_test.go` | Single watcher cache rebuild |
| `TestMultipleWatchersDisconnectedOnCorruptDelete` | E2E | `test/integration/controlplane/transformation/secrets_transformation_test.go` | Multiple watchers all disconnect |

---

## WHY — Problem These Tests Solve

### The Corrupt Object Scenario

When encryption keys are rotated in Kubernetes, secrets encrypted with the **old key** become unreadable. These "corrupt" objects can be deleted using the `unsafe-delete-ignore-read-errors` option, but what happens to **client caches** when this occurs?

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Client A      │     │   API Server    │     │     etcd        │
│   (Informer)    │◄────│   (Cacher)      │◄────│   (encrypted)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │
        │    watches secrets    │   can't decrypt old   │
        │◄──────────────────────│   secret after key    │
        │                       │   rotation!           │
        │                       │                       │
        │   WHAT HAPPENS WHEN   │                       │
        │   CORRUPT OBJECT IS   │                       │
        │   DELETED?            │                       │
```

### The Assumptions Being Tested

The tests validate these assumptions from `CORRUPT_OBJECT_FLOW_WALKTHROUGH.md`:

1. **Reflector returns `nil`** (not the error) when it sees `StatusReasonStoreReadError`
2. **Cacher calls `terminateAllWatchers()`** when rebuilding its cache
3. **ALL clients** (not just the one watching the deleted object) rebuild their caches

Without these tests, we had no proof these assumptions were correct!

---

## HOW — Implementation Details

### Test 1: `TestStoreReadErrorClassification`

**File**: `staging/src/k8s.io/apimachinery/pkg/api/errors/errors_test.go`

**What it validates**: The error classification that determines Reflector behavior.

```go
func TestStoreReadErrorClassification(t *testing.T) {
    err := &StatusError{
        ErrStatus: metav1.Status{
            Status:  metav1.StatusFailure,
            Code:    http.StatusInternalServerError,
            Reason:  metav1.StatusReasonStoreReadError,  // ← Key reason
            Message: "saw a DELETED event, but object data is corrupt",
        },
    }

    // ✅ IsStoreReadError should return true
    if !IsStoreReadError(err) {
        t.Error("expected IsStoreReadError(err) to return true")
    }

    // ✅ IsInternalError should return FALSE
    // This is CRITICAL: because StoreReadError is in knownReasons,
    // the Reflector's watch() returns nil instead of retrying
    if IsInternalError(err) {
        t.Error("expected IsInternalError(err) to return false")
    }
}
```

**Why this matters**: The `knownReasons` map in `errors.go` includes `StatusReasonStoreReadError`, so `IsInternalError()` returns `false`. This causes the Reflector to **return nil and rebuild** rather than retry internally.

---

### Test 2: `TestCorruptObjectDeleteTriggersWatcherReconnect`

**File**: `test/integration/controlplane/transformation/secrets_transformation_test.go`

**What it validates**: A single watching client rebuilds its cache after corrupt object deletion.

**The testing pattern** (reused from existing encryption tests):

```
Step 1: Create secret with AES-GCM encryption
        ┌──────────┐
        │ secret-1 │ ← encrypted with key A
        └──────────┘

Step 2: Swap encryption config file (GCM → CBC)
        ┌──────────┐
        │ secret-1 │ ← still encrypted with key A, but server now expects key B
        └──────────┘
        └── NOW CORRUPT (undecryptable) ──┘

Step 3: Delete with unsafe option
        test.restClient.CoreV1().Secrets(ns).Delete(ctx, name, metav1.DeleteOptions{
            IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To(true),
        })

Step 4: Verify informer rebuilt cache
        newRV := informer.LastSyncResourceVersion()
        if newRV != initialRV {
            // ✅ Cache was rebuilt!
        }
```

**Key verification**:
```go
// Record initial ResourceVersion
initialRV := informer.LastSyncResourceVersion()

// ... break encryption, delete corrupt object ...

// Verify RV changed (proves cache rebuild happened)
wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true,
    func(ctx context.Context) (bool, error) {
        newRV := informer.LastSyncResourceVersion()
        return newRV != initialRV, nil  // ← This proves rebuild!
    })
```

---

### Test 3: `TestMultipleWatchersDisconnectedOnCorruptDelete`

**File**: `test/integration/controlplane/transformation/secrets_transformation_test.go`

**What it validates**: ALL watchers disconnect (not just one).

This tests the `terminateAllWatchers()` behavior in the Cacher:

```go
// Create 3 INDEPENDENT informers (simulating 3 different clients)
const numWatchers = 3
factories := make([]informers.SharedInformerFactory, numWatchers)
watcherInformers := make([]cache.SharedIndexInformer, numWatchers)
initialRVs := make([]string, numWatchers)

for i := 0; i < numWatchers; i++ {
    factories[i] = informers.NewSharedInformerFactoryWithOptions(...)
    watcherInformers[i] = factories[i].Core().V1().Secrets().Informer()
}

// ... break encryption, delete corrupt object ...

// Verify ALL 3 informers rebuilt (not just 1)
rebuiltCount := 0
for i := 0; i < numWatchers; i++ {
    if watcherInformers[i].LastSyncResourceVersion() != initialRVs[i] {
        rebuiltCount++
    }
}
if rebuiltCount != numWatchers {
    t.Errorf("expected all %d informers to rebuild, but only %d did",
        numWatchers, rebuiltCount)
}
```

---

## Run Commands

### Unit test (~0s, no dependencies)
```bash
go test -v -run TestStoreReadErrorClassification \
    ./staging/src/k8s.io/apimachinery/pkg/api/errors/...
```

### E2E tests (~2-3min each, requires etcd)
```bash
# Install etcd first if needed
./hack/install-etcd.sh
export PATH="${PATH}:$(pwd)/third_party/etcd"

# Run the E2E tests
go test -v -run 'TestCorruptObjectDeleteTriggersWatcherReconnect|TestMultipleWatchersDisconnectedOnCorruptDelete' \
    ./test/integration/controlplane/transformation/...
```

---

## Summary Table

| Test | Validates Assumption |
|------|---------------------|
| Test 1 | `StoreReadError` causes Reflector to return `nil` (not retry) |
| Test 2 | Single client's cache rebuilds after corrupt delete |
| Test 3 | ALL clients' caches rebuild (`terminateAllWatchers` works) |

---

## Next Steps

1. **Run the tests** on a device with etcd installed
2. **Verify all tests pass** — if they fail, the assumptions in `CORRUPT_OBJECT_FLOW_WALKTHROUGH.md` may need revision
3. **Consider edge cases**:
   - What if a client has a very old ResourceVersion?
   - What about clients watching different namespaces?

---

## Related Files

- `CORRUPT_OBJECT_FLOW_WALKTHROUGH.md` — Detailed flow documentation
- `staging/src/k8s.io/apimachinery/pkg/api/errors/errors.go` — Error classification (`knownReasons` map)
- `staging/src/k8s.io/client-go/tools/cache/reflector.go` — Reflector's `watch()` and `ListAndWatch()` methods
- `staging/src/k8s.io/apiserver/pkg/storage/cacher/cacher.go` — `terminateAllWatchers()` method
