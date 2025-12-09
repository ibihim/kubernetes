# etcd3 Watcher Architecture — UML Diagrams

This document contains UML diagrams explaining how the Kubernetes API server watches etcd
and distributes events to subscribers.

---

## 1. Class/Struct Diagram — Component Relationships

This diagram shows how the main structs relate to each other.

```mermaid
classDiagram
    direction TB

    %% Interfaces
    class IWatch {
        <<interface>>
        +ResultChan() channel
        +Stop()
    }

    class IStorage {
        <<interface>>
        +Watch() IWatch
        +GetList() error
        +Get() error
    }

    class IListerWatcher {
        <<interface>>
        +List() Object
        +Watch() IWatch
    }

    class IStore {
        <<interface>>
        +Add(obj) error
        +Update(obj) error
        +Delete(obj) error
        +List() slice
        +Replace() error
    }

    %% etcd3 layer - THIS IS THE ROOT on apiserver side
    class etcd3store {
        <<etcd3/store.go>>
        -client : etcdClient
        -codec : Codec
        -versioner : Versioner
        -transformer : Transformer
        -pathPrefix : string
        -groupResource : GroupResource
        -watcher : watcher
        -leaseManager : leaseManager
        -decoder : Decoder
        +Watch() IWatch
        +Get() error
        +GetList() error
        +Create() error
        +Delete() error
        +GuaranteedUpdate() error
    }

    class watcher {
        <<etcd3/watcher.go>>
        -client : etcdClient
        -codec : Codec
        -versioner : Versioner
        -transformer : Transformer
        +Watch() IWatch
    }

    class watchChan {
        <<etcd3/watcher.go>>
        -watcher : watcher
        -key : string
        -incomingEventChan : channel
        -resultChan : channel
        -ctx : Context
        +ResultChan() channel
        +Stop()
    }

    %% Cacher layer
    class listerWatcher {
        -storage : IStorage
        -resourcePrefix : string
        -newListFunc : function
        +List() Object
        +Watch() IWatch
    }

    class Reflector {
        -listerWatcher : IListerWatcher
        -store : IStore
        -expectedType : Type
        +ListAndWatch() error
        +LastSyncResourceVersion() string
    }

    class watchCache {
        -cache : eventSlice
        -startIndex : int
        -endIndex : int
        -capacity : int
        -store : storeIndexer
        -snapshots : Snapshotter
        -eventHandler : callback
        -resourceVersion : uint64
        +Add(obj) error
        +Update(obj) error
        +Delete(obj) error
        +getAllEventsSinceLocked() interval
    }

    class storeSnapshotter {
        <<store_btree.go>>
        -snapshots : BTreeG[rvSnapshot]
        +Add(rv, indexer)
        +GetLessOrEqual(rv) orderedLister
        +RemoveLess(rv)
        +Reset()
    }

    class btreeStore {
        <<store_btree.go>>
        -tree : BTreeG[storeElement]
        +Clone() orderedLister
        +ListPrefix(prefix) slice
        +Add(obj) error
        +Update(obj) error
        +Delete(obj) error
    }

    class watchCacheEvent {
        +Type : EventType
        +Object : Object
        +PrevObject : Object
        +Key : string
        +ResourceVersion : uint64
        +ObjLabels : LabelSet
        +ObjFields : FieldSet
    }

    class Cacher {
        -incoming : channel
        -watchCache : watchCache
        -reflector : Reflector
        -watchers : indexedWatchers
        -watcherIdx : int
        -bookmarkWatchers : timeBuckets
        +Watch() IWatch
        +processEvent()
        -dispatchEvents()
        -dispatchEvent()
        -startDispatching()
    }

    class indexedWatchers {
        -allWatchers : watcherMap
        -valueWatchers : watcherMap
        +addWatcher()
        +deleteWatcher()
    }

    class cacheWatcher {
        -input : channel
        -result : channel
        -filter : filterFunc
        -forget : forgetFunc
        -stopped : bool
        +ResultChan() channel
        +Stop()
        +nonblockingAdd() bool
        +add() bool
    }

    class watchCacheInterval {
        -startIndex : int
        -endIndex : int
        -indexer : indexerFunc
        -indexValidator : validatorFunc
        +Next() event
    }

    %% Relationships

    %% etcd3 layer relationships
    etcd3store ..|> IStorage : implements
    etcd3store *-- watcher : owns
    watcher ..> watchChan : creates per Watch call
    watchChan ..|> IWatch : implements

    %% listerWatcher bridges etcd3 and Cacher
    listerWatcher ..|> IListerWatcher : implements
    listerWatcher --> etcd3store : wraps

    %% Reflector connects listerWatcher to watchCache
    Reflector --> listerWatcher : calls List and Watch
    Reflector --> watchCache : calls Add Update Delete

    %% watchCache layer
    watchCache ..|> IStore : implements
    watchCache *-- watchCacheEvent : contains in cyclic buffer
    watchCache --> Cacher : calls eventHandler
    watchCache *-- btreeStore : store (current state)
    watchCache *-- storeSnapshotter : snapshots (historical states)
    storeSnapshotter o-- btreeStore : COW clones per RV

    %% Cacher owns everything and dispatches
    Cacher *-- watchCache : owns
    Cacher *-- Reflector : owns
    Cacher *-- indexedWatchers : owns
    Cacher --> cacheWatcher : dispatches to

    %% Watcher management
    indexedWatchers o-- cacheWatcher : manages many

    %% cacheWatcher delivers to clients
    cacheWatcher ..|> IWatch : implements
    cacheWatcher --> watchCacheInterval : reads initial events

    watchCacheInterval --> watchCache : reads from buffer
```

---

## 2. Sequence Diagram — Event Flow from etcd to Client

This diagram shows the runtime flow when an event occurs in etcd.

```mermaid
sequenceDiagram
    autonumber
    participant etcd as etcd cluster
    participant ew as etcd3/watcher.go<br/>(watchChan)
    participant lw as listerWatcher
    participant ref as Reflector
    participant wc as watchCache
    participant cacher as Cacher
    participant disp as dispatchEvents()<br/>goroutine
    participant iw as indexedWatchers
    participant cw as cacheWatcher
    participant client as API Client<br/>(kubectl, controller)

    Note over etcd,client: SETUP PHASE (happens once at startup)

    cacher->>wc: newWatchCache(..., cacher.processEvent)
    Note right of wc: eventHandler = cacher.processEvent

    cacher->>lw: NewListerWatcher(storage, prefix)
    cacher->>ref: NewNamedReflector(lw, watchCache)
    Note right of ref: store = watchCache

    cacher->>disp: go dispatchEvents()
    cacher->>ref: go reflector.ListAndWatch()

    ref->>lw: Watch(options)
    lw->>ew: storage.Watch(ctx, prefix, opts)
    ew->>etcd: gRPC Watch stream
    ew-->>lw: watch.Interface (watchChan)
    lw-->>ref: watch.Interface

    Note over etcd,client: EVENT FLOW (repeats for each event)

    etcd->>ew: WatchResponse (Pod updated)
    ew->>ew: transform & filter event

    rect rgb(240, 248, 255)
        Note over ref,wc: Reflector reads event and updates Store
        ref->>ew: event := <-w.ResultChan()
        ew-->>ref: watch.Event{Type: Modified, Object: Pod}

        alt event.Type == Added
            ref->>wc: store.Add(pod)
        else event.Type == Modified
            ref->>wc: store.Update(pod)
        else event.Type == Deleted
            ref->>wc: store.Delete(pod)
        end
    end

    rect rgb(255, 248, 240)
        Note over wc,cacher: watchCache updates buffer and calls callback
        wc->>wc: updateCache(event)<br/>cyclic buffer write
        wc->>wc: store.Update(elem)<br/>current state update
        wc->>cacher: eventHandler(wcEvent)
        Note right of cacher: This IS cacher.processEvent()
    end

    rect rgb(240, 255, 240)
        Note over cacher,disp: Cacher queues event
        cacher->>cacher: c.incoming <- event
    end

    rect rgb(255, 240, 255)
        Note over disp,cw: dispatchEvents routes to watchers
        disp->>disp: event := <-c.incoming
        disp->>cacher: dispatchEvent(&event)
        cacher->>cacher: startDispatching(event)
        cacher->>iw: find watchers by ns/name
        iw-->>cacher: watchersBuffer (matching watchers)

        loop for each watcher in watchersBuffer
            cacher->>cw: nonblockingAdd(event)
            cw->>cw: input <- event
        end
    end

    rect rgb(240, 255, 255)
        Note over cw,client: cacheWatcher delivers to client
        cw->>cw: process() reads input
        cw->>cw: convertToWatchEvent(event)
        cw->>cw: result <- watchEvent
        client->>cw: event := <-ResultChan()
        cw-->>client: watch.Event{Type: Modified, Object: Pod}
    end
```

---

## 3. Sequence Diagram — New Watch Subscription

This diagram shows what happens when a new client starts watching.

```mermaid
sequenceDiagram
    autonumber
    participant client as API Client
    participant cacher as Cacher
    participant wc as watchCache
    participant iw as indexedWatchers
    participant cw as cacheWatcher
    participant interval as watchCacheInterval

    client->>cacher: Watch(ctx, "/pods", opts)<br/>ResourceVersion: 1000

    Note over cacher,wc: Wait for cache to be fresh enough
    cacher->>wc: waitUntilFreshAndBlock(ctx, 1000)
    wc->>wc: wait until resourceVersion >= 1000
    wc-->>cacher: ready (holding RLock)

    Note over cacher,interval: Get historical events from cyclic buffer
    cacher->>wc: getAllEventsSinceLocked(1000, key, opts)
    wc->>wc: binary search for RV > 1000
    wc->>interval: newCacheInterval(startIdx, endIdx, indexer)
    wc-->>cacher: watchCacheInterval

    Note over cacher,cw: Create and register watcher
    cacher->>cw: newCacheWatcher(chanSize, filter, ...)
    cacher->>cacher: Lock()
    cacher->>iw: addWatcher(watcher, idx, scope, trigger)
    cacher->>cacher: watcherIdx++
    cacher->>cacher: Unlock()

    Note over cw,client: Start processing in background
    cacher->>cw: go processInterval(ctx, interval, rv)
    cacher-->>client: return watcher (watch.Interface)

    rect rgb(240, 248, 255)
        Note over cw,interval: Phase 1: Send historical events
        loop while interval.Next() returns events
            cw->>interval: Next()
            interval->>wc: read from cyclic buffer
            interval-->>cw: watchCacheEvent (RV 1001)
            cw->>cw: convertToWatchEvent()
            cw->>cw: result <- event
            client->>cw: <-ResultChan()
            cw-->>client: watch.Event (RV 1001)
        end
    end

    rect rgb(240, 255, 240)
        Note over cw,client: Phase 2: Receive live events (via dispatchEvent)
        Note right of cw: Now watcher receives events<br/>through input channel<br/>from Cacher.dispatchEvent()
    end
```

---

## 4. Component Diagram — High-Level Architecture

```mermaid
flowchart TB
    subgraph etcd_cluster["etcd cluster"]
        etcd[(etcd)]
    end

    subgraph apiserver["kube-apiserver"]
        subgraph storage_layer["Storage Layer"]
            etcd3_watcher["etcd3/watcher.go<br/>watchChan"]
            etcd3_store["etcd3/store.go"]
        end

        subgraph cacher_layer["Cacher Layer (per resource type)"]
            lister_watcher["listerWatcher<br/>(adapts storage.Interface<br/>to cache.ListerWatcher)"]

            subgraph cacher_box["Cacher"]
                reflector["Reflector<br/>(client-go)"]
                subgraph watch_cache_box["watchCache"]
                    ring_buffer["Cyclic Buffer<br/>(event history)"]
                    btree_store["B-tree Store<br/>(current state)"]
                    snapshotter["Snapshotter<br/>(COW clones by RV)"]
                end
                incoming["incoming channel<br/>(cap: 100)"]
                dispatch["dispatchEvents()<br/>goroutine"]
                watchers_registry["indexedWatchers<br/>• allWatchers (by ns/name)<br/>• valueWatchers (by index)"]
            end
        end

        subgraph subscribers["Subscribers"]
            cw1["cacheWatcher 1"]
            cw2["cacheWatcher 2"]
            cw3["cacheWatcher N"]
        end
    end

    subgraph clients["API Clients"]
        kubectl["kubectl"]
        controller["Controllers"]
        operator["Operators"]
    end

    etcd <-->|gRPC Watch| etcd3_watcher
    etcd3_watcher --> lister_watcher
    lister_watcher --> reflector
    reflector -->|Add/Update/Delete| watch_cache_box
    watch_cache_box -->|eventHandler callback| incoming
    incoming --> dispatch
    dispatch -->|startDispatching| watchers_registry
    watchers_registry -.->|find matching| cw1
    watchers_registry -.->|find matching| cw2
    watchers_registry -.->|find matching| cw3
    dispatch -->|nonblockingAdd| cw1
    dispatch -->|nonblockingAdd| cw2
    dispatch -->|nonblockingAdd| cw3
    cw1 --> kubectl
    cw2 --> controller
    cw3 --> operator

    style ring_buffer fill:#e1f5fe
    style btree_store fill:#fff3e0
    style snapshotter fill:#e8f5e9
    style dispatch fill:#fff3e0
    style watchers_registry fill:#f3e5f5
```

---

## 5. State Diagram — cacheWatcher Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Created: newCacheWatcher()

    Created --> Registered: addWatcher()

    Registered --> ProcessingHistory: go processInterval()

    ProcessingHistory --> ProcessingHistory: interval.Next()<br/>send historical event
    ProcessingHistory --> ProcessingLive: interval exhausted

    ProcessingLive --> ProcessingLive: receive from input channel<br/>send to result channel
    ProcessingLive --> Stopping: Stop() called or<br/>context canceled or<br/>blocked too long

    Stopping --> Stopped: stopLocked()

    Stopped --> [*]: channels closed

    note right of ProcessingHistory
        Reading from watchCacheInterval
        (cyclic buffer snapshot)
    end note

    note right of ProcessingLive
        Receiving from Cacher.dispatchEvent()
        via input channel
    end note
```

---

## 6. Data Flow Diagram — Cyclic Buffer Operations

```mermaid
flowchart LR
    subgraph cyclic_buffer["Cyclic Buffer (watchCache.cache)"]
        direction LR
        slot0["[0]"]
        slot1["[1]"]
        slot2["[2]"]
        slot3["[3]"]
        slot4["[4]<br/>RV:1005"]
        slot5["[5]<br/>RV:1008"]
        slot6["[6]<br/>RV:1012"]
        slot7["[7]<br/>RV:1015"]
    end

    startIdx["startIndex=12<br/>(12%8=4)"]
    endIdx["endIndex=16<br/>(16%8=0)"]

    startIdx -.->|points to| slot4
    endIdx -.->|next write| slot0

    subgraph operations["Operations"]
        write["Write new event:<br/>cache[endIndex%cap] = event<br/>endIndex++"]
        evict["When full:<br/>startIndex++<br/>(evict oldest)"]
        read["Read for watcher:<br/>binary search RV<br/>return interval [start, end)"]
    end

    write --> slot0
    evict --> slot4
    read --> slot5
    read --> slot6
    read --> slot7
```

---

## 7. Sequence Diagram — Cache Reset and Watcher Termination

This diagram shows what happens when an error (e.g., corrupt object deletion) causes
the Cacher to reset. **Important:** The Cacher bypasses `RunWithContext` and calls
`ListAndWatch` directly, using `wait.Until` for retry logic.

```mermaid
sequenceDiagram
    autonumber
    participant etcd as etcd cluster
    participant ew as etcd3/watcher.go<br/>(watchChan)
    participant ref as Reflector<br/>(backend)
    participant cacher as Cacher
    participant wutil as wait.Until<br/>goroutine
    participant iw as indexedWatchers
    participant cw as cacheWatcher
    participant http as HTTP Handler
    participant client as API Client

    Note over etcd,client: ⚠️ CACHER BYPASSES RunWithContext!<br/>Uses wait.Until with FIXED 1-second delay

    rect rgb(255, 230, 230)
        Note over etcd,ref: ERROR OCCURS: Corrupt object deleted
        etcd->>ew: DELETE event (corrupt object)
        ew->>ew: transform() → prepareObjs() FAILS
        ew->>ew: sendError(corruptObjectDeletedError)
        ew->>ew: transformErrorToEvent() → watch.Error
        ew->>ref: watch.Error{StatusReasonStoreReadError}
    end

    rect rgb(255, 248, 220)
        Note over ref,cacher: Reflector handles error, returns nil
        ref->>ref: handleAnyWatch() returns error
        ref->>ref: watch() - isExpiredError? NO<br/>IsInternalError? NO (known reason)
        ref->>ref: default: log warning
        ref->>ref: return nil (NOT the error!)
        ref-->>cacher: ListAndWatch() returns nil
    end

    rect rgb(220, 240, 255)
        Note over cacher,wutil: startCaching() returns, wait.Until delays
        cacher->>cacher: startCaching() returns
        Note right of cacher: err == nil, no error logged
        cacher->>wutil: function returns
        wutil->>wutil: sleep(1 * time.Second)
        Note right of wutil: FIXED delay, NO exponential backoff!
        wutil->>cacher: call startCaching() again
    end

    rect rgb(255, 220, 220)
        Note over cacher,cw: terminateAllWatchers() called FIRST
        cacher->>cacher: terminateAllWatchers()
        cacher->>cacher: Lock()
        cacher->>iw: terminateAll(groupResource, stopWatcherLocked)

        loop for each cacheWatcher
            iw->>cacher: stopWatcherLocked(watcher)
            cacher->>cw: watcher.stopLocked()
            cw->>cw: close(c.done)
            cw->>cw: close(c.input)
        end

        cacher->>cacher: Unlock()
    end

    rect rgb(220, 255, 220)
        Note over cw,client: Channel closure cascades to clients
        cw->>cw: process() sees input closed
        cw->>cw: return from process()
        cw->>cw: processInterval() defer executes
        cw->>cw: close(c.result)

        http->>cw: event, ok := <-ResultChan()
        cw-->>http: ok = false (channel closed)
        http->>http: return (ends HTTP response)
        http-->>client: Connection closes
    end

    rect rgb(240, 255, 240)
        Note over cacher,etcd: THEN Cacher re-lists from etcd
        cacher->>ref: reflector.ListAndWatch(stopChannel)
        ref->>etcd: List all objects
        etcd-->>ref: Object list
        ref->>cacher: watchCache.Replace(objects, rv)
        Note right of cacher: Event buffer cleared<br/>listResourceVersion updated
    end

    Note over client: Client sees channel close<br/>NOT watch.Error event!<br/>Must reconnect and rebuild cache
```

---

## 8. Sequence Diagram — Cacher Startup Loop (wait.Until Pattern)

This diagram emphasizes the Cacher's unique retry pattern that differs from normal
Reflector usage.

```mermaid
sequenceDiagram
    autonumber
    participant main as NewCacher()
    participant wutil as wait.Until<br/>goroutine
    participant cacher as Cacher
    participant ref as Reflector
    participant etcd as etcd

    Note over main,etcd: INITIALIZATION

    main->>cacher: Create Cacher
    main->>ref: NewNamedReflector(lw, watchCache, 0)
    Note right of ref: resyncPeriod = 0

    main->>wutil: go wait.Until(func, 1*time.Second, stopCh)
    Note right of wutil: NOT wait.BackoffUntil!<br/>Fixed 1-second delay

    rect rgb(230, 245, 255)
        Note over wutil,etcd: NORMAL OPERATION LOOP

        loop wait.Until loop (repeats on any return)
            wutil->>cacher: startCaching(stopCh)

            cacher->>cacher: terminateAllWatchers()
            Note right of cacher: Called EVERY time,<br/>even on first run!

            cacher->>ref: ListAndWatch(stopChannel)
            Note right of ref: Direct call,<br/>NOT RunWithContext()!

            ref->>etcd: List + Watch

            Note over ref,etcd: Watch runs until error or stop

            alt Watch error occurs
                ref-->>cacher: returns nil (error swallowed)
            else stopCh closed
                ref-->>cacher: returns nil
            end

            cacher-->>wutil: startCaching() returns

            alt stopCh not closed
                wutil->>wutil: sleep(1 second)
                Note right of wutil: Then loops back to startCaching()
            else stopCh closed
                wutil->>wutil: exit goroutine
            end
        end
    end
```

---

## 9. Comparison Diagram — Cacher vs Normal Client Reflector Usage

```mermaid
flowchart TB
    subgraph normal_client["Normal Client (e.g., controller-manager)"]
        direction TB
        nc_run["RunWithContext()"]
        nc_backoff["wait.BackoffUntil<br/>───────────<br/>• Exponential backoff<br/>• 800ms → 30s max"]
        nc_law["ListAndWatchWithContext()"]
        nc_handler["watchErrorHandler()<br/>───────────<br/>• Custom error handling<br/>• Can log, metric, alert"]
        nc_list["list() → store.Replace()"]
        nc_watch["watch() loop"]

        nc_run --> nc_backoff
        nc_backoff --> nc_law
        nc_law --> nc_list
        nc_list --> nc_watch
        nc_watch -->|"error"| nc_handler
        nc_handler -->|"return"| nc_backoff
    end

    subgraph cacher["Cacher (kube-apiserver)"]
        direction TB
        c_until["wait.Until<br/>───────────<br/>• Fixed 1-second delay<br/>• NO backoff!"]
        c_start["startCaching()"]
        c_term["terminateAllWatchers()<br/>───────────<br/>• Called FIRST!<br/>• ALL clients disconnected"]
        c_law["ListAndWatch()<br/>───────────<br/>• Direct call<br/>• NOT RunWithContext!"]
        c_list["list() → watchCache.Replace()"]
        c_watch["watch() loop"]
        c_log["klog.Errorf()<br/>───────────<br/>• Just logs<br/>• No custom handler"]

        c_until --> c_start
        c_start --> c_term
        c_term --> c_law
        c_law --> c_list
        c_list --> c_watch
        c_watch -->|"error"| c_log
        c_log -->|"return nil"| c_until
    end

    style nc_backoff fill:#c8e6c9
    style nc_handler fill:#c8e6c9
    style c_until fill:#ffcdd2
    style c_term fill:#ffcdd2
    style c_log fill:#ffcdd2
```

---

## How to Render These Diagrams

### Option 1: GitHub/GitLab
Simply view this `.md` file in GitHub or GitLab — they render Mermaid natively.

### Option 2: VS Code
Install the "Markdown Preview Mermaid Support" extension.

### Option 3: Online
Copy the Mermaid code blocks to [mermaid.live](https://mermaid.live/)

### Option 4: Command Line
```bash
# Install mermaid-cli
npm install -g @mermaid-js/mermaid-cli

# Generate PNG
mmdc -i etcd3-watcher-uml-diagrams.md -o diagrams.png
```

---

## Key Insights from the Diagrams

1. **Interface Boundaries**: Notice how each major component communicates through well-defined interfaces (`watch.Interface`, `cache.Store`, `cache.ListerWatcher`). This allows components to be tested and replaced independently.

2. **Callback Pattern**: The `watchCache.eventHandler` callback is the key bridge between the storage layer and the distribution layer. It's set during initialization and decouples the two concerns.

3. **Two-Phase Watch**: New watchers first read from `watchCacheInterval` (historical), then switch to receiving from `input` channel (live). This ensures no events are missed.

4. **Fan-Out Architecture**: One etcd watch stream fans out to many `cacheWatcher` instances, each with its own filter. This is why the API server can handle thousands of watch connections efficiently.

5. **Three-Part watchCache**: The watchCache has three distinct components:
   - **Cyclic Buffer**: Stores event history for WATCH requests ("what changed since RV X?")
   - **B-tree Store**: Stores current objects for LIST requests ("what exists now?")
   - **Snapshotter**: Stores COW clones for exact-RV LIST requests ("what existed at RV X?")

   This separation allows each query pattern to be served by an optimized data structure.

6. **Copy-on-Write Snapshots**: The `storeSnapshotter` creates O(1) snapshots via B-tree Clone(). After each event, a COW clone is created—both the original and clone share nodes until one is modified. This enables serving LIST requests at specific resource versions without expensive deep copies. (Requires `ListFromCacheSnapshot` feature gate.)

7. **Cacher Bypasses RunWithContext**: Unlike normal clients that use `RunWithContext()` with exponential backoff, the Cacher directly calls `ListAndWatch()` and manages its own retry loop with `wait.Until`. This means:
   - **Fixed 1-second delay** between retries (no exponential backoff)
   - **No `watchErrorHandler`** is called (errors just logged)
   - **`terminateAllWatchers()` called on EVERY restart** — all clients disconnected before re-listing

8. **terminateAllWatchers Cascade**: When the Cacher resets (for any reason), the termination cascades:
   ```
   terminateAllWatchers() → stopLocked() → close(input) → process() returns
                         → close(result) → HTTP handler returns → client disconnected
   ```
   Clients see their watch channel close but receive NO `watch.Error` event explaining why.

9. **Error Classification Affects Retry**: The `StatusReasonStoreReadError` used for corrupt objects is in the `knownReasons` map, so `IsInternalError()` returns `false`. This prevents the internal retry mechanism from kicking in, causing the watch to end immediately.
