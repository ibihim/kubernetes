# Client-Side Informer Architecture — UML Diagrams

This document contains UML diagrams explaining how Kubernetes controllers watch the
API server using SharedInformers, DeltaFIFO, and Reflector.

---

## 1. Class/Struct Diagram — Component Relationships

This diagram shows how the main client-side components relate to each other.

```mermaid
classDiagram
    direction TB

    %% Interfaces
    class ISharedInformer {
        <<interface>>
        +AddEventHandler() registration
        +GetStore() Store
        +Run(stopCh)
        +HasSynced() bool
    }

    class IStore {
        <<interface>>
        +Add(obj) error
        +Update(obj) error
        +Delete(obj) error
        +List() slice
        +Get(obj) item
        +Replace() error
    }

    class IQueue {
        <<interface>>
        +Pop(process) item
        +Add(obj) error
        +Close()
        +HasSynced() bool
    }

    class IListerWatcher {
        <<interface>>
        +List(options) Object
        +Watch(options) watch.Interface
    }

    class IResourceEventHandler {
        <<interface>>
        +OnAdd(obj, isInitialList)
        +OnUpdate(oldObj, newObj)
        +OnDelete(obj)
    }

    %% Factory layer
    class SharedInformerFactory {
        <<informers/factory.go>>
        -client : kubernetes.Interface
        -namespace : string
        -defaultResync : Duration
        -informers : map[Type]SharedIndexInformer
        -startedInformers : map[Type]bool
        +Start(stopCh)
        +Shutdown()
        +WaitForCacheSync() map
        +InformerFor(obj, newFunc) SharedIndexInformer
        +Core() CoreInterface
        +Apps() AppsInterface
    }

    %% SharedIndexInformer - the main component
    class sharedIndexInformer {
        <<shared_informer.go>>
        -indexer : Indexer
        -controller : Controller
        -processor : sharedProcessor
        -listerWatcher : ListerWatcher
        -objectType : Object
        -resyncCheckPeriod : Duration
        -transform : TransformFunc
        +Run(stopCh)
        +AddEventHandler(handler) registration
        +GetStore() Store
        +GetIndexer() Indexer
        +HasSynced() bool
        +HandleDeltas(obj) error
    }

    %% Controller - ties Reflector and Queue
    class controller {
        <<controller.go>>
        -config : Config
        -reflector : Reflector
        +Run(stopCh)
        +HasSynced() bool
        +processLoop()
    }

    class Config {
        <<controller.go>>
        +Queue : Queue
        +ListerWatcher : ListerWatcher
        +Process : ProcessFunc
        +ObjectType : Object
        +FullResyncPeriod : Duration
        +ShouldResync : func
    }

    %% Reflector - watches API server
    class Reflector {
        <<reflector.go>>
        -listerWatcher : ListerWatcher
        -store : Store
        -expectedType : Type
        -lastSyncResourceVersion : string
        +ListAndWatch(stopCh) error
        +Run(stopCh)
        +LastSyncResourceVersion() string
    }

    %% DeltaFIFO - the key queue implementation
    class DeltaFIFO {
        <<delta_fifo.go>>
        -lock : RWMutex
        -cond : Cond
        -items : map[string]Deltas
        -queue : []string
        -populated : bool
        -initialPopulationCount : int
        -keyFunc : KeyFunc
        -knownObjects : KeyListerGetter
        -closed : bool
        -transformer : TransformFunc
        +Add(obj) error
        +Update(obj) error
        +Delete(obj) error
        +Pop(process) item
        +Replace(list) error
        +Resync() error
        +HasSynced() bool
        -queueActionLocked()
    }

    class Delta {
        <<delta_fifo.go>>
        +Type : DeltaType
        +Object : interface
    }

    class Deltas {
        <<delta_fifo.go>>
        +Oldest() Delta
        +Newest() Delta
    }

    class DeletedFinalStateUnknown {
        <<delta_fifo.go>>
        +Key : string
        +Obj : interface
    }

    %% Indexer/Store - local cache
    class cache {
        <<store.go>>
        -cacheStorage : ThreadSafeStore
        -keyFunc : KeyFunc
        -transformer : TransformFunc
        +Add(obj) error
        +Update(obj) error
        +Delete(obj) error
        +List() slice
        +Get(obj) item
        +GetByKey(key) item
        +Index(name, obj) slice
    }

    class ThreadSafeStore {
        <<thread_safe_store.go>>
        -lock : RWMutex
        -items : map[string]interface
        -index : Indices
        +Add(key, obj)
        +Update(key, obj)
        +Delete(key)
        +Get(key) item
        +List() slice
    }

    %% Event distribution
    class sharedProcessor {
        <<shared_informer.go>>
        -listenersStarted : bool
        -listeners : map[processorListener]bool
        +run(ctx)
        +addListener(listener) registration
        +distribute(obj, sync)
        +shouldResync() bool
    }

    class processorListener {
        <<shared_informer.go>>
        -nextCh : channel
        -addCh : channel
        -handler : ResourceEventHandler
        -pendingNotifications : RingGrowing
        -resyncPeriod : Duration
        +add(notification)
        +pop()
        +run()
        +HasSynced() bool
    }

    class addNotification {
        +newObj : interface
        +isInInitialList : bool
    }

    class updateNotification {
        +oldObj : interface
        +newObj : interface
    }

    class deleteNotification {
        +oldObj : interface
    }

    %% Relationships

    %% Factory owns informers
    SharedInformerFactory *-- sharedIndexInformer : manages many

    %% SharedIndexInformer composition
    sharedIndexInformer ..|> ISharedInformer : implements
    sharedIndexInformer *-- controller : owns
    sharedIndexInformer *-- cache : indexer
    sharedIndexInformer *-- sharedProcessor : owns
    sharedIndexInformer --> IListerWatcher : uses

    %% Controller composition
    controller *-- Config : owns
    controller *-- Reflector : owns

    %% Config references
    Config --> DeltaFIFO : Queue
    Config --> IListerWatcher : ListerWatcher

    %% Reflector connections
    Reflector --> IListerWatcher : calls List/Watch
    Reflector --> DeltaFIFO : store (calls Add/Update/Delete)

    %% DeltaFIFO
    DeltaFIFO ..|> IQueue : implements
    DeltaFIFO ..|> IStore : implements
    DeltaFIFO *-- Deltas : items map value
    DeltaFIFO --> cache : knownObjects
    Deltas *-- Delta : contains many
    Delta --> DeletedFinalStateUnknown : may contain

    %% Cache/Indexer
    cache ..|> IStore : implements
    cache *-- ThreadSafeStore : owns

    %% Event distribution
    sharedProcessor o-- processorListener : manages many
    processorListener --> IResourceEventHandler : calls handler
    processorListener --> addNotification : processes
    processorListener --> updateNotification : processes
    processorListener --> deleteNotification : processes

    %% sharedIndexInformer also implements ResourceEventHandler
    sharedIndexInformer ..|> IResourceEventHandler : implements (OnAdd/OnUpdate/OnDelete)
```

---

## 2. Sequence Diagram — Event Flow from API Server to Handler

This diagram shows the runtime flow when an object changes in the API server.

```mermaid
sequenceDiagram
    autonumber
    participant apiserver as kube-apiserver
    participant lw as ListerWatcher
    participant ref as Reflector
    participant fifo as DeltaFIFO
    participant ctrl as controller.processLoop
    participant si as sharedIndexInformer
    participant idx as Indexer (cache)
    participant proc as sharedProcessor
    participant pl as processorListener
    participant handler as ResourceEventHandler

    Note over apiserver,handler: SETUP PHASE (happens once at startup)

    si->>fifo: NewDeltaFIFO(keyFunc, indexer)
    Note right of fifo: knownObjects = indexer

    si->>ctrl: New(Config{Queue: fifo, ...})
    ctrl->>ref: NewReflector(lw, fifo)
    Note right of ref: store = fifo

    si->>proc: processor.run(ctx)
    ctrl->>ref: go reflector.Run()
    ctrl->>ctrl: go processLoop()

    ref->>lw: List(options)
    lw->>apiserver: GET /api/v1/pods
    apiserver-->>lw: PodList
    lw-->>ref: PodList

    ref->>fifo: Replace(items, resourceVersion)
    Note right of fifo: Populates items map with Replaced deltas

    ref->>lw: Watch(options)
    lw->>apiserver: GET /api/v1/pods?watch=true
    apiserver-->>lw: watch.Interface
    lw-->>ref: watch.Interface

    Note over apiserver,handler: EVENT FLOW (repeats for each event)

    apiserver->>lw: watch.Event{Type: MODIFIED, Object: Pod}
    lw-->>ref: watch.Event

    rect rgb(240, 248, 255)
        Note over ref,fifo: Reflector receives event and updates DeltaFIFO
        alt event.Type == ADDED
            ref->>fifo: Add(pod)
        else event.Type == MODIFIED
            ref->>fifo: Update(pod)
        else event.Type == DELETED
            ref->>fifo: Delete(pod)
        end

        fifo->>fifo: queueActionLocked(Updated, pod)
        Note right of fifo: 1. Get key via keyFunc<br/>2. Append Delta to items[key]<br/>3. Add key to queue if new<br/>4. cond.Broadcast()
    end

    rect rgb(255, 248, 240)
        Note over ctrl,si: processLoop pops and processes Deltas
        ctrl->>fifo: Pop(processFunc)
        fifo->>fifo: Wait on cond if queue empty
        fifo-->>ctrl: Deltas{Delta{Updated, pod}}

        ctrl->>si: HandleDeltas(deltas)
        si->>si: processDeltas(handler, indexer, deltas)
    end

    rect rgb(240, 255, 240)
        Note over si,idx: Update local cache and notify
        loop for each delta in deltas
            alt delta.Type == Added/Updated/Replaced/Sync
                si->>idx: Get(obj) to check if exists
                alt exists
                    si->>idx: Update(obj)
                    si->>si: OnUpdate(old, new)
                else not exists
                    si->>idx: Add(obj)
                    si->>si: OnAdd(obj, isInitialList)
                end
            else delta.Type == Deleted
                si->>idx: Delete(obj)
                si->>si: OnDelete(obj)
            end
        end
    end

    rect rgb(255, 240, 255)
        Note over si,proc: SharedIndexInformer distributes to processor
        si->>proc: distribute(updateNotification{old, new}, isSync)
        proc->>proc: listenersLock.RLock()

        loop for each listener
            proc->>pl: add(notification)
            pl->>pl: addCh <- notification
        end
    end

    rect rgb(240, 255, 255)
        Note over pl,handler: processorListener delivers to handler
        pl->>pl: pop() reads from addCh
        pl->>pl: pendingNotifications or nextCh
        pl->>pl: run() reads from nextCh

        alt notification is addNotification
            pl->>handler: OnAdd(obj, isInInitialList)
        else notification is updateNotification
            pl->>handler: OnUpdate(oldObj, newObj)
        else notification is deleteNotification
            pl->>handler: OnDelete(obj)
        end
    end
```

---

## 3. DeltaFIFO Internal Structure — Data Flow Diagram

This diagram shows how DeltaFIFO maintains its internal state.

```mermaid
flowchart TB
    subgraph deltafifo["DeltaFIFO Internal State"]
        direction TB

        subgraph queue_slice["queue []string (FIFO order)"]
            q0["[0] 'default/pod-a'"]
            q1["[1] 'kube-system/pod-b'"]
            q2["[2] 'default/pod-c'"]
        end

        subgraph items_map["items map[string]Deltas"]
            direction LR

            subgraph deltas_a["'default/pod-a'"]
                da1["Delta{Added, Pod}"]
                da2["Delta{Updated, Pod}"]
            end

            subgraph deltas_b["'kube-system/pod-b'"]
                db1["Delta{Replaced, Pod}"]
            end

            subgraph deltas_c["'default/pod-c'"]
                dc1["Delta{Deleted, Pod}"]
            end
        end

        subgraph known["knownObjects (Indexer)"]
            ka["'default/pod-a' -> Pod"]
            kb["'kube-system/pod-b' -> Pod"]
            kd["'default/pod-d' -> Pod"]
        end
    end

    subgraph operations["Operations"]
        direction TB

        add["Add(pod):<br/>1. key = keyFunc(pod)<br/>2. items[key] = append(items[key], Delta{Added, pod})<br/>3. if key not in queue: queue = append(queue, key)<br/>4. cond.Broadcast()"]

        pop["Pop(process):<br/>1. Wait on cond while queue empty<br/>2. key = queue[0]; queue = queue[1:]<br/>3. deltas = items[key]; delete(items, key)<br/>4. process(deltas)<br/>5. return deltas"]

        replace["Replace(list):<br/>1. For each obj in list: queueAction(Replaced, obj)<br/>2. For keys in items not in list: queue Delete<br/>3. For keys in knownObjects not in list: queue Delete<br/>4. Deletions use DeletedFinalStateUnknown"]

        resync["Resync():<br/>1. For each key in knownObjects.ListKeys()<br/>2. If key not in items: queueAction(Sync, obj)"]
    end

    add --> items_map
    pop --> queue_slice
    replace --> items_map
    replace --> known
    resync --> known

    style deltas_a fill:#e1f5fe
    style deltas_b fill:#fff3e0
    style deltas_c fill:#ffebee
```

---

## 4. Sequence Diagram — DeltaFIFO Accumulation

This diagram shows how multiple events for the same object are accumulated.

```mermaid
sequenceDiagram
    autonumber
    participant ref as Reflector
    participant fifo as DeltaFIFO
    participant ctrl as processLoop

    Note over ref,ctrl: Scenario: Multiple rapid updates to same Pod

    ref->>fifo: Add(pod-v1)
    Note right of fifo: items["default/my-pod"] = [{Added, pod-v1}]<br/>queue = ["default/my-pod"]

    ref->>fifo: Update(pod-v2)
    Note right of fifo: items["default/my-pod"] = [{Added, pod-v1}, {Updated, pod-v2}]<br/>queue unchanged (key already present)

    ref->>fifo: Update(pod-v3)
    Note right of fifo: items["default/my-pod"] = [{Added, pod-v1}, {Updated, pod-v2}, {Updated, pod-v3}]

    Note over ctrl: processLoop calls Pop()

    ctrl->>fifo: Pop(process)
    fifo-->>ctrl: Deltas[{Added, pod-v1}, {Updated, pod-v2}, {Updated, pod-v3}]
    Note right of ctrl: All changes delivered together!<br/>Handler sees complete history

    ctrl->>ctrl: process(deltas)
    Note right of ctrl: for each delta:<br/>  - delta[0]: Add to indexer, OnAdd<br/>  - delta[1]: Update indexer, OnUpdate(v1, v2)<br/>  - delta[2]: Update indexer, OnUpdate(v2, v3)

    Note over ref,ctrl: Scenario: Delete followed by recreate

    ref->>fifo: Delete(pod)
    Note right of fifo: items["default/my-pod"] = [{Deleted, pod}]

    ref->>fifo: Add(new-pod-same-name)
    Note right of fifo: items["default/my-pod"] = [{Deleted, pod}, {Added, new-pod}]

    ctrl->>fifo: Pop(process)
    fifo-->>ctrl: Deltas[{Deleted, old-pod}, {Added, new-pod}]
    Note right of ctrl: Handler sees delete then add<br/>Different UIDs handled correctly

    Note over ref,ctrl: Scenario: Consecutive deletes (deduplication)

    ref->>fifo: Delete(pod)
    Note right of fifo: items["default/my-pod"] = [{Deleted, pod}]

    ref->>fifo: Delete(pod)
    Note right of fifo: dedupDeltas() keeps only one Delete<br/>items["default/my-pod"] = [{Deleted, pod}]
```

---

## 5. Component Diagram — High-Level Client Architecture

```mermaid
flowchart TB
    subgraph apiserver["kube-apiserver"]
        api[(API Server)]
    end

    subgraph client_app["Controller / Operator Application"]

        subgraph factory_layer["Factory Layer"]
            factory["SharedInformerFactory<br/>• Manages informer lifecycle<br/>• Deduplicates by type<br/>• Coordinates start/stop"]
        end

        subgraph informer_layer["Informer Layer (per resource type)"]

            subgraph informer_box["sharedIndexInformer"]
                direction TB

                subgraph sync_layer["Synchronization"]
                    lw["ListerWatcher<br/>(REST client wrapper)"]
                    reflector["Reflector<br/>• List + Watch loop<br/>• ResourceVersion tracking"]
                    fifo["DeltaFIFO<br/>• Accumulates deltas per key<br/>• FIFO processing order<br/>• Deduplication"]
                end

                subgraph cache_layer["Local Cache"]
                    indexer["Indexer<br/>• ThreadSafeStore<br/>• Secondary indices<br/>• Namespace index"]
                end

                subgraph notify_layer["Event Distribution"]
                    processor["sharedProcessor<br/>• Fan-out to listeners<br/>• Resync coordination"]
                end
            end
        end

        subgraph handler_layer["Event Handlers"]
            listener1["processorListener 1<br/>• Unbounded buffer<br/>• Async delivery"]
            listener2["processorListener 2"]
            listenerN["processorListener N"]
        end

        subgraph workqueue_layer["Work Queues"]
            wq1["workqueue 1<br/>• Rate limiting<br/>• Deduplication"]
            wq2["workqueue 2"]
        end

        subgraph reconcile_layer["Reconciliation"]
            reconciler1["Reconciler 1<br/>• Business logic<br/>• Idempotent"]
            reconciler2["Reconciler 2"]
        end
    end

    api <-->|"List + Watch<br/>(HTTP/2)"| lw
    lw --> reflector
    reflector -->|"Add/Update/Delete"| fifo
    fifo -->|"Pop() returns Deltas"| indexer
    indexer -->|"callback"| processor
    processor --> listener1
    processor --> listener2
    processor --> listenerN
    listener1 -->|"OnAdd/OnUpdate/OnDelete"| wq1
    listener2 -->|"OnAdd/OnUpdate/OnDelete"| wq2
    wq1 --> reconciler1
    wq2 --> reconciler2

    factory --> informer_box

    style fifo fill:#e1f5fe
    style indexer fill:#fff3e0
    style processor fill:#f3e5f5
```

---

## 6. State Diagram — DeltaFIFO Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Empty: NewDeltaFIFO()

    Empty --> HasItems: Add/Update/Delete/Replace
    note right of Empty
        queue = []
        items = {}
        populated = false
    end note

    HasItems --> HasItems: Add/Update/Delete
    note right of HasItems
        queue has entries
        items has Deltas
        cond.Broadcast() on each add
    end note

    HasItems --> Empty: Pop() drains last item

    HasItems --> Processing: Pop() called
    Processing --> HasItems: process() completes
    Processing --> Empty: process() completes (was last item)

    note right of Processing
        Pop() removes from queue first
        Then calls process() under lock
        If process() fails, item is lost
        (must re-add with AddIfNotPresent)
    end note

    Empty --> WaitingForItems: Pop() called on empty
    WaitingForItems --> Processing: cond.Wait() returns
    note right of WaitingForItems
        Blocks on cond.Wait()
        Until Add/Update/Delete/Replace
        or Close()
    end note

    WaitingForItems --> Closed: Close() called
    HasItems --> Closed: Close() called
    Empty --> Closed: Close() called

    Closed --> [*]
    note right of Closed
        closed = true
        cond.Broadcast()
        Pop() returns ErrFIFOClosed
    end note
```

---

## 7. Sequence Diagram — Replace (Relist) Operation

This shows what happens during a relist (after watch disconnect).

```mermaid
sequenceDiagram
    autonumber
    participant ref as Reflector
    participant fifo as DeltaFIFO
    participant idx as Indexer (knownObjects)

    Note over ref,idx: Watch disconnected, Reflector does relist

    ref->>ref: List() returns [pod-a, pod-b, pod-c]

    ref->>fifo: Replace([pod-a, pod-b, pod-c], rv)

    Note over fifo: Step 1: Add Replaced deltas for all items in list

    fifo->>fifo: queueAction(Replaced, pod-a)
    fifo->>fifo: queueAction(Replaced, pod-b)
    fifo->>fifo: queueAction(Replaced, pod-c)

    Note over fifo: Step 2: Check items map for keys NOT in new list

    fifo->>fifo: items has "default/pod-x" not in list
    fifo->>fifo: queueAction(Deleted, DeletedFinalStateUnknown{key, lastObj})
    Note right of fifo: Uses newest object from existing Deltas

    Note over fifo: Step 3: Check knownObjects for keys NOT in list

    fifo->>idx: ListKeys()
    idx-->>fifo: ["default/pod-a", "default/pod-b", "default/pod-d"]

    fifo->>fifo: "default/pod-d" not in list and not in items
    fifo->>idx: GetByKey("default/pod-d")
    idx-->>fifo: pod-d object

    fifo->>fifo: queueAction(Deleted, DeletedFinalStateUnknown{"default/pod-d", pod-d})
    Note right of fifo: Synthetic delete for object<br/>that disappeared during disconnect

    Note over fifo: Final state after Replace

    Note right of fifo: queue = ["default/pod-a", "default/pod-b", "default/pod-c", "default/pod-x", "default/pod-d"]<br/><br/>items["default/pod-a"] = [{Replaced, pod-a}]<br/>items["default/pod-b"] = [{Replaced, pod-b}]<br/>items["default/pod-c"] = [{Replaced, pod-c}]<br/>items["default/pod-x"] = [{Deleted, DeletedFinalStateUnknown}]<br/>items["default/pod-d"] = [{Deleted, DeletedFinalStateUnknown}]
```

---

## 8. Delta Types — When Each Is Used

```mermaid
flowchart LR
    subgraph delta_types["DeltaType Usage"]
        direction TB

        added["Added<br/>─────────<br/>• Reflector receives ADDED watch event<br/>• New object appears in cluster"]

        updated["Updated<br/>─────────<br/>• Reflector receives MODIFIED watch event<br/>• Existing object changed"]

        deleted["Deleted<br/>─────────<br/>• Reflector receives DELETED watch event<br/>• Object removed from cluster<br/>• Or synthetic delete during Replace"]

        replaced["Replaced<br/>─────────<br/>• During Replace() after relist<br/>• Object exists in new list<br/>• May or may not have changed<br/>• Only if EmitDeltaTypeReplaced=true"]

        sync["Sync<br/>─────────<br/>• During Resync() for periodic resync<br/>• Object unchanged, just re-notifying<br/>• Or Replace() if EmitDeltaTypeReplaced=false"]
    end

    subgraph handler_behavior["Handler Behavior"]
        direction TB

        add_behavior["OnAdd()<br/>─────────<br/>• Object not in local cache<br/>• For: Added, Replaced, Sync (if new)"]

        update_behavior["OnUpdate()<br/>─────────<br/>• Object exists in local cache<br/>• For: Updated, Replaced, Sync (if exists)<br/>• isSync=true if RV unchanged"]

        delete_behavior["OnDelete()<br/>─────────<br/>• Remove from local cache<br/>• For: Deleted<br/>• May receive DeletedFinalStateUnknown"]
    end

    added --> add_behavior
    updated --> update_behavior
    deleted --> delete_behavior
    replaced --> add_behavior
    replaced --> update_behavior
    sync --> add_behavior
    sync --> update_behavior
```

---

## 9. processorListener — Two-Goroutine Architecture

```mermaid
flowchart LR
    subgraph producer["Producer (sharedProcessor.distribute)"]
        dist["distribute()<br/>calls listener.add()"]
    end

    subgraph listener["processorListener"]
        direction TB

        addCh["addCh<br/>(unbuffered channel)"]

        subgraph pop_goroutine["pop() goroutine"]
            pop_select["select {<br/>  case nextCh <- notification:<br/>  case notification := <-addCh:<br/>}"]
            ring["pendingNotifications<br/>(RingGrowing buffer)"]
        end

        nextCh["nextCh<br/>(unbuffered channel)"]

        subgraph run_goroutine["run() goroutine"]
            run_loop["for notification := range nextCh {<br/>  handler.OnAdd/OnUpdate/OnDelete<br/>}"]
        end
    end

    subgraph consumer["Consumer (ResourceEventHandler)"]
        handler["OnAdd()<br/>OnUpdate()<br/>OnDelete()"]
    end

    dist -->|"add(notification)"| addCh
    addCh --> pop_select
    pop_select <-->|"buffer if blocked"| ring
    pop_select --> nextCh
    nextCh --> run_loop
    run_loop --> handler

    style ring fill:#fff3e0
```

**Why two goroutines?**
- `pop()` ensures the producer (sharedProcessor) never blocks
- `run()` can take as long as needed for handler execution
- `pendingNotifications` ring buffer grows unboundedly if handler is slow
- This prevents slow handlers from blocking other listeners

---

## 10. Sequence Diagram — Watch Reconnection After Server-Side Channel Close

This diagram shows what happens on the **client side** when the API server terminates the watch connection (e.g., due to a corrupt object error in the server-side Cacher).

```mermaid
sequenceDiagram
    autonumber
    participant apiserver as kube-apiserver
    participant lw as ListerWatcher
    participant ref as Reflector
    participant fifo as DeltaFIFO
    participant idx as Indexer (cache)
    participant handler as ResourceEventHandler

    Note over apiserver,handler: NORMAL OPERATION — Watch is active

    ref->>lw: Watch(opts{ResourceVersion: "12345"})
    lw->>apiserver: GET /api/v1/pods?watch=true&rv=12345
    apiserver-->>lw: watch.Interface (HTTP/2 stream)

    loop Watch events flowing normally
        apiserver->>lw: watch.Event{MODIFIED, pod}
        lw-->>ref: event
        ref->>fifo: Update(pod)
    end

    Note over apiserver,handler: ⚡ SERVER-SIDE ERROR — Cacher terminates all watchers

    rect rgb(255, 220, 220)
        Note over apiserver: Server encounters corrupt object<br/>(KMS decryption error, encoding failure, etc.)
        apiserver->>apiserver: Cacher calls terminateAllWatchers()
        apiserver->>apiserver: Each cache_watcher.stopLocked()
        apiserver->>apiserver: close(input) → process() exits → close(result)
        apiserver-->>lw: watch.ResultChan() closes
        lw-->>ref: channel closed (for-range loop ends)
    end

    Note over ref,handler: REFLECTOR watch() LOOP — Does NOT return immediately

    rect rgb(255, 248, 220)
        Note over ref: watch() is called inside a for-loop<br/>Channel close breaks inner loop, outer loop continues

        ref->>ref: watchHandler() returns (channel closed)
        ref->>ref: Still inside watch() for-loop
        ref->>ref: lastSyncResourceVersion = "12345" (preserved)

        Note over ref: Try to resume watch from lastSyncResourceVersion
        ref->>lw: Watch(opts{ResourceVersion: "12345"})
        lw->>apiserver: GET /api/v1/pods?watch=true&rv=12345
    end

    Note over apiserver,handler: SERVER RESPONSE — ResourceVersion too old

    rect rgb(255, 230, 230)
        apiserver-->>lw: HTTP 410 Gone<br/>"resourceVersion too old"
        lw-->>ref: watch error

        ref->>ref: isExpiredError() = true
        ref->>ref: watch() returns nil
        Note right of ref: nil return signals: need fresh LIST
    end

    Note over ref,handler: ListAndWatch() HANDLES watch() RETURN

    rect rgb(220, 255, 220)
        ref->>ref: watch() returned nil
        ref->>ref: ListAndWatch() loops back to top

        Note over ref: ★ FRESH LIST — Cache rebuild starts
        ref->>lw: List(options)
        lw->>apiserver: GET /api/v1/pods
        apiserver-->>lw: PodList (rv: "99999")
        lw-->>ref: PodList

        ref->>ref: lastSyncResourceVersion = "99999"
    end

    Note over fifo,idx: CACHE REPLACEMENT

    rect rgb(220, 240, 255)
        ref->>fifo: Replace(items, "99999")

        Note over fifo: Replace() process:
        fifo->>fifo: Queue Replaced delta for each item in list
        fifo->>fifo: Check knownObjects for items NOT in list
        fifo->>fifo: Queue DeletedFinalStateUnknown for missing items

        Note over fifo,idx: processLoop delivers updates
        fifo-->>idx: Update/Add/Delete operations
        idx->>handler: OnAdd/OnUpdate/OnDelete callbacks
    end

    Note over ref,handler: NEW WATCH ESTABLISHED

    ref->>lw: Watch(opts{ResourceVersion: "99999"})
    lw->>apiserver: GET /api/v1/pods?watch=true&rv=99999
    apiserver-->>lw: New watch.Interface

    Note over apiserver,handler: Normal operation resumes
```

### Key Points About Client-Side Reconnection

| Step | What Happens | Why It Matters |
|------|--------------|----------------|
| 1. Channel closes | `watch.ResultChan()` closes, breaking `for-range` in `watchHandler()` | Does NOT immediately trigger cache rebuild |
| 2. watch() loops | Tries to resume from `lastSyncResourceVersion` | Client hopes to continue where it left off |
| 3. "too old" error | Server's `watchCache` was replaced, old RV is gone | This triggers the relist |
| 4. watch() returns nil | Signals to `ListAndWatch()`: retry needed | Different from error return |
| 5. Fresh LIST | Gets current state from server | Cache fully rebuilt |
| 6. Replace() | DeltaFIFO reconciles new list with local cache | `DeletedFinalStateUnknown` for vanished objects |

---

## 11. Flowchart — Reflector watch() Return Behavior

This diagram clarifies when `watch()` returns vs loops internally.

```mermaid
flowchart TB
    start["watch() called"] --> create_watch["Create watch with lastSyncResourceVersion"]

    create_watch --> watch_result{Watch creation<br/>succeeded?}

    watch_result -->|Error| handle_err{Error type?}

    handle_err -->|"isExpiredError()"| return_nil["return nil<br/>(signals: do fresh LIST)"]
    handle_err -->|"Other error"| return_err["return error<br/>(ListAndWatch will retry)"]

    watch_result -->|Success| watch_loop["Enter watchHandler()<br/>for event := range w.ResultChan()"]

    watch_loop --> event{Event received?}

    event -->|Yes| process_event["Process event<br/>Add/Update/Delete to store"]
    process_event --> update_rv["Update lastSyncResourceVersion"]
    update_rv --> watch_loop

    event -->|"Channel closed"| watchhandler_returns["watchHandler() returns"]

    watchhandler_returns --> outer_loop["Back to watch() outer loop<br/>⚠️ Does NOT return yet"]

    outer_loop --> create_watch

    subgraph "Return conditions"
        return_nil
        return_err
    end

    style return_nil fill:#d4edda
    style return_err fill:#f8d7da
    style outer_loop fill:#fff3cd
```

### ⚠️ Critical Understanding

The `watch()` function in `reflector.go` has this structure:

```go
func (r *Reflector) watch(options metav1.ListOptions, ...) error {
    for {  // ← OUTER LOOP - keeps trying
        w, err := r.listerWatcher.Watch(options)
        if err != nil {
            if isExpiredError(err) {
                return nil  // ← Only returns here
            }
            // ... other error handling
            return err
        }

        err = watchHandler(w, ...)  // ← Blocks until channel closes
        // Channel close: watchHandler returns, loop continues
        // ↓ Goes back to top of for-loop, tries another Watch()
    }
}
```

The channel closing does NOT cause `watch()` to return. It just causes the inner `watchHandler()` to return, and the outer loop tries again.

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
mmdc -i client-side-informer-uml-diagrams.md -o diagrams.png
```

---

## Key Insights from the Diagrams

1. **DeltaFIFO as Accumulator**: Unlike a simple queue, DeltaFIFO accumulates ALL changes to an object between Pop() calls. If a Pod is created then updated 5 times before the handler processes it, the handler sees all 6 deltas in one Pop().

2. **DeletedFinalStateUnknown**: When a watch disconnects and objects disappear during relist, DeltaFIFO synthesizes delete events. The object in these events may be stale (the last known state), wrapped in `DeletedFinalStateUnknown` to signal handlers should handle it carefully.

3. **knownObjects Bridge**: DeltaFIFO's `knownObjects` reference to the Indexer is crucial for detecting deletions during Replace(). Without it, objects deleted during a watch disconnect would be silently lost.

4. **Two-Phase Processing**: The controller's `processLoop` first updates the local cache (Indexer), THEN notifies handlers. This ensures handlers always see consistent cache state when they query it.

5. **Fan-Out with Backpressure**: Each `processorListener` has its own unbounded buffer (`pendingNotifications`). A slow handler only affects its own memory usage, not other handlers. But beware: a consistently slow handler will eventually OOM the process.

6. **Sync vs Replaced**: The distinction matters for handlers that want to differentiate "object changed" from "we're just resyncing." Check `isSync` in `OnUpdate()` to skip unnecessary work during resync.

7. **Channel Close ≠ Immediate Relist**: When the server closes a watch channel, the client's `watchHandler()` returns, but `watch()` does NOT return. It loops back and tries to resume from `lastSyncResourceVersion`. Only when the server responds with "too old" does `watch()` return `nil`, triggering a fresh LIST.

8. **The "Too Old" Error is the Trigger**: The critical moment that causes cache rebuild is receiving `HTTP 410 Gone` (resource version too old) from the server, NOT the channel closing. The client optimistically tries to resume first.

9. **Server-Side Cache Reset Causes Client-Side Cache Rebuild**: When the server's Cacher calls `terminateAllWatchers()` and does a fresh `ListAndWatch()`, its `watchCache` gets new resource versions. Old client watches become invalid, forcing ALL connected clients to relist and rebuild their caches.

10. **Cascade of Rebuilds**: A single corrupt object error on the server triggers: (1) server cache reset, (2) all client watch channels close, (3) all clients get "too old" error, (4) all clients do fresh LIST, (5) all client caches rebuild. This is a cluster-wide impact for that resource type.
