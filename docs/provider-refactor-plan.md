# Provider Refactor Plan: Transactional Execution Model

## Executive Summary

This refactor fundamentally reimagines the provider interface to make transactionality the core primitive. Instead of separate history and queue operations that can fail independently, we move to a unified, atomic fetch-update model that guarantees consistency.

## New Provider Interface

```rust
/// The new unified provider interface - all operations are transactional
#[async_trait]
pub trait RuntimeProvider: Send + Sync {
    
    // ===== Orchestrator Operations =====
    
    /// Fetch next batch of work for an orchestration instance
    /// Returns lock token, instance metadata, history, and pending messages
    async fn fetch_orchestration_work(&self) -> Option<OrchestrationWorkItem>;
    
    /// Atomically update orchestration state
    /// - Appends to history
    /// - Enqueues worker/timer messages  
    /// - Acks processed orchestrator messages
    async fn update_orchestration(
        &self, 
        lock_token: String,
        history_delta: Vec<Event>,
        worker_messages: Vec<WorkerMessage>,
        timer_messages: Vec<TimerMessage>,
    ) -> Result<(), UpdateError>;
    
    /// Abandon orchestration work, making it visible again after delay
    async fn abandon_orchestration(
        &self,
        lock_token: String, 
        retry_delay_ms: u64
    ) -> Result<(), AbandonError>;
    
    // ===== Worker Operations =====
    
    /// Fetch next activity to execute
    async fn fetch_worker_item(&self) -> Option<WorkerWorkItem>;
    
    /// Ack worker item and enqueue completion to orchestrator queue
    async fn update_worker(
        &self,
        lock_token: String,
        orchestrator_message: OrchestratorMessage,
    ) -> Result<(), UpdateError>;
    
    // ===== Timer Operations =====
    
    /// Fetch next timer to schedule
    async fn fetch_timer_item(&self) -> Option<TimerWorkItem>;
    
    /// Ack timer (fire-and-forget after scheduling internally)
    async fn ack_timer(&self, lock_token: String) -> Result<(), AckError>;
}

// Work item types
pub struct OrchestrationWorkItem {
    pub lock_token: String,
    pub instance_id: String,
    pub orchestration_name: String,
    pub execution_id: u64,
    pub version: String,
    pub history: Vec<Event>,
    pub messages: Vec<OrchestratorMessage>,  // Completions, starts, events
}

pub struct WorkerWorkItem {
    pub lock_token: String,
    pub activity_name: String,
    pub input: String,
    pub instance_id: String,
    pub execution_id: u64,
    pub activity_id: u64,
}

pub struct TimerWorkItem {
    pub lock_token: String,
    pub instance_id: String,
    pub execution_id: u64,
    pub timer_id: u64,
    pub fire_at_ms: u64,
}
```

## Refactor Phases

### Phase 1: Define New Provider Interface (Week 1)

**Tasks:**
1. Create new `RuntimeProvider` trait in `src/providers/runtime_provider.rs`
2. Define all message types and work items
3. Define error types with clear semantics
4. Create comprehensive documentation

**Key Files:**
- `src/providers/runtime_provider.rs` (NEW)
- `src/providers/messages.rs` (NEW)
- `src/providers/errors.rs` (NEW)

### Phase 2: Implement Provider for Testing (Week 1-2)

**Tasks:**
1. Create in-memory implementation for unit tests
2. Ensure full transactional semantics (all-or-nothing)
3. Add failure injection capabilities
4. Comprehensive test coverage

**Key Files:**
- `src/providers/transactional_memory.rs` (NEW)
- `tests/provider_tests.rs` (NEW)

```rust
pub struct TransactionalMemoryProvider {
    orchestrator_queue: Arc<Mutex<Vec<OrchestratorMessage>>>,
    worker_queue: Arc<Mutex<Vec<WorkerMessage>>>,
    timer_queue: Arc<Mutex<Vec<TimerMessage>>>,
    instances: Arc<Mutex<HashMap<String, InstanceData>>>,
    locks: Arc<Mutex<HashMap<String, LockInfo>>>,
}
```

### Phase 3: Refactor Execution Model (Week 2-3)

**Current Flow (BROKEN):**
```
1. Dequeue message
2. Load history separately  
3. Execute turn
4. Append history (can succeed)
5. Dispatch actions (can fail silently!)
6. Ack message
```

**New Flow (TRANSACTIONAL):**
```
1. fetch_orchestration_work() - Get everything atomically
2. Execute turn(s) in memory
3. update_orchestration() - Commit everything atomically
   OR
   abandon_orchestration() - On failure
```

**Key Changes:**

```rust
// OLD - execution.rs
pub async fn run_single_execution(
    self: Arc<Self>,
    instance: &str,
    orchestration_name: &str,
    initial_history: Vec<Event>,
    completion_messages: Vec<OrchestratorMsg>,
) -> (Vec<Event>, Result<String, String>)

// NEW - execution.rs  
pub async fn run_orchestration_work(
    self: Arc<Self>,
    provider: Arc<dyn RuntimeProvider>,
) -> Result<(), ExecutionError> {
    // Fetch work
    let Some(work) = provider.fetch_orchestration_work().await else {
        return Ok(()); // No work available
    };
    
    // Execute all possible turns
    let mut history = work.history;
    let mut messages = work.messages;
    let mut worker_msgs = Vec::new();
    let mut timer_msgs = Vec::new();
    
    loop {
        let (new_history, new_workers, new_timers, continue) = 
            self.execute_turn(&history, &messages, &work)?;
        
        history = new_history;
        worker_msgs.extend(new_workers);
        timer_msgs.extend(new_timers);
        
        if !continue { break; }
        
        // Process any synchronous completions
        messages = self.get_synchronous_completions(&history);
    }
    
    // Commit all changes atomically
    provider.update_orchestration(
        work.lock_token,
        history[work.history.len()..].to_vec(), // Delta only
        worker_msgs,
        timer_msgs,
    ).await?;
    
    Ok(())
}
```

**Files to Modify:**
- `src/runtime/execution.rs` - Complete rewrite
- `src/runtime/orchestration_turn.rs` - Simplify (no persistence)
- `src/runtime/dispatch.rs` - Remove (logic moves to turn)

### Phase 4: Refactor Dispatcher Architecture (Week 3)

**Current:** Three separate dispatchers with complex queue management

**New:** Simple fetch-execute-update loops

```rust
// Orchestrator Dispatcher
async fn run_orchestrator_dispatcher(provider: Arc<dyn RuntimeProvider>) {
    loop {
        match runtime.run_orchestration_work(provider.clone()).await {
            Ok(_) => continue,
            Err(e) if e.is_transient() => {
                // Transient error - work will retry
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => {
                // Fatal error - log and continue with next item
                error!("Orchestration execution failed: {}", e);
            }
        }
    }
}

// Worker Dispatcher  
async fn run_worker_dispatcher(
    provider: Arc<dyn RuntimeProvider>,
    activities: Arc<ActivityRegistry>
) {
    loop {
        let Some(work) = provider.fetch_worker_item().await else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        
        // Execute activity
        let result = activities.execute(&work.activity_name, &work.input).await;
        
        // Update atomically
        let message = match result {
            Ok(output) => OrchestratorMessage::ActivityCompleted { 
                instance_id: work.instance_id,
                execution_id: work.execution_id,
                activity_id: work.activity_id,
                output,
            },
            Err(error) => OrchestratorMessage::ActivityFailed {
                instance_id: work.instance_id,
                execution_id: work.execution_id,
                activity_id: work.activity_id,
                error,
            }
        };
        
        provider.update_worker(work.lock_token, message).await?;
    }
}
```

**Files to Modify:**
- `src/runtime/mod.rs` - Simplify dispatcher spawning
- Remove complex queue management code

### Phase 5: Implement FileSystem Provider (Week 4)

```rust
pub struct TransactionalFsProvider {
    root: PathBuf,
    lock_manager: FileLockManager,
}

impl RuntimeProvider for TransactionalFsProvider {
    async fn fetch_orchestration_work(&self) -> Option<OrchestrationWorkItem> {
        // 1. Scan orchestrator queue directory for next message
        // 2. Acquire exclusive lock on instance
        // 3. Read instance history
        // 4. Read all pending messages for instance
        // 5. Return work item with lock token
    }
    
    async fn update_orchestration(&self, ...) -> Result<(), UpdateError> {
        // BEGIN TRANSACTION (via staging directory)
        let txn_id = Uuid::new_v4();
        let staging = self.root.join(".txn").join(txn_id.to_string());
        
        // 1. Write history delta to staging
        // 2. Write worker messages to staging  
        // 3. Write timer messages to staging
        // 4. Prepare message acks
        
        // COMMIT (atomic via rename)
        self.commit_transaction(staging).await?;
        
        // Release lock
        self.lock_manager.release(lock_token).await;
        
        Ok(())
    }
}
```

### Phase 6: Remove Old Provider Interface (Week 4-5)

**Remove:**
- `HistoryStore` trait
- All old provider implementations
- Old dispatch code
- Router/channel infrastructure

**Keep (temporarily):**
- Query APIs for testing/debugging
- Status APIs

### Phase 7: Cloud Provider Implementation (Week 5-6)

Implement for real cloud providers:
- Azure (using Storage Tables + Queues with transactions)
- AWS (using DynamoDB + SQS with transactions)
- GCP (using Firestore + Cloud Tasks)

## Migration Strategy

### Step 1: Parallel Implementation
- New provider runs alongside old
- Feature flag to switch between them
- Comprehensive comparison testing

### Step 2: Gradual Rollout
- Start with test environments
- Move to production with careful monitoring
- Keep old code for emergency rollback

### Step 3: Complete Cutover
- Remove old provider interface
- Remove all legacy code
- Simplify runtime significantly

## Success Metrics

1. **Reliability:**
   - Zero silent failures
   - All operations atomic
   - Proper error propagation

2. **Performance:**
   - Batched operations reduce round trips
   - Lock-based concurrency control
   - Efficient message processing

3. **Simplicity:**
   - ~50% less code in runtime
   - Clear separation of concerns
   - Easier to reason about

## Risk Mitigation

1. **Testing:**
   - Extensive unit tests with failure injection
   - Property-based testing for invariants
   - Chaos testing for reliability

2. **Rollback Plan:**
   - Keep old implementation initially
   - Feature flag for instant rollback
   - Careful monitoring of metrics

3. **Performance:**
   - Benchmark new vs old implementation
   - Optimize critical paths
   - Add caching where appropriate

## Code Structure After Refactor

```
src/
├── providers/
│   ├── mod.rs
│   ├── runtime_provider.rs      # New trait
│   ├── messages.rs              # Message types
│   ├── errors.rs                # Error types
│   ├── transactional_memory.rs  # Test implementation
│   ├── transactional_fs.rs      # FS implementation
│   └── transactional_azure.rs   # Cloud implementation
├── runtime/
│   ├── mod.rs                   # Simplified runtime
│   ├── execution.rs             # New execution model
│   ├── orchestration_turn.rs    # Pure computation
│   ├── worker.rs                # Worker dispatcher
│   └── timer.rs                 # Timer dispatcher
```

## Timeline

- **Week 1:** New provider interface + in-memory implementation
- **Week 2-3:** Refactor execution model
- **Week 3:** Refactor dispatchers  
- **Week 4:** FileSystem provider
- **Week 4-5:** Remove old interface
- **Week 5-6:** Cloud providers

Total: 6 weeks for complete refactor

## Conclusion

This refactor addresses every critical reliability issue by making transactionality the foundation rather than an afterthought. The new provider interface is simpler, more reliable, and easier to implement correctly. The runtime becomes much simpler by delegating consistency to the provider layer.
