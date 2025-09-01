# Provider Refactor: Detailed Implementation Guide

## Core Concept

The fundamental change is moving from **scattered operations** to **transactional batches**:

```rust
// ❌ OLD: Multiple failure points
let history = provider.read(instance).await;           // Read 1
let turn_result = execute_turn(history, messages);     
provider.append(instance, new_events).await?;          // Write 1 (can succeed)
let _ = provider.enqueue_work(activity_work).await;    // Write 2 (can fail!)
provider.ack(token).await?;                            // Write 3

// ✅ NEW: Single atomic operation  
let work = provider.fetch_orchestration_work().await;  // Read everything
let turn_result = execute_turn(work);
provider.update_orchestration(                         // Write everything atomically
    work.lock_token,
    history_delta,
    worker_messages,
    timer_messages
).await?;
```

## Implementation Walkthrough

### 1. New Provider Types

```rust
// src/providers/runtime_provider.rs

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Core provider interface - all operations are transactional
#[async_trait]
pub trait RuntimeProvider: Send + Sync {
    /// Fetch next orchestration work item with exclusive lock
    async fn fetch_orchestration_work(&self) -> Option<OrchestrationWorkItem>;
    
    /// Atomically update all orchestration state
    async fn update_orchestration(
        &self,
        lock_token: String,
        update: OrchestrationUpdate,
    ) -> Result<(), ProviderError>;
    
    /// Abandon work for retry later
    async fn abandon_orchestration(
        &self,
        lock_token: String,
        retry_after_ms: u64,
    ) -> Result<(), ProviderError>;
    
    /// Fetch next activity to execute
    async fn fetch_worker_item(&self) -> Option<WorkerItem>;
    
    /// Complete activity and notify orchestrator atomically
    async fn complete_activity(
        &self,
        lock_token: String,
        completion: ActivityCompletion,
    ) -> Result<(), ProviderError>;
    
    /// Fetch next timer to schedule
    async fn fetch_timer_item(&self) -> Option<TimerItem>;
    
    /// Acknowledge timer after scheduling
    async fn ack_timer(&self, lock_token: String) -> Result<(), ProviderError>;
}

/// Work item for orchestration execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestrationWorkItem {
    /// Exclusive lock token for this work
    pub lock_token: String,
    
    /// Instance metadata
    pub instance_id: String,
    pub orchestration_name: String,
    pub execution_id: u64,
    pub version: semver::Version,
    
    /// Current state
    pub history: Vec<Event>,
    
    /// Messages to process (may be empty for new instances)
    pub messages: Vec<OrchestratorMessage>,
    
    /// Parent linkage for sub-orchestrations
    pub parent: Option<ParentLink>,
}

/// Atomic update for orchestration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestrationUpdate {
    /// Events to append to history
    pub history_delta: Vec<Event>,
    
    /// Activities to execute
    pub activities: Vec<ActivityRequest>,
    
    /// Timers to schedule
    pub timers: Vec<TimerRequest>,
    
    /// External events to subscribe to
    pub subscriptions: Vec<ExternalSubscription>,
    
    /// Sub-orchestrations to start
    pub sub_orchestrations: Vec<SubOrchestrationRequest>,
    
    /// Terminal state if orchestration completed
    pub terminal_state: Option<TerminalState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TerminalState {
    Completed(String),
    Failed(String),
    Cancelled(String),
    ContinuedAsNew { input: String, version: Option<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityRequest {
    pub activity_id: u64,
    pub name: String,
    pub input: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimerRequest {
    pub timer_id: u64,
    pub fire_at_ms: u64,
}
```

### 2. Refactored Execution Logic

```rust
// src/runtime/execution.rs

use std::sync::Arc;
use crate::providers::runtime_provider::{RuntimeProvider, OrchestrationUpdate, TerminalState};

pub struct ExecutionEngine {
    registry: Arc<OrchestrationRegistry>,
    provider: Arc<dyn RuntimeProvider>,
}

impl ExecutionEngine {
    /// Main execution loop - fetch and process orchestration work
    pub async fn run_orchestration_loop(self: Arc<Self>) {
        loop {
            match self.process_next_orchestration().await {
                Ok(true) => continue,  // Processed work
                Ok(false) => {
                    // No work available, idle
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => {
                    error!("Orchestration processing failed: {}", e);
                    // Error is logged but we continue processing
                }
            }
        }
    }
    
    /// Process a single orchestration work item
    async fn process_next_orchestration(&self) -> Result<bool, ExecutionError> {
        // Fetch work with exclusive lock
        let Some(work) = self.provider.fetch_orchestration_work().await else {
            return Ok(false); // No work available
        };
        
        // Resolve orchestration handler
        let handler = self.registry
            .resolve(&work.orchestration_name, &work.version)
            .await
            .map_err(|e| ExecutionError::HandlerNotFound(e))?;
        
        // Execute turns until no more progress
        let mut executor = TurnExecutor::new(work.clone(), handler);
        let mut total_update = OrchestrationUpdate::default();
        
        loop {
            // Execute one turn
            let turn_update = executor.execute_turn()?;
            
            // Accumulate updates
            total_update.merge(turn_update.clone());
            
            // Check if we should continue
            if !turn_update.made_progress() {
                break;
            }
            
            // Check for terminal state
            if turn_update.terminal_state.is_some() {
                total_update.terminal_state = turn_update.terminal_state;
                break;
            }
        }
        
        // Commit all changes atomically
        match self.provider.update_orchestration(work.lock_token, total_update).await {
            Ok(_) => Ok(true),
            Err(e) if e.is_lock_lost() => {
                // Lock was lost, another worker took over
                warn!("Lock lost for instance {}", work.instance_id);
                Ok(true)
            }
            Err(e) if e.is_transient() => {
                // Transient error, abandon for retry
                self.provider.abandon_orchestration(
                    work.lock_token, 
                    5000 // Retry after 5 seconds
                ).await?;
                Err(ExecutionError::Transient(e))
            }
            Err(e) => {
                // Fatal error
                error!("Failed to update orchestration: {}", e);
                
                // Try to mark as failed
                let failure_update = OrchestrationUpdate {
                    history_delta: vec![Event::OrchestrationFailed { 
                        error: e.to_string() 
                    }],
                    terminal_state: Some(TerminalState::Failed(e.to_string())),
                    ..Default::default()
                };
                
                let _ = self.provider.update_orchestration(
                    work.lock_token, 
                    failure_update
                ).await;
                
                Err(ExecutionError::Fatal(e))
            }
        }
    }
}

/// Executes individual turns within an orchestration
struct TurnExecutor {
    work: OrchestrationWorkItem,
    handler: Arc<dyn OrchestrationHandler>,
    current_history: Vec<Event>,
    processed_messages: HashSet<MessageId>,
}

impl TurnExecutor {
    fn execute_turn(&mut self) -> Result<OrchestrationUpdate, TurnError> {
        // Build completion map from unprocessed messages
        let completion_map = self.build_completion_map()?;
        
        // Extract input from history
        let input = self.extract_input()?;
        
        // Run orchestration logic
        let (new_events, actions, output) = run_orchestration_logic(
            &*self.handler,
            self.current_history.clone(),
            completion_map,
            input,
        )?;
        
        // Update internal state
        self.current_history.extend(new_events.clone());
        self.mark_messages_processed();
        
        // Build update from actions
        let mut update = OrchestrationUpdate {
            history_delta: new_events,
            ..Default::default()
        };
        
        // Convert actions to requests
        for action in actions {
            match action {
                Action::CallActivity { id, name, input } => {
                    update.activities.push(ActivityRequest {
                        activity_id: id,
                        name,
                        input,
                    });
                }
                Action::CreateTimer { id, delay_ms } => {
                    let fire_at = self.get_current_time() + delay_ms;
                    update.timers.push(TimerRequest {
                        timer_id: id,
                        fire_at_ms: fire_at,
                    });
                }
                Action::StartSubOrchestration { id, name, instance, input } => {
                    update.sub_orchestrations.push(SubOrchestrationRequest {
                        sub_id: id,
                        name,
                        instance,
                        input,
                    });
                }
                Action::ContinueAsNew { input, version } => {
                    update.terminal_state = Some(TerminalState::ContinuedAsNew {
                        input,
                        version,
                    });
                }
            }
        }
        
        // Handle orchestration output
        if let Some(result) = output {
            update.terminal_state = Some(match result {
                Ok(value) => TerminalState::Completed(value),
                Err(error) => TerminalState::Failed(error),
            });
        }
        
        Ok(update)
    }
}
```

### 3. Worker Execution

```rust
// src/runtime/worker.rs

pub struct WorkerEngine {
    activities: Arc<ActivityRegistry>,
    provider: Arc<dyn RuntimeProvider>,
}

impl WorkerEngine {
    pub async fn run_worker_loop(self: Arc<Self>) {
        loop {
            match self.process_next_activity().await {
                Ok(true) => continue,
                Ok(false) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => {
                    error!("Activity processing failed: {}", e);
                }
            }
        }
    }
    
    async fn process_next_activity(&self) -> Result<bool, WorkerError> {
        // Fetch work
        let Some(work) = self.provider.fetch_worker_item().await else {
            return Ok(false);
        };
        
        // Execute activity
        let result = match self.activities.get(&work.activity_name) {
            Some(handler) => {
                match handler.invoke(work.input.clone()).await {
                    Ok(output) => ActivityCompletion::Success {
                        instance_id: work.instance_id,
                        execution_id: work.execution_id,
                        activity_id: work.activity_id,
                        output,
                    },
                    Err(error) => ActivityCompletion::Failure {
                        instance_id: work.instance_id,
                        execution_id: work.execution_id,
                        activity_id: work.activity_id,
                        error,
                    },
                }
            }
            None => {
                ActivityCompletion::Failure {
                    instance_id: work.instance_id,
                    execution_id: work.execution_id,
                    activity_id: work.activity_id,
                    error: format!("Activity '{}' not registered", work.activity_name),
                }
            }
        };
        
        // Complete atomically (ack work item + enqueue completion)
        self.provider.complete_activity(work.lock_token, result).await?;
        
        Ok(true)
    }
}
```

### 4. Example Provider Implementation

```rust
// src/providers/transactional_fs.rs

pub struct TransactionalFsProvider {
    root: PathBuf,
    locks: Arc<Mutex<HashMap<String, LockInfo>>>,
}

impl RuntimeProvider for TransactionalFsProvider {
    async fn fetch_orchestration_work(&self) -> Option<OrchestrationWorkItem> {
        // Scan for next available message
        let queue_dir = self.root.join("queues/orchestrator");
        let messages = self.scan_queue_directory(&queue_dir).await?;
        
        // Group by instance
        let instance_messages = self.group_by_instance(messages);
        
        // Try to acquire lock on an instance
        for (instance_id, messages) in instance_messages {
            if let Some(lock_token) = self.try_acquire_lock(&instance_id).await {
                // Load history
                let history = self.load_instance_history(&instance_id).await;
                
                // Load metadata
                let metadata = self.load_instance_metadata(&instance_id).await?;
                
                return Some(OrchestrationWorkItem {
                    lock_token,
                    instance_id,
                    orchestration_name: metadata.name,
                    execution_id: metadata.execution_id,
                    version: metadata.version,
                    history,
                    messages,
                    parent: metadata.parent,
                });
            }
        }
        
        None
    }
    
    async fn update_orchestration(
        &self,
        lock_token: String,
        update: OrchestrationUpdate,
    ) -> Result<(), ProviderError> {
        // Verify lock is still held
        self.verify_lock(&lock_token)?;
        
        // Create transaction directory
        let txn_id = Uuid::new_v4();
        let txn_dir = self.root.join(".transactions").join(txn_id.to_string());
        fs::create_dir_all(&txn_dir).await?;
        
        // Stage all changes
        let mut staged_ops = Vec::new();
        
        // Stage history append
        if !update.history_delta.is_empty() {
            let history_file = self.stage_history_append(
                &txn_dir, 
                &lock_token,
                &update.history_delta
            ).await?;
            staged_ops.push(history_file);
        }
        
        // Stage activity messages
        for activity in update.activities {
            let msg_file = self.stage_queue_message(
                &txn_dir,
                QueueKind::Worker,
                WorkerMessage::ActivityExecute(activity),
            ).await?;
            staged_ops.push(msg_file);
        }
        
        // Stage timer messages
        for timer in update.timers {
            let msg_file = self.stage_queue_message(
                &txn_dir,
                QueueKind::Timer,
                TimerMessage::Schedule(timer),
            ).await?;
            staged_ops.push(msg_file);
        }
        
        // Stage message acknowledgments
        let ack_file = self.stage_message_acks(&txn_dir, &lock_token).await?;
        staged_ops.push(ack_file);
        
        // Commit transaction atomically
        self.commit_transaction(staged_ops).await?;
        
        // Release lock
        self.release_lock(lock_token).await;
        
        Ok(())
    }
    
    async fn commit_transaction(&self, staged_ops: Vec<StagedOp>) -> Result<(), ProviderError> {
        // Use rename for atomicity
        for op in staged_ops {
            match op {
                StagedOp::FileMove { from, to } => {
                    fs::rename(from, to).await
                        .map_err(|e| ProviderError::CommitFailed(e.to_string()))?;
                }
                StagedOp::FileAppend { source, target } => {
                    let data = fs::read(source).await?;
                    let mut file = OpenOptions::new()
                        .append(true)
                        .open(target)
                        .await?;
                    file.write_all(&data).await?;
                    fs::remove_file(source).await?;
                }
            }
        }
        Ok(())
    }
}
```

## Key Benefits

### 1. **Atomicity Guaranteed**
Every operation is all-or-nothing. No more partial failures.

### 2. **Simplified Runtime**
The runtime no longer needs to handle complex failure scenarios.

### 3. **Clear Ownership**
Lock tokens provide clear ownership semantics.

### 4. **Efficient Batching**
Multiple operations are batched into single transactions.

### 5. **Provider Flexibility**
Each provider can use its native transaction support optimally.

## Testing Strategy

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_atomic_update() {
        let provider = create_test_provider();
        
        // Fetch work
        let work = provider.fetch_orchestration_work().await.unwrap();
        
        // Create update
        let update = OrchestrationUpdate {
            history_delta: vec![Event::ActivityScheduled { id: 1, name: "test" }],
            activities: vec![ActivityRequest { 
                activity_id: 1, 
                name: "test", 
                input: "data" 
            }],
            ..Default::default()
        };
        
        // Inject failure during commit
        provider.inject_failure_at(FailurePoint::CommitTransaction);
        
        // Attempt update (should fail atomically)
        let result = provider.update_orchestration(work.lock_token, update).await;
        assert!(result.is_err());
        
        // Verify nothing was persisted
        let work2 = provider.fetch_orchestration_work().await.unwrap();
        assert_eq!(work2.history, work.history); // No changes
        
        // Verify worker queue is empty
        let worker_item = provider.fetch_worker_item().await;
        assert!(worker_item.is_none());
    }
    
    #[tokio::test] 
    async fn test_lock_semantics() {
        let provider = create_test_provider();
        
        // Worker 1 fetches work
        let work1 = provider.fetch_orchestration_work().await.unwrap();
        
        // Worker 2 tries to fetch same instance (should get nothing)
        let work2 = provider.fetch_orchestration_work().await;
        assert!(work2.is_none());
        
        // Worker 1 abandons
        provider.abandon_orchestration(work1.lock_token, 1000).await.unwrap();
        
        // Worker 2 still can't get it (retry delay)
        let work2 = provider.fetch_orchestration_work().await;
        assert!(work2.is_none());
        
        // After delay, Worker 2 can fetch
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let work2 = provider.fetch_orchestration_work().await;
        assert!(work2.is_some());
    }
}
```

## Migration Checklist

- [ ] Define new provider trait
- [ ] Implement in-memory provider for testing
- [ ] Refactor execution engine to use new provider
- [ ] Refactor worker dispatcher
- [ ] Refactor timer dispatcher  
- [ ] Implement FileSystem provider
- [ ] Add comprehensive tests
- [ ] Performance benchmarks
- [ ] Remove old HistoryStore trait
- [ ] Remove old dispatch code
- [ ] Update all tests to use new provider
- [ ] Documentation update
- [ ] Cloud provider implementations

## Conclusion

This refactor fundamentally transforms DTF from a system with multiple failure points to one with guaranteed transactional semantics. The new provider interface is simpler, more reliable, and maps directly to modern storage systems' native transaction support.
