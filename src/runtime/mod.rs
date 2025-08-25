use crate::_typed_codec::{Codec, Json};
use crate::providers::in_memory::InMemoryHistoryStore;
use crate::providers::{HistoryStore, QueueKind, WorkItem};
use crate::{Event, OrchestrationContext};
use semver::Version;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
pub mod completions;
pub mod detect;
pub mod dispatch;
pub mod registry;
pub mod router;
pub mod status;
mod timers;
use async_trait::async_trait;
use std::collections::HashSet;

pub mod replay;

/// High-level orchestration status derived from history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrchestrationStatus {
    NotFound,
    Running,
    Completed { output: String },
    Failed { error: String },
}

/// Error type returned by orchestration wait helpers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitError {
    Timeout,
    Other(String),
}

/// Trait implemented by orchestration handlers that can be invoked by the runtime.
#[async_trait]
pub trait OrchestrationHandler: Send + Sync {
    async fn invoke(&self, ctx: OrchestrationContext, input: String) -> Result<String, String>;
}

/// Function wrapper that implements `OrchestrationHandler`.
pub struct FnOrchestration<F, Fut>(pub F)
where
    F: Fn(OrchestrationContext, String) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<String, String>> + Send + 'static;

#[async_trait]
impl<F, Fut> OrchestrationHandler for FnOrchestration<F, Fut>
where
    F: Fn(OrchestrationContext, String) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<String, String>> + Send + 'static,
{
    async fn invoke(&self, ctx: OrchestrationContext, input: String) -> Result<String, String> {
        (self.0)(ctx, input).await
    }
}

/// Immutable registry mapping orchestration names to versioned handlers.
pub use crate::runtime::registry::{OrchestrationRegistry, OrchestrationRegistryBuilder, VersionPolicy};

// Legacy VersionedOrchestrationRegistry removed; use OrchestrationRegistry instead

// ActivityWorkItem removed; activities are executed by WorkDispatcher via provider queues

// TimerWorkItem no longer used; timers flow via provider-backed queues

pub use router::{InstanceRouter, OrchestratorMsg};

/// In-process runtime that executes activities and timers and persists
/// history via a `HistoryStore`.
pub struct Runtime {
    // removed: in-proc activity channel
    router_tx: mpsc::UnboundedSender<OrchestratorMsg>,
    router: Arc<InstanceRouter>,
    joins: Mutex<Vec<JoinHandle<()>>>,
    instance_joins: Mutex<Vec<JoinHandle<()>>>,
    history_store: Arc<dyn HistoryStore>,
    active_instances: Mutex<HashSet<String>>,
    // pending_starts removed
    result_waiters: Mutex<HashMap<String, Vec<oneshot::Sender<(Vec<Event>, Result<String, String>)>>>>,
    orchestration_registry: OrchestrationRegistry,
    // Pinned versions for instances started in this runtime (in-memory for now)
    pinned_versions: Mutex<HashMap<String, Version>>,
    /// Track the current execution ID for each active instance
    current_execution_ids: Mutex<HashMap<String, u64>>,
    // StartRequest layer removed; instances are activated directly
}

/// Introspection: descriptor of an orchestration derived from history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestrationDescriptor {
    pub name: String,
    pub version: String,
    pub parent_instance: Option<String>,
    pub parent_id: Option<u64>,
}

impl Runtime {
    /// Internal: apply pure decisions by appending necessary history and dispatching work.
    async fn apply_decisions(
        self: &Arc<Self>,
        instance: &str,
        history: &Vec<Event>,
        decisions: Vec<crate::runtime::replay::Decision>,
    ) {
        debug!("apply_decisions: {instance} {decisions:#?}");
        for d in decisions {
            match d {
                crate::runtime::replay::Decision::ContinueAsNew { .. } => { /* handled by caller */ }
                crate::runtime::replay::Decision::CallActivity { id, name, input } => {
                    dispatch::dispatch_call_activity(self, instance, history, id, name, input).await;
                }
                crate::runtime::replay::Decision::CreateTimer { id, delay_ms } => {
                    dispatch::dispatch_create_timer(self, instance, history, id, delay_ms).await;
                }
                crate::runtime::replay::Decision::WaitExternal { id, name } => {
                    dispatch::dispatch_wait_external(self, instance, history, id, name).await;
                }
                crate::runtime::replay::Decision::StartOrchestrationDetached {
                    id,
                    name,
                    version,
                    instance: child_inst,
                    input,
                } => {
                    dispatch::dispatch_start_detached(self, instance, id, name, version, child_inst, input).await;
                }
                crate::runtime::replay::Decision::StartSubOrchestration {
                    id,
                    name,
                    version,
                    instance: child_suffix,
                    input,
                } => {
                    dispatch::dispatch_start_sub_orchestration(
                        self,
                        instance,
                        history,
                        id,
                        name,
                        version,
                        child_suffix,
                        input,
                    )
                    .await;
                }
            }
        }
    }
    /// Return the most recent descriptor `{ name, version, parent_instance?, parent_id? }` for an instance.
    /// Returns `None` if the instance/history does not exist or no OrchestrationStarted is present.
    pub async fn get_orchestration_descriptor(
        &self,
        instance: &str,
    ) -> Option<crate::runtime::OrchestrationDescriptor> {
        let hist = self.history_store.read(instance).await;
        for e in hist.iter().rev() {
            if let Event::OrchestrationStarted {
                name,
                version,
                parent_instance,
                parent_id,
                ..
            } = e
            {
                return Some(crate::runtime::OrchestrationDescriptor {
                    name: name.clone(),
                    version: version.clone(),
                    parent_instance: parent_instance.clone(),
                    parent_id: *parent_id,
                });
            }
        }
        None
    }
    // Associated constants for runtime behavior
    const COMPLETION_BATCH_LIMIT: usize = 128;
    const POLLER_GATE_DELAY_MS: u64 = 5;
    const POLLER_IDLE_SLEEP_MS: u64 = 10;
    const ORCH_IDLE_DEHYDRATE_MS: u64 = 1000;

    async fn ensure_instance_active(self: &Arc<Self>, instance: &str, orchestration_name: &str) -> bool {
        if self.active_instances.lock().await.contains(instance) {
            return false;
        }
        let inner = self.clone().spawn_instance_to_completion(instance, orchestration_name);
        // Wrap to normalize handle type to JoinHandle<()>
        let wrapper = tokio::spawn(async move {
            let _ = inner.await;
        });
        self.instance_joins.lock().await.push(wrapper);
        true
    }
    async fn start_internal_rx(
        self: Arc<Self>,
        instance: &str,
        orchestration_name: &str,
        input: String,
        pin_version: Option<Version>,
        parent_instance: Option<String>,
        parent_id: Option<u64>,
    ) -> Result<oneshot::Receiver<(Vec<Event>, Result<String, String>)>, String> {
        // Ensure instance exists (best-effort)
        let _ = self.history_store.create_instance(instance).await;
        // Append start marker if empty
        let hist = self.history_store.read(instance).await;
        if hist.is_empty() {
            // Determine version to pin and to write into the start event
            let maybe_resolved: Option<Version> = if let Some(v) = pin_version {
                Some(v)
            } else if let Some(v) = self.pinned_versions.lock().await.get(instance).cloned() {
                Some(v)
            } else if let Some((resolved_v, _handler)) =
                self.orchestration_registry.resolve_for_start(orchestration_name).await
            {
                Some(resolved_v)
            } else {
                None
            };
            // If resolved, pin for handler resolution and write that version; otherwise use a fallback string version
            let version_str_for_event: String = if let Some(v) = maybe_resolved.clone() {
                self.pinned_versions
                    .lock()
                    .await
                    .insert(instance.to_string(), v.clone());
                v.to_string()
            } else {
                "0.0.0".to_string()
            };
            let started = vec![Event::OrchestrationStarted {
                name: orchestration_name.to_string(),
                version: version_str_for_event,
                input,
                parent_instance,
                parent_id,
            }];
            self.history_store
                .append(instance, started)
                .await
                .map_err(|e| format!("failed to append OrchestrationStarted: {e}"))?;
        } else {
            // Allow duplicate starts as a warning for detached or at-least-once semantics
            warn!(
                instance,
                "instance already has history; duplicate start accepted (deduped)"
            );
        }
        // Enqueue a start request; background worker will dedupe and run exactly one execution
        self.ensure_instance_active(instance, orchestration_name).await;
        // Register a oneshot waiter for string result
        let (tx, rx) = oneshot::channel::<(Vec<Event>, Result<String, String>)>();
        self.result_waiters
            .lock()
            .await
            .entry(instance.to_string())
            .or_default()
            .push(tx);
        Ok(rx)
    }

    /// Get the current execution ID for an instance, or fetch from store if not tracked
    async fn get_execution_id_for_instance(&self, instance: &str) -> u64 {
        // First check in-memory tracking
        if let Some(&exec_id) = self.current_execution_ids.lock().await.get(instance) {
            return exec_id;
        }

        // Fall back to querying the store
        self.history_store.latest_execution_id(instance).await.unwrap_or(1)
    }

    /// Common handler for orchestrator-queue items that target a specific instance.
    /// Validates execution ID (if provided), ensures the instance is active (rehydrates) or
    /// forwards the message to the in-proc router with the provided ack token.
    async fn orchestrator_deliver_or_rehydrate<F>(
        self: &Arc<Self>,
        instance: &str,
        exec_id_check: Option<u64>,
        token: String,
        build_msg: F,
    ) where
        F: FnOnce(String) -> OrchestratorMsg,
    {
        // Validate execution ID first (if provided)
        if let Some(execution_id) = exec_id_check
            && !self.validate_completion_execution_id(instance, execution_id).await {
                let _ = self.history_store.ack(QueueKind::Orchestrator, &token).await;
                return;
            }

        // Ensure instance is active; if dehydrated, rehydrate and abandon for redelivery
        if !self.router.inboxes.lock().await.contains_key(instance) {
            let orch_name_opt = self.history_store.read(instance).await.iter().find_map(|e| match e {
                Event::OrchestrationStarted { name, .. } => Some(name.clone()),
                _ => None,
            });
            let orch_name = match orch_name_opt {
                Some(n) => n,
                None => {
                    error!(
                        instance,
                        "rehydration requested but no OrchestrationStarted found; state corruption"
                    );
                    panic!("no OrchestrationStarted in history for instance");
                }
            };
            self.ensure_instance_active(instance, &orch_name).await;
            let _ = self.history_store.abandon(QueueKind::Orchestrator, &token).await;
            tokio::time::sleep(std::time::Duration::from_millis(Self::POLLER_GATE_DELAY_MS)).await;
            return;
        }

        // Active: forward with ack token
        let msg = build_msg(token);
        let _ = self.router_tx.send(msg);
    }

    /// Validate that a completion's execution_id matches the current running execution.
    /// Returns true if valid, false if should be ignored (with warning logged).
    async fn validate_completion_execution_id(&self, instance: &str, completion_execution_id: u64) -> bool {
        let current_execution_ids = self.current_execution_ids.lock().await;
        if let Some(&current_id) = current_execution_ids.get(instance)
            && completion_execution_id != current_id {
                if completion_execution_id < current_id {
                    warn!(
                        instance = %instance,
                        completion_execution_id = completion_execution_id,
                        current_execution_id = current_id,
                        "ignoring completion from older execution (likely from ContinueAsNew)"
                    );
                } else {
                    warn!(
                        instance = %instance,
                        completion_execution_id = completion_execution_id,
                        current_execution_id = current_id,
                        "ignoring completion from future execution (unexpected)"
                    );
                }
                return false;
            }
        true
    }

    /// Enqueue a new orchestration instance start. The runtime will pick this up
    /// in the background and drive it to completion.
    /// Start a typed orchestration; input/output are serialized internally.
    pub async fn start_orchestration_typed<In, Out>(
        self: Arc<Self>,
        instance: &str,
        orchestration_name: &str,
        input: In,
    ) -> Result<JoinHandle<(Vec<Event>, Result<Out, String>)>, String>
    where
        In: Serialize,
        Out: DeserializeOwned + Send + 'static,
    {
        let payload = Json::encode(&input).map_err(|e| format!("encode: {e}"))?;
        let rx = self
            .clone()
            .start_internal_rx(instance, orchestration_name, payload, None, None, None)
            .await?;
        Ok(tokio::spawn(async move {
            let (hist, res_s) = rx.await.expect("result");
            let res_t: Result<Out, String> = match res_s {
                Ok(s) => Json::decode::<Out>(&s),
                Err(e) => Err(e),
            };
            (hist, res_t)
        }))
    }

    /// Start a typed orchestration with an explicit version (semver string).
    pub async fn start_orchestration_versioned_typed<In, Out>(
        self: Arc<Self>,
        instance: &str,
        orchestration_name: &str,
        version: impl AsRef<str>,
        input: In,
    ) -> Result<JoinHandle<(Vec<Event>, Result<Out, String>)>, String>
    where
        In: Serialize,
        Out: DeserializeOwned + Send + 'static,
    {
        let v = semver::Version::parse(version.as_ref()).map_err(|e| e.to_string())?;
        let payload = Json::encode(&input).map_err(|e| format!("encode: {e}"))?;
        let rx = self
            .clone()
            .start_internal_rx(instance, orchestration_name, payload, Some(v), None, None)
            .await?;
        Ok(tokio::spawn(async move {
            let (hist, res_s) = rx.await.expect("result");
            let res_t: Result<Out, String> = match res_s {
                Ok(s) => Json::decode::<Out>(&s),
                Err(e) => Err(e),
            };
            (hist, res_t)
        }))
    }

    /// Start an orchestration using raw String input/output (back-compat API).
    pub async fn start_orchestration(
        self: Arc<Self>,
        instance: &str,
        orchestration_name: &str,
        input: impl Into<String>,
    ) -> Result<JoinHandle<(Vec<Event>, Result<String, String>)>, String> {
        let rx = self
            .clone()
            .start_internal_rx(instance, orchestration_name, input.into(), None, None, None)
            .await?;
        Ok(tokio::spawn(async move { rx.await.expect("result") }))
    }

    /// Start an orchestration with an explicit version (string I/O).
    pub async fn start_orchestration_versioned(
        self: Arc<Self>,
        instance: &str,
        orchestration_name: &str,
        version: impl AsRef<str>,
        input: impl Into<String>,
    ) -> Result<JoinHandle<(Vec<Event>, Result<String, String>)>, String> {
        let v = semver::Version::parse(version.as_ref()).map_err(|e| e.to_string())?;
        let rx = self
            .clone()
            .start_internal_rx(instance, orchestration_name, input.into(), Some(v), None, None)
            .await?;
        Ok(tokio::spawn(async move { rx.await.expect("result") }))
    }

    /// Internal: start an orchestration and record parent linkage.
    pub(crate) async fn start_orchestration_with_parent(
        self: Arc<Self>,
        instance: &str,
        orchestration_name: &str,
        input: impl Into<String>,
        parent_instance: String,
        parent_id: u64,
        version: Option<semver::Version>,
    ) -> Result<JoinHandle<(Vec<Event>, Result<String, String>)>, String> {
        let rx = self
            .clone()
            .start_internal_rx(
                instance,
                orchestration_name,
                input.into(),
                version,
                Some(parent_instance),
                Some(parent_id),
            )
            .await?;
        Ok(tokio::spawn(async move { rx.await.expect("result") }))
    }

    // Status helpers implemented in status.rs

    // Status helpers moved to status.rs
    /// Start a new runtime using the in-memory history store.
    pub async fn start(
        activity_registry: Arc<registry::ActivityRegistry>,
        orchestration_registry: OrchestrationRegistry,
    ) -> Arc<Self> {
        let history_store: Arc<dyn HistoryStore> = Arc::new(InMemoryHistoryStore::default());
        Self::start_with_store(history_store, activity_registry, orchestration_registry).await
    }

    /// Start a new runtime with a custom `HistoryStore` implementation.
    pub async fn start_with_store(
        history_store: Arc<dyn HistoryStore>,
        activity_registry: Arc<registry::ActivityRegistry>,
        orchestration_registry: OrchestrationRegistry,
    ) -> Arc<Self> {
        // Install a default subscriber if none set (ok to call many times)
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
            .try_init();

        let (router_tx, mut router_rx) = mpsc::unbounded_channel::<OrchestratorMsg>();
        let router = Arc::new(InstanceRouter {
            inboxes: Mutex::new(HashMap::new()),
        });
        let mut joins: Vec<JoinHandle<()>> = Vec::new();

        // spawn router forwarding task
        let router_clone = router.clone();
        joins.push(tokio::spawn(async move {
            while let Some(msg) = router_rx.recv().await {
                router_clone.forward(msg).await;
            }
        }));

        // start request queue + worker
        let runtime = Arc::new(Self {
            router_tx,
            router,
            joins: Mutex::new(joins),
            instance_joins: Mutex::new(Vec::new()),
            history_store,
            active_instances: Mutex::new(HashSet::new()),
            result_waiters: Mutex::new(HashMap::new()),
            orchestration_registry,
            pinned_versions: Mutex::new(HashMap::new()),
            current_execution_ids: Mutex::new(HashMap::new()),
        });

        // background orchestrator dispatcher (extracted from inline poller)
        let handle = runtime.clone().start_orchestration_dispatcher();
        runtime.joins.lock().await.push(handle);

        // background work dispatcher (executes activities)
        let work_handle = runtime.clone().start_work_dispatcher(activity_registry);
        runtime.joins.lock().await.push(work_handle);

        // background timer dispatcher (scaffold); current implementation handled by run_timer_worker
        // kept here as an explicit lifecycle hook for provider-backed timer queue
        let timer_handle = runtime.clone().start_timer_dispatcher();
        runtime.joins.lock().await.push(timer_handle);

        runtime
    }

    fn start_orchestration_dispatcher(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if let Some((item, token)) = self.history_store.dequeue_peek_lock(QueueKind::Orchestrator).await {
                    match item {
                        WorkItem::StartOrchestration {
                            instance,
                            orchestration,
                            input,
                        } => {
                            debug!("StartOrchestration: {instance} {orchestration} {input}");
                            match self.clone().start_orchestration(&instance, &orchestration, input).await {
                                _ => {}
                            }
                            let _ = self.history_store.ack(QueueKind::Orchestrator, &token).await;
                        }
                        WorkItem::ActivityCompleted {
                            instance,
                            execution_id,
                            id,
                            result,
                        } => {
                            debug!("ActivityCompleted: {instance} {execution_id} {id} {result}");
                            self.orchestrator_deliver_or_rehydrate(&instance, Some(execution_id), token, {
                                let instance_c = instance.clone();
                                let result_c = result.clone();
                                move |t| OrchestratorMsg::ActivityCompleted {
                                    instance: instance_c,
                                    execution_id,
                                    id,
                                    result: result_c,
                                    ack_token: Some(t),
                                }
                            })
                            .await;
                        }
                        WorkItem::ActivityFailed {
                            instance,
                            execution_id,
                            id,
                            error,
                        } => {
                            debug!("ActivityFailed: {instance} {execution_id} {id} {error}");
                            self.orchestrator_deliver_or_rehydrate(&instance, Some(execution_id), token, {
                                let instance_c = instance.clone();
                                let error_c = error.clone();
                                move |t| OrchestratorMsg::ActivityFailed {
                                    instance: instance_c,
                                    execution_id,
                                    id,
                                    error: error_c,
                                    ack_token: Some(t),
                                }
                            })
                            .await;
                        }
                        WorkItem::TimerFired {
                            instance,
                            execution_id,
                            id,
                            fire_at_ms,
                        } => {
                            debug!("TimerFired: {instance} {execution_id} {id} {fire_at_ms}");
                            self.orchestrator_deliver_or_rehydrate(&instance, Some(execution_id), token, {
                                let instance_c = instance.clone();
                                move |t| OrchestratorMsg::TimerFired {
                                    instance: instance_c,
                                    execution_id,
                                    id,
                                    fire_at_ms,
                                    ack_token: Some(t),
                                }
                            })
                            .await;
                        }
                        // No TimerSchedule should land on Orchestrator queue
                        WorkItem::ExternalRaised { instance, name, data } => {
                            debug!("ExternalRaised: {instance} {name} {data}");
                            self.orchestrator_deliver_or_rehydrate(&instance, None, token, {
                                let instance_c = instance.clone();
                                let name_c = name.clone();
                                let data_c = data.clone();
                                move |t| OrchestratorMsg::ExternalByName {
                                    instance: instance_c,
                                    name: name_c,
                                    data: data_c,
                                    ack_token: Some(t),
                                }
                            })
                            .await;
                        }
                        WorkItem::SubOrchCompleted {
                            parent_instance,
                            parent_execution_id,
                            parent_id,
                            result,
                        } => {
                            debug!("SubOrchCompleted: {parent_instance} {parent_execution_id} {parent_id} {result}");
                            let inst = parent_instance.clone();
                            self.orchestrator_deliver_or_rehydrate(&inst, Some(parent_execution_id), token, move |t| {
                                OrchestratorMsg::SubOrchCompleted {
                                    instance: parent_instance,
                                    execution_id: parent_execution_id,
                                    id: parent_id,
                                    result,
                                    ack_token: Some(t),
                                }
                            })
                            .await;
                        }
                        WorkItem::SubOrchFailed {
                            parent_instance,
                            parent_execution_id,
                            parent_id,
                            error,
                        } => {
                            debug!("SubOrchFailed: {parent_instance} {parent_execution_id} {parent_id} {error}");
                            let inst = parent_instance.clone();
                            self.orchestrator_deliver_or_rehydrate(&inst, Some(parent_execution_id), token, move |t| {
                                OrchestratorMsg::SubOrchFailed {
                                    instance: parent_instance,
                                    execution_id: parent_execution_id,
                                    id: parent_id,
                                    error,
                                    ack_token: Some(t),
                                }
                            })
                            .await;
                        }
                        WorkItem::CancelInstance { instance, reason } => {
                            debug!("CancelInstance: {instance} {reason}");
                            // Attempt to deliver; if inbox missing or dropped, rehydrate by enqueuing a start
                            if self
                                .router
                                .try_send(OrchestratorMsg::CancelRequested {
                                    instance: instance.clone(),
                                    reason: reason.clone(),
                                    ack_token: Some(token.clone()),
                                })
                                .await
                                .is_err()
                            {
                                let orch_name_opt =
                                    self.history_store.read(&instance).await.iter().find_map(|e| match e {
                                        Event::OrchestrationStarted { name, .. } => Some(name.clone()),
                                        _ => None,
                                    });
                                let orch_name = match orch_name_opt {
                                    Some(n) => n,
                                    None => {
                                        error!(
                                            instance,
                                            "rehydration requested but no OrchestrationStarted found; state corruption"
                                        );
                                        panic!("no OrchestrationStarted in history for instance");
                                    }
                                };
                                self.ensure_instance_active(&instance, &orch_name).await;
                                let _ = self.history_store.abandon(QueueKind::Orchestrator, &token).await;
                                tokio::time::sleep(std::time::Duration::from_millis(Self::POLLER_GATE_DELAY_MS)).await;
                            }
                        }
                        // No ActivityExecute should land on Orchestrator queue
                        other => {
                            error!(
                                ?other,
                                "unexpected WorkItem in Orchestrator dispatcher; state corruption"
                            );
                            panic!("unexpected WorkItem in Orchestrator dispatcher");
                        }
                    }
                } else {
                    tokio::time::sleep(std::time::Duration::from_millis(Self::POLLER_IDLE_SLEEP_MS)).await;
                }
            }
        })
    }

    fn start_work_dispatcher(self: Arc<Self>, activities: Arc<registry::ActivityRegistry>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if let Some((item, token)) = self.history_store.dequeue_peek_lock(QueueKind::Worker).await {
                    match item {
                        WorkItem::ActivityExecute {
                            instance,
                            execution_id,
                            id,
                            name,
                            input,
                        } => {
                            // Execute activity via registry directly; enqueue completion/failure to orchestrator queue
                            if let Some(handler) = activities.get(&name) {
                                match handler.invoke(input).await {
                                    Ok(result) => {
                                        let _ = self
                                            .history_store
                                            .enqueue_work(
                                                QueueKind::Orchestrator,
                                                WorkItem::ActivityCompleted {
                                                    instance: instance.clone(),
                                                    execution_id,
                                                    id,
                                                    result,
                                                },
                                            )
                                            .await;
                                    }
                                    Err(error) => {
                                        let _ = self
                                            .history_store
                                            .enqueue_work(
                                                QueueKind::Orchestrator,
                                                WorkItem::ActivityFailed {
                                                    instance: instance.clone(),
                                                    execution_id,
                                                    id,
                                                    error,
                                                },
                                            )
                                            .await;
                                    }
                                }
                            } else {
                                let _ = self
                                    .history_store
                                    .enqueue_work(
                                        QueueKind::Orchestrator,
                                        WorkItem::ActivityFailed {
                                            instance: instance.clone(),
                                            execution_id,
                                            id,
                                            error: format!("unregistered:{}", name),
                                        },
                                    )
                                    .await;
                            }
                            let _ = self.history_store.ack(QueueKind::Worker, &token).await;
                        }
                        other => {
                            error!(?other, "unexpected WorkItem in Worker dispatcher; state corruption");
                            panic!("unexpected WorkItem in Worker dispatcher");
                        }
                    }
                } else {
                    tokio::time::sleep(std::time::Duration::from_millis(Self::POLLER_IDLE_SLEEP_MS)).await;
                }
            }
        })
    }

    fn start_timer_dispatcher(self: Arc<Self>) -> JoinHandle<()> {
        if self.history_store.supports_delayed_visibility() {
            return tokio::spawn(async move {
                loop {
                    if let Some((item, token)) = self.history_store.dequeue_peek_lock(QueueKind::Timer).await {
                        match item {
                            WorkItem::TimerSchedule {
                                instance,
                                execution_id,
                                id,
                                fire_at_ms,
                            } => {
                                // Provider supports delayed visibility: enqueue TimerFired with fire_at_ms and let provider deliver when due
                                let _ = self
                                    .history_store
                                    .enqueue_work(
                                        QueueKind::Orchestrator,
                                        WorkItem::TimerFired {
                                            instance,
                                            execution_id,
                                            id,
                                            fire_at_ms,
                                        },
                                    )
                                    .await;
                                let _ = self.history_store.ack(QueueKind::Timer, &token).await;
                            }
                            other => {
                                error!(?other, "unexpected WorkItem in Timer dispatcher; state corruption");
                                panic!("unexpected WorkItem in Timer dispatcher");
                            }
                        }
                    } else {
                        tokio::time::sleep(std::time::Duration::from_millis(Self::POLLER_IDLE_SLEEP_MS)).await;
                    }
                }
            });
        }

        // Fallback in-process timer service (refactored)
        tokio::spawn(async move {
            let (svc_jh, svc_tx) =
                crate::runtime::timers::TimerService::start(self.history_store.clone(), Self::POLLER_IDLE_SLEEP_MS);

            // Intake task: keep pulling schedules and forwarding to service, then ack
            let intake_rt = self.clone();
            let intake_tx = svc_tx.clone();
            tokio::spawn(async move {
                loop {
                    if let Some((item, token)) = intake_rt.history_store.dequeue_peek_lock(QueueKind::Timer).await {
                        match item {
                            WorkItem::TimerSchedule {
                                instance,
                                execution_id,
                                id,
                                fire_at_ms,
                            } => {
                                let _ = intake_tx.send(WorkItem::TimerSchedule {
                                    instance,
                                    execution_id,
                                    id,
                                    fire_at_ms,
                                });
                                let _ = intake_rt.history_store.ack(QueueKind::Timer, &token).await;
                            }
                            other => {
                                error!(?other, "unexpected WorkItem in Timer dispatcher; state corruption");
                                panic!("unexpected WorkItem in Timer dispatcher");
                            }
                        }
                    } else {
                        tokio::time::sleep(std::time::Duration::from_millis(Self::POLLER_IDLE_SLEEP_MS)).await;
                    }
                }
            });
            // Keep service join handle alive within dispatcher lifetime
            let _ = svc_jh.await;
        })
    }

    /// Abort background tasks. Channels are dropped with the runtime.
    pub async fn shutdown(self: Arc<Self>) {
        // Abort background tasks; channels will be dropped with Runtime
        let mut joins = self.joins.lock().await;
        for j in joins.drain(..) {
            j.abort();
        }
    }

    /// Await completion of all outstanding spawned orchestration instances.
    pub async fn drain_instances(self: Arc<Self>) {
        let mut joins = self.instance_joins.lock().await;
        while let Some(j) = joins.pop() {
            let _ = j.await;
        }
    }

    /// Run a single instance to completion by orchestration name, returning
    /// its final history and output.
    pub async fn run_instance_to_completion(
        self: Arc<Self>,
        instance: &str,
        orchestration_name: &str,
    ) -> (Vec<Event>, Result<String, String>) {
        // Ensure instance not already active in this runtime
        {
            let mut act = self.active_instances.lock().await;
            if !act.insert(instance.to_string()) {
                // Already active: bail out gracefully
                return (Vec::new(), Err("already_active".into()));
            }
        }
        // Ensure removal of active flag even if the task panics
        struct ActiveGuard {
            rt: Arc<Runtime>,
            inst: String,
        }
        impl Drop for ActiveGuard {
            fn drop(&mut self) {
                // best-effort removal; ignore poisoning
                let rt = self.rt.clone();
                let inst = self.inst.clone();
                // spawn a blocking remove since Drop can't be async
                let _ = tokio::spawn(async move {
                    rt.active_instances.lock().await.remove(&inst);
                    rt.current_execution_ids.lock().await.remove(&inst);
                });
            }
        }
        let _active_guard = ActiveGuard {
            rt: self.clone(),
            inst: instance.to_string(),
        };
        // Instance is expected to be created by start_orchestration; do not create here
        // Load existing history from store (now exists). For ContinueAsNew we may start with a
        // fresh execution that only has OrchestrationStarted; we always want the latest execution.
        let mut history: Vec<Event> = self.history_store.read(instance).await;

        // Track the current execution ID for this instance
        let current_execution_id = self.history_store.latest_execution_id(instance).await.unwrap_or(1);
        self.current_execution_ids
            .lock()
            .await
            .insert(instance.to_string(), current_execution_id);
        // Pin version from history if present for this orchestration; ignore placeholder "0.0.0"
        if let Some(ver_str) = history.iter().rev().find_map(|e| match e {
            Event::OrchestrationStarted { name: n, version, .. } if n == orchestration_name => Some(version.clone()),
            _ => None,
        })
            && ver_str != "0.0.0"
                && let Ok(v) = semver::Version::parse(&ver_str) {
                    self.pinned_versions.lock().await.insert(instance.to_string(), v);
                }
        let mut comp_rx = self.router.register(instance).await;

        // Rehydrate pending activities and timers from history
        completions::rehydrate_pending(instance, &history, &self.history_store).await;

        // Capture input and parent linkage from the most recent OrchestrationStarted for this orchestration
        let mut current_input: String = String::new();
        let mut parent_link: Option<(String, u64)> = None;
        for e in history.iter().rev() {
            if let Event::OrchestrationStarted {
                name: n,
                input,
                parent_instance,
                parent_id,
                ..
            } = e
                && n == orchestration_name {
                    current_input = input.clone();
                    if let (Some(pinst), Some(pid)) = (parent_instance.clone(), *parent_id) {
                        parent_link = Some((pinst, pid));
                    }
                    break;
                }
        }

        // Resolve handler now, preferring pinned version from history
        let pinned_v = self.pinned_versions.lock().await.get(instance).cloned();
        let orchestration_handler_opt = if let Some(v) = pinned_v.clone() {
            self.orchestration_registry.resolve_exact(orchestration_name, &v)
        } else {
            self.orchestration_registry.get(orchestration_name)
        };

        // If orchestration not registered, fail gracefully and exit
        if orchestration_handler_opt.is_none() {
            let err = if let Some(v) = pinned_v {
                format!("canceled: missing version {}@{}", orchestration_name, v)
            } else {
                format!("unregistered:{}", orchestration_name)
            };
            // Append terminal failed event (idempotent at provider)
            if let Err(e) = self
                .history_store
                .append(instance, vec![Event::OrchestrationFailed { error: err.clone() }])
                .await
            {
                error!(instance, error=%e, "failed to append OrchestrationFailed for unknown orchestration");
                panic!("history append failed: {e}");
            }
            // Reflect in local history and notify waiters
            history.push(Event::OrchestrationFailed { error: err.clone() });
            if let Some(waiters) = self.result_waiters.lock().await.remove(instance) {
                for w in waiters {
                    let _ = w.send((history.clone(), Err(err.clone())));
                }
            }
            // If this is a child, enqueue failure to parent
            if let Some((pinst, pid)) = parent_link.clone() {
                let _ = self
                    .history_store
                    .enqueue_work(
                        QueueKind::Orchestrator,
                        WorkItem::SubOrchFailed {
                            parent_instance: pinst.clone(),
                            parent_execution_id: self.get_execution_id_for_instance(&pinst).await,
                            parent_id: pid,
                            error: err.clone(),
                        },
                    )
                    .await;
            }
            return (history, Err(err));
        }

        let orchestration_handler = orchestration_handler_opt.unwrap();

        let mut turn_index: u64 = 0;
        // Track completions appended in the previous iteration to validate against current code's awaited ids
        let mut last_appended_completions: Vec<(&'static str, u64)> = Vec::new();
        loop {
            let baseline_len = history.len();
            // Use the extracted replay engine to produce pure decisions
            use crate::runtime::replay::ReplayEngine as _;
            let engine = crate::runtime::replay::DefaultReplayEngine::new();
            let (hist_after, decisions, _logs, out_opt, claims) = engine.replay(
                history,
                turn_index,
                orchestration_handler.clone(),
                current_input.clone(),
            );
            // Determinism guard: If prior history already contained at least one schedule event, and contained no
            // completion events yet (still at the same decision frontier), then the orchestrator must not introduce
            // net-new schedule events that were not present in prior history. This catches code swaps where, e.g.,
            // A1 was scheduled previously but the new code tries to schedule B1 without any new completion.
            if let Some(err) =
                detect::detect_frontier_nondeterminism(&hist_after[..baseline_len], &hist_after[baseline_len..])
            {
                let _ = self
                    .history_store
                    .append(instance, vec![Event::OrchestrationFailed { error: err.clone() }])
                    .await;
                history = hist_after.clone();
                history.push(Event::OrchestrationFailed { error: err.clone() });
                if let Some(waiters) = self.result_waiters.lock().await.remove(instance) {
                    for w in waiters {
                        let _ = w.send((history.clone(), Err(err.clone())));
                    }
                }
                return (history, Err(err));
            }
            history = hist_after;
            // (nondeterminism validation happens after completion batches, using last_appended_completions)
            // Validate any completions appended in the previous iteration against what the current code awaited.
            if !last_appended_completions.is_empty() {
                if let Some(err) = detect::detect_await_mismatch(&last_appended_completions, &claims) {
                    let _ = self
                        .history_store
                        .append(instance, vec![Event::OrchestrationFailed { error: err.clone() }])
                        .await;
                    history.push(Event::OrchestrationFailed { error: err.clone() });
                    if let Some(waiters) = self.result_waiters.lock().await.remove(instance) {
                        for w in waiters {
                            let _ = w.send((history.clone(), Err(err.clone())));
                        }
                    }
                    return (history, Err(err));
                }
                // Clear after validation
                last_appended_completions.clear();
            }
            // Handle ContinueAsNew as terminal for this execution, regardless of out_opt
            if let Some((input, version)) = decisions.iter().find_map(|d| match d {
                crate::runtime::replay::Decision::ContinueAsNew { input, version } => {
                    Some((input.clone(), version.clone()))
                }
                _ => None,
            }) {
                self.handle_continue_as_new(
                    instance,
                    orchestration_name,
                    &mut history,
                    input.clone(),
                    version.clone(),
                )
                .await;
                return (history, Ok(String::new()));
            }
            if let Some(out) = out_opt {
                // Persist any deltas produced during this final turn
                let deltas = if history.len() > baseline_len {
                    history[baseline_len..].to_vec()
                } else {
                    Vec::new()
                };
                if !deltas.is_empty()
                    && let Err(e) = self.history_store.append(instance, deltas.clone()).await {
                        error!(instance, turn_index, error=%e, "failed to append final turn events");
                        // Wake any waiters with error to avoid hangs
                        if let Some(waiters) = self.result_waiters.lock().await.remove(instance) {
                            for w in waiters {
                                let _ = w.send((history.clone(), Err(format!("history append failed: {e}"))));
                            }
                        }
                        panic!("history append failed: {e}");
                    }
                // Exactly-once dispatch for detached orchestration starts recorded in this turn:
                // enqueue provider work-items; poller will perform the idempotent start.
                for e in deltas {
                    if let Event::OrchestrationChained {
                        id,
                        name,
                        instance: child_inst,
                        input,
                    } = e
                    {
                        let wi = crate::providers::WorkItem::StartOrchestration {
                            instance: child_inst.clone(),
                            orchestration: name.clone(),
                            input: input.clone(),
                        };
                        if let Err(err) = self.history_store.enqueue_work(QueueKind::Orchestrator, wi).await {
                            warn!(instance, id, name=%name, child_instance=%child_inst, error=%err, "failed to enqueue detached start; will rely on bootstrap rehydration");
                        } else {
                            debug!(instance, id, name=%name, child_instance=%child_inst, "enqueued detached orchestration start (final turn)");
                        }
                    }
                }
                // Persist terminal event based on result
                let term = match &out {
                    Ok(s) => Event::OrchestrationCompleted { output: s.clone() },
                    Err(e) => Event::OrchestrationFailed { error: e.clone() },
                };
                if let Err(e) = self.history_store.append(instance, vec![term]).await {
                    error!(instance, turn_index, error=%e, "failed to append terminal event");
                    if let Some(waiters) = self.result_waiters.lock().await.remove(instance) {
                        for w in waiters {
                            let _ = w.send((history.clone(), Err(format!("history append failed: {e}"))));
                        }
                    }
                    panic!("history append failed: {e}");
                }
                // Reflect terminal in local history
                let term_local = match &out {
                    Ok(s) => Event::OrchestrationCompleted { output: s.clone() },
                    Err(e) => Event::OrchestrationFailed { error: e.clone() },
                };
                history.push(term_local);
                // Notify any waiters with string result
                if let Some(waiters) = self.result_waiters.lock().await.remove(instance) {
                    let out_s: Result<String, String> = match &out {
                        Ok(s) => Ok(s.clone()),
                        Err(e) => Err(e.clone()),
                    };
                    for w in waiters {
                        let _ = w.send((history.clone(), out_s.clone()));
                    }
                }
                // If child, enqueue completion to parent
                if let Some((pinst, pid)) = parent_link.clone() {
                    match &out {
                        Ok(s) => {
                            let _ = self
                                .history_store
                                .enqueue_work(
                                    QueueKind::Orchestrator,
                                    WorkItem::SubOrchCompleted {
                                        parent_instance: pinst.clone(),
                                        parent_execution_id: self.get_execution_id_for_instance(&pinst).await,
                                        parent_id: pid,
                                        result: s.clone(),
                                    },
                                )
                                .await;
                        }
                        Err(e) => {
                            let _ = self
                                .history_store
                                .enqueue_work(
                                    QueueKind::Orchestrator,
                                    WorkItem::SubOrchFailed {
                                        parent_instance: pinst.clone(),
                                        parent_execution_id: self.get_execution_id_for_instance(&pinst).await,
                                        parent_id: pid,
                                        error: e.clone(),
                                    },
                                )
                                .await;
                        }
                    }
                }
                return (history, out);
            }

            // Persist deltas incrementally to avoid duplicates
            let mut persisted_len = baseline_len;
            let mut appended_any = false;
            if history.len() > persisted_len {
                let new_events = history[persisted_len..].to_vec();
                if let Err(e) = self.history_store.append(instance, new_events).await {
                    error!(instance, turn_index, error=%e, "failed to append scheduled events");
                    if let Some(waiters) = self.result_waiters.lock().await.remove(instance) {
                        for w in waiters {
                            let _ = w.send((history.clone(), Err(format!("history append failed: {e}"))));
                        }
                    }
                    panic!("history append failed: {e}");
                }
                appended_any = true;
                persisted_len = history.len();
            }

            self.apply_decisions(instance, &history, decisions).await;

            // Receive at least one completion, or dehydrate on idle timeout
            let len_before_completions = history.len();
            let first_opt = tokio::time::timeout(
                std::time::Duration::from_millis(Self::ORCH_IDLE_DEHYDRATE_MS),
                comp_rx.recv(),
            )
            .await;
            let first = match first_opt {
                Ok(Some(msg)) => msg,
                Ok(None) => {
                    self.router.unregister(instance).await;
                    return (history, Ok(String::new()));
                }
                Err(_timeout) => {
                    // Dehydrate only if no outstanding result waiters
                    let has_waiters = self.result_waiters.lock().await.contains_key(instance);
                    if has_waiters {
                        tokio::time::sleep(std::time::Duration::from_millis(Self::POLLER_IDLE_SLEEP_MS)).await;
                        continue;
                    } else {
                        // Unregister inbox and release active guard by returning
                        self.router.unregister(instance).await;
                        return (history, Ok(String::new()));
                    }
                }
            };
            let mut ack_tokens_persist_after: Vec<String> = Vec::new();
            let mut ack_tokens_immediate: Vec<String> = Vec::new();
            if let (Some(t), changed) = completions::append_completion(&mut history, first) {
                if changed {
                    ack_tokens_persist_after.push(t);
                } else {
                    ack_tokens_immediate.push(t);
                }
            }
            for _ in 0..Self::COMPLETION_BATCH_LIMIT {
                match comp_rx.try_recv() {
                    Ok(msg) => {
                        if let (Some(t), changed) = completions::append_completion(&mut history, msg) {
                            if changed {
                                ack_tokens_persist_after.push(t);
                            } else {
                                ack_tokens_immediate.push(t);
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
            // Ack immediately for messages that resulted in no history change (e.g., dropped externals)
            for t in ack_tokens_immediate.drain(..) {
                let _ = self.history_store.ack(QueueKind::Orchestrator, &t).await;
            }

            // Persist any further events appended during completion handling
            if history.len() > persisted_len {
                let new_events = history[persisted_len..].to_vec();
                if let Err(e) = self.history_store.append(instance, new_events).await {
                    error!(instance, turn_index, error=%e, "failed to append history");
                    if let Some(waiters) = self.result_waiters.lock().await.remove(instance) {
                        for w in waiters {
                            let _ = w.send((history.clone(), Err(format!("history append failed: {e}"))));
                        }
                    }
                    panic!("history append failed: {e}");
                }
                appended_any = true;
                // Ack any peek-locked items now that the history is persisted
                for t in ack_tokens_persist_after.drain(..) {
                    let _ = self.history_store.ack(QueueKind::Orchestrator, &t).await;
                }
            } else {
                // No persistence occurred (duplicate completions); ack tokens if any
                for t in ack_tokens_persist_after.drain(..) {
                    let _ = self.history_store.ack(QueueKind::Orchestrator, &t).await;
                }
            }

            // Record which completions were appended in this iteration to validate on the next run_turn
            detect::collect_last_appended(&history, len_before_completions, &mut last_appended_completions);

            // If a cancel request was appended in this batch, terminate deterministically.
            if history
                .iter()
                .skip(len_before_completions)
                .any(|e| matches!(e, Event::OrchestrationCancelRequested { .. }))
            {
                let reason = history
                    .iter()
                    .rev()
                    .find_map(|e| match e {
                        Event::OrchestrationCancelRequested { reason } => Some(reason.clone()),
                        _ => None,
                    })
                    .unwrap_or_else(|| "canceled".to_string());
                let term = Event::OrchestrationFailed {
                    error: format!("canceled: {}", reason),
                };
                if let Err(e) = self.history_store.append(instance, vec![term.clone()]).await {
                    error!(instance, turn_index, error=%e, "failed to append terminal cancel");
                    panic!("history append failed: {e}");
                }
                history.push(term.clone());
                // Notify any waiters with canceled error
                if let Some(waiters) = self.result_waiters.lock().await.remove(instance) {
                    for w in waiters {
                        let _ = w.send((history.clone(), Err(format!("canceled: {}", reason))));
                    }
                }
                // Downward propagation: cancel any scheduled sub-orchestrations without completion
                let scheduled_children: Vec<(u64, String)> = history
                    .iter()
                    .filter_map(|e| match e {
                        Event::SubOrchestrationScheduled {
                            id, instance: child, ..
                        } => Some((*id, child.clone())),
                        _ => None,
                    })
                    .collect();
                let completed_ids: std::collections::HashSet<u64> = history
                    .iter()
                    .filter_map(|e| match e {
                        Event::SubOrchestrationCompleted { id, .. } | Event::SubOrchestrationFailed { id, .. } => {
                            Some(*id)
                        }
                        _ => None,
                    })
                    .collect();
                for (id, child_suffix) in scheduled_children {
                    if !completed_ids.contains(&id) {
                        let child_full = format!("{}::{}", instance, child_suffix);
                        let _ = self
                            .history_store
                            .enqueue_work(
                                QueueKind::Orchestrator,
                                WorkItem::CancelInstance {
                                    instance: child_full,
                                    reason: "parent canceled".into(),
                                },
                            )
                            .await;
                    }
                }
                return (history, Err(format!("canceled: {}", reason)));
            }

            // Validate that each newly appended completion correlates to a start event of the same kind.
            // If a completion's correlation id points to a different kind of start (or none), classify as nondeterministic.
            if !last_appended_completions.is_empty()
                && let Some(err) = detect::detect_completion_kind_mismatch(
                    &history[..len_before_completions],
                    &last_appended_completions,
                ) {
                    let _ = self
                        .history_store
                        .append(instance, vec![Event::OrchestrationFailed { error: err.clone() }])
                        .await;
                    history.push(Event::OrchestrationFailed { error: err.clone() });
                    if let Some(waiters) = self.result_waiters.lock().await.remove(instance) {
                        for w in waiters {
                            let _ = w.send((history.clone(), Err(err.clone())));
                        }
                    }
                    return (history, Err(err));
                }

            if appended_any {
                turn_index = turn_index.saturating_add(1);
            }
        }
    }

    /// Spawn an instance and return a handle that resolves to its history
    /// and output when complete.
    pub fn spawn_instance_to_completion(
        self: Arc<Self>,
        instance: &str,
        orchestration_name: &str,
    ) -> JoinHandle<(Vec<Event>, Result<String, String>)> {
        let this_for_task = self.clone();
        let inst = instance.to_string();
        let orch_name = orchestration_name.to_string();
        tokio::spawn(async move { this_for_task.run_instance_to_completion(&inst, &orch_name).await })
    }
}

// timer worker removed; timers now flow via WorkDispatcher (TimerSchedule -> TimerFired)

// moved to completions.rs

impl Runtime {
    /// Raise an external event by name into a running instance.
    pub async fn raise_event(&self, instance: &str, name: impl Into<String>, data: impl Into<String>) {
        let name_str = name.into();
        let data_str = data.into();
        // Best-effort: only enqueue if a subscription for this name exists in the latest execution
        let hist = self.history_store.read(instance).await;
        let has_subscription = hist
            .iter()
            .any(|e| matches!(e, Event::ExternalSubscribed { name, .. } if name == &name_str));
        if !has_subscription {
            warn!(instance, event_name=%name_str, "raise_event: dropping external event with no active subscription");
            return;
        }
        if let Err(e) = self
            .history_store
            .enqueue_work(
                QueueKind::Orchestrator,
                WorkItem::ExternalRaised {
                    instance: instance.to_string(),
                    name: name_str.clone(),
                    data: data_str.clone(),
                },
            )
            .await
        {
            warn!(instance, name=%name_str, error=%e, "raise_event: failed to enqueue ExternalRaised");
        }
        info!(instance, name=%name_str, data=%data_str, "raise_event: enqueued external");
    }

    /// Request cancellation of a running orchestration instance.
    pub async fn cancel_instance(&self, instance: &str, reason: impl Into<String>) {
        let reason_s = reason.into();
        // Only enqueue if instance is active, else best-effort: provider queue will deliver when it becomes active
        let _ = self
            .history_store
            .enqueue_work(
                QueueKind::Orchestrator,
                WorkItem::CancelInstance {
                    instance: instance.to_string(),
                    reason: reason_s,
                },
            )
            .await;
    }

    /// Wait until the orchestration reaches a terminal state (Completed/Failed) or the timeout elapses.
    pub async fn wait_for_orchestration(
        &self,
        instance: &str,
        timeout: std::time::Duration,
    ) -> Result<OrchestrationStatus, WaitError> {
        let deadline = std::time::Instant::now() + timeout;
        // quick path
        match self.get_orchestration_status(instance).await {
            OrchestrationStatus::Completed { output } => return Ok(OrchestrationStatus::Completed { output }),
            OrchestrationStatus::Failed { error } => return Ok(OrchestrationStatus::Failed { error }),
            _ => {}
        }
        // poll with backoff
        let mut delay_ms: u64 = 5;
        while std::time::Instant::now() < deadline {
            match self.get_orchestration_status(instance).await {
                OrchestrationStatus::Completed { output } => return Ok(OrchestrationStatus::Completed { output }),
                OrchestrationStatus::Failed { error } => return Ok(OrchestrationStatus::Failed { error }),
                _ => {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    delay_ms = (delay_ms.saturating_mul(2)).min(100);
                }
            }
        }
        Err(WaitError::Timeout)
    }

    /// Typed variant: returns Ok(Ok<T>) on Completed with decoded output, Ok(Err(String)) on Failed.
    pub async fn wait_for_orchestration_typed<Out: serde::de::DeserializeOwned>(
        &self,
        instance: &str,
        timeout: std::time::Duration,
    ) -> Result<Result<Out, String>, WaitError> {
        match self.wait_for_orchestration(instance, timeout).await? {
            OrchestrationStatus::Completed { output } => match crate::_typed_codec::Json::decode::<Out>(&output) {
                Ok(v) => Ok(Ok(v)),
                Err(e) => Err(WaitError::Other(format!("decode failed: {e}"))),
            },
            OrchestrationStatus::Failed { error } => Ok(Err(error)),
            _ => unreachable!("wait_for_orchestration returns only terminal or timeout"),
        }
    }

    /// Blocking wrapper around wait_for_orchestration.
    pub fn wait_for_orchestration_blocking(
        &self,
        instance: &str,
        timeout: std::time::Duration,
    ) -> Result<OrchestrationStatus, WaitError> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(self.wait_for_orchestration(instance, timeout)))
        } else {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| WaitError::Other(e.to_string()))?;
            rt.block_on(self.wait_for_orchestration(instance, timeout))
        }
    }

    /// Blocking wrapper for typed wait.
    pub fn wait_for_orchestration_typed_blocking<Out: serde::de::DeserializeOwned>(
        &self,
        instance: &str,
        timeout: std::time::Duration,
    ) -> Result<Result<Out, String>, WaitError> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(self.wait_for_orchestration_typed::<Out>(instance, timeout)))
        } else {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| WaitError::Other(e.to_string()))?;
            rt.block_on(self.wait_for_orchestration_typed::<Out>(instance, timeout))
        }
    }
}

// moved to completions.rs
